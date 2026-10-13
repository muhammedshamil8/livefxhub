// Package feed streams ticks from the LiveFXHub websocket and keeps the
// connection alive across drops.
package feed

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

// ErrInvalidCredentials is returned by Run without retrying.
var ErrInvalidCredentials = errors.New("feed: invalid credentials")

// SessionBusyError means the server still holds a previous session for this key.
type SessionBusyError struct{ Msg string }

func (e *SessionBusyError) Error() string { return e.Msg }

type Tick struct {
	Symbol string
	Bid    float64
	Ask    float64
	At     time.Time // arrival time, the feed has no timestamp
}

type Handler func(Tick)

type Config struct {
	URL     string
	APIKey  string
	Secret  string
	Symbols []string

	HandshakeTimeout time.Duration
	AuthTimeout      time.Duration
	PingInterval     time.Duration
	ReadTimeout      time.Duration // reconnect if nothing arrives for this long

	MinBackoff         time.Duration
	MaxBackoff         time.Duration
	SessionBusyBackoff time.Duration // floor when the server reports an active session

	Debug bool // log every raw frame
}

func (c *Config) defaults() {
	set := func(d *time.Duration, v time.Duration) {
		if *d == 0 {
			*d = v
		}
	}
	set(&c.HandshakeTimeout, 10*time.Second)
	set(&c.AuthTimeout, 15*time.Second)
	set(&c.PingInterval, 20*time.Second)
	set(&c.ReadTimeout, 60*time.Second)
	set(&c.MinBackoff, time.Second)
	set(&c.MaxBackoff, 30*time.Second)
	set(&c.SessionBusyBackoff, 30*time.Second)
}

type authRequest struct {
	Action string `json:"action"`
	APIKey string `json:"api_key"`
	Secret string `json:"secret"`
}

type subscribeRequest struct {
	Action  string   `json:"action"`
	Symbols []string `json:"symbols"`
}

type serverMsg struct {
	Event   string  `json:"event"`
	Success bool    `json:"success"`
	Message string  `json:"message"`
	Symbol  string  `json:"symbol"`
	Bid     float64 `json:"bid"`
	Ask     float64 `json:"ask"`
}

// Run connects and streams ticks to h until ctx is done, reconnecting with
// exponential backoff. Only bad credentials make it give up. h is always
// called from one goroutine at a time.
func Run(ctx context.Context, cfg Config, h Handler) error {
	cfg.defaults()
	backoff := cfg.MinBackoff
	last := map[string][2]float64{} // dedup state survives reconnects

	for {
		streamed, err := runOnce(ctx, cfg, h, last)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errors.Is(err, ErrInvalidCredentials) {
			return err
		}
		if streamed {
			backoff = cfg.MinBackoff
		}

		wait := backoff
		var busy *SessionBusyError
		if errors.As(err, &busy) && wait < cfg.SessionBusyBackoff {
			wait = cfg.SessionBusyBackoff
		}
		wait += time.Duration(rand.Int63n(int64(wait)/4 + 1)) // jitter
		log.Printf("feed: %v, reconnecting in %s", err, wait.Round(time.Millisecond))

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
		if backoff *= 2; backoff > cfg.MaxBackoff {
			backoff = cfg.MaxBackoff
		}
	}
}

// runOnce handles a single connection. streamed reports whether it got as far
// as receiving data, which resets the backoff.
func runOnce(ctx context.Context, cfg Config, h Handler, last map[string][2]float64) (streamed bool, err error) {
	dialer := websocket.Dialer{HandshakeTimeout: cfg.HandshakeTimeout}
	conn, _, err := dialer.DialContext(ctx, cfg.URL, nil)
	if err != nil {
		return false, fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()
	log.Printf("feed: connected to %s", cfg.URL)

	// unblock ReadMessage on shutdown and tell the server we're leaving,
	// otherwise it keeps the session and refuses the next login
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.WriteControl(websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseNormalClosure, "shutdown"),
				time.Now().Add(time.Second))
			conn.Close()
		case <-done:
		}
	}()

	if err := conn.WriteJSON(authRequest{Action: "auth", APIKey: cfg.APIKey, Secret: cfg.Secret}); err != nil {
		return false, fmt.Errorf("send auth: %w", err)
	}
	if err := waitForAuth(conn, cfg.AuthTimeout); err != nil {
		return false, err
	}
	if err := conn.WriteJSON(subscribeRequest{Action: "subscribeMarketPrice", Symbols: cfg.Symbols}); err != nil {
		return false, fmt.Errorf("send subscribe: %w", err)
	}
	log.Printf("feed: authenticated, subscribed to %d symbols", len(cfg.Symbols))

	// any frame, pong included, pushes the deadline; silence means the link is dead
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(cfg.ReadTimeout))
	})
	go func() {
		t := time.NewTicker(cfg.PingInterval)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				if err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(5*time.Second)); err != nil {
					return
				}
			}
		}
	}()

	var ticks, dups int
	defer func() {
		if ticks > 0 {
			log.Printf("feed: session ended after %d ticks, %d duplicates dropped", ticks, dups)
		}
	}()

	for {
		_ = conn.SetReadDeadline(time.Now().Add(cfg.ReadTimeout))
		_, raw, err := conn.ReadMessage()
		if err != nil {
			return streamed, fmt.Errorf("read: %w", err)
		}
		streamed = true
		if cfg.Debug {
			log.Printf("feed: raw %s", raw)
		}

		var m serverMsg
		if err := json.Unmarshal(raw, &m); err != nil || m.Symbol == "" {
			log.Printf("feed: non-tick message: %.200s", raw)
			continue
		}
		if m.Event == "error" {
			return streamed, fmt.Errorf("server error: %s", m.Message)
		}

		// no sequence numbers or timestamps in the feed, so a duplicate can
		// only be recognised as an unchanged bid/ask for the same symbol
		if p, ok := last[m.Symbol]; ok && p[0] == m.Bid && p[1] == m.Ask {
			dups++
			continue
		}
		last[m.Symbol] = [2]float64{m.Bid, m.Ask}
		ticks++
		h(Tick{Symbol: m.Symbol, Bid: m.Bid, Ask: m.Ask, At: time.Now()})
	}
}

func waitForAuth(conn *websocket.Conn, timeout time.Duration) error {
	_ = conn.SetReadDeadline(time.Now().Add(timeout))
	defer conn.SetReadDeadline(time.Time{})

	_, raw, err := conn.ReadMessage()
	if err != nil {
		return fmt.Errorf("read auth response: %w", err)
	}
	var m serverMsg
	if err := json.Unmarshal(raw, &m); err != nil {
		return fmt.Errorf("auth response is not JSON: %.200s", raw)
	}
	switch {
	case m.Event == "authenticated" && m.Success:
		return nil
	case m.Event == "authenticated":
		return fmt.Errorf("authentication rejected: %s", m.Message)
	case m.Event == "error" && strings.Contains(m.Message, "active session"):
		return &SessionBusyError{Msg: m.Message}
	case m.Event == "error" && strings.Contains(m.Message, "invalid credentials"):
		return ErrInvalidCredentials
	case m.Event == "error":
		return fmt.Errorf("authentication failed: %s", m.Message)
	default:
		return fmt.Errorf("unexpected message before authentication: %.200s", raw)
	}
}
