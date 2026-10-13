package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

const defaultWSURL = "wss://v3-prod.livefxhub.com/ws/algo"

var symbols = []string{
	"AUDUSD", "AUDCAD", "AUDCHF", "AUDJPY", "AUDNZD",
	"AUDSGD", "CADCHF", "CADJPY", "CHFJPY", "CHFSGD",
	"EURAUD", "EURCAD", "EURCHF", "EURGBP", "EURJPY",
	"EURNZD", "EURUSD", "GBPAUD", "GBPCAD", "GBPCHF",
	"GBPJPY", "GBPNZD", "GBPUSD", "NZDCAD", "NZDCHF",
	"NZDJPY", "NZDUSD", "USDCAD", "USDCHF", "USDCNH",
	"USDJPY", "USDMXN", "USDNOK", "USDPLN", "USDSEK",
	"USDSGD", "USDTRY", "USDZAR", "EURTRY", "GBPTRY",
	"NOKJPY", "SEKJPY", "SGDJPY", "ZARJPY", "XAUUSD",
	"XAGUSD", "BTCUSD", "ETHUSD", "BNBUSD", "SOLUSD",
	"XRPUSD", "ADAUSD", "DOGUSD", "DOTUSD", "LTCUSD",
	"BCHUSD", "XLMUSD", "TRXUSD", "UNIUSD", "FILUSD",
	"AVXUSD",
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

type authResponse struct {
	Event   string `json:"event"`
	Success bool   `json:"success"`
	Message string `json:"message"`
}

// actual feed shape differs from the pdf: bid/ask instead of buy/sell, no timestamp.
// high/low/percentage come from the feed but we compute our own.
type tick struct {
	Event      string  `json:"event"`
	Symbol     string  `json:"symbol"`
	Bid        float64 `json:"bid"`
	Ask        float64 `json:"ask"`
	High       float64 `json:"high"`
	Low        float64 `json:"low"`
	Percentage float64 `json:"percentage"`
}

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	loadDotEnv(".env")

	apiKey := os.Getenv("API_KEY")
	secret := os.Getenv("SECRET_KEY")
	if apiKey == "" || secret == "" {
		log.Fatal("API_KEY and SECRET_KEY must be set (see .env.example)")
	}
	wsURL := os.Getenv("WS_URL")
	if wsURL == "" {
		wsURL = defaultWSURL
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, wsURL, apiKey, secret); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("client stopped: %v", err)
	}
	log.Println("bye")
}

func run(ctx context.Context, wsURL, apiKey, secret string) error {
	dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	conn, _, err := dialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		return fmt.Errorf("dial %s: %w", wsURL, err)
	}
	defer conn.Close()
	log.Printf("connected to %s", wsURL)

	// unblock ReadMessage on ctrl-c
	go func() {
		<-ctx.Done()
		_ = conn.WriteControl(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, "shutdown"),
			time.Now().Add(time.Second))
		conn.Close()
	}()

	if err := conn.WriteJSON(authRequest{Action: "auth", APIKey: apiKey, Secret: secret}); err != nil {
		return fmt.Errorf("send auth: %w", err)
	}
	log.Println("auth sent, waiting for response...")

	if err := waitForAuth(conn, 15*time.Second); err != nil {
		return err
	}

	if err := conn.WriteJSON(subscribeRequest{Action: "subscribeMarketPrice", Symbols: symbols}); err != nil {
		return fmt.Errorf("send subscribe: %w", err)
	}
	log.Printf("subscribe sent for %d symbols", len(symbols))

	_ = conn.SetReadDeadline(time.Time{})
	debugRaw := os.Getenv("DEBUG_RAW") != ""
	var count int
	seen := map[string]bool{}
	events := map[string]int{}
	lastReport := time.Now()
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("read: %w", err)
		}

		if debugRaw {
			log.Printf("raw: %s", raw)
		}
		var t tick
		if err := json.Unmarshal(raw, &t); err != nil || t.Symbol == "" {
			log.Printf("non-tick message (%d bytes): %s", len(raw), truncate(raw, 300))
			continue
		}

		count++
		seen[t.Symbol] = true
		events[t.Event]++
		if count <= 5 {
			log.Printf("tick #%d (%d bytes): %+v", count, len(raw), t)
		}
		if time.Since(lastReport) >= 5*time.Second {
			log.Printf("received %d ticks across %d/%d symbols, events=%v; last %s bid=%.5f ask=%.5f",
				count, len(seen), len(symbols), events, t.Symbol, t.Bid, t.Ask)
			lastReport = time.Now()
		}
	}
}

// server replies {"event":"authenticated","success":true,...} on success and
// {"event":"error","message":"..."} otherwise (invalid credentials, already have an active session).
func waitForAuth(conn *websocket.Conn, timeout time.Duration) error {
	_ = conn.SetReadDeadline(time.Now().Add(timeout))
	defer conn.SetReadDeadline(time.Time{})

	_, raw, err := conn.ReadMessage()
	if err != nil {
		return fmt.Errorf("read auth response: %w", err)
	}
	log.Printf("auth response: %s", raw)

	var resp authResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		return fmt.Errorf("auth response is not JSON: %s", truncate(raw, 200))
	}
	switch {
	case resp.Event == "authenticated" && resp.Success:
		return nil
	case resp.Event == "authenticated":
		return fmt.Errorf("authentication rejected: %s", resp.Message)
	case resp.Event == "error":
		return fmt.Errorf("authentication failed: %s", resp.Message)
	default:
		return fmt.Errorf("unexpected message before authentication: %s", truncate(raw, 200))
	}
}

func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "..."
}

// minimal .env loader, real env vars take precedence
func loadDotEnv(path string) {
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		k = strings.TrimSpace(k)
		v = strings.Trim(strings.TrimSpace(v), `"'`)
		if os.Getenv(k) == "" {
			os.Setenv(k, v)
		}
	}
}
