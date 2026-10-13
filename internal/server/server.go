// Package server exposes the hub over websocket: clients subscribe to
// symbols and receive our computed updates as small delta batches.
package server

import (
	"encoding/json"
	"math"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"muhammedshamil8/livefxhub/internal/hub"
)

const (
	maxBatch     = 32 // updates per frame, keeps a frame well under 10KB
	writeTimeout = 5 * time.Second
	pingEvery    = 30 * time.Second
	readTimeout  = 90 * time.Second
	maxMsgBytes  = 4096
)

type request struct {
	Action  string   `json:"action"`
	Symbols []string `json:"symbols"`
}

// A batch carries one timestamp (unix ms, arrival of the newest tick in it)
// and only the fields that changed per symbol since the last frame sent to
// this client. The first time a symbol appears all fields are present.
type batchMsg struct {
	At      int64      `json:"t"`
	Updates []deltaMsg `json:"u"`
}

type deltaMsg struct {
	Symbol    string   `json:"s"`
	Bid       *float64 `json:"b,omitempty"`
	Ask       *float64 `json:"a,omitempty"`
	High      *float64 `json:"h,omitempty"`
	Low       *float64 `json:"l,omitempty"`
	ChangePct *float64 `json:"c,omitempty"`
}

type eventMsg struct {
	Event   string   `json:"event"`
	Message string   `json:"message,omitempty"`
	Symbols []string `json:"symbols,omitempty"`
}

// control messages from the reader to the writer goroutine
type control struct {
	frame  []byte
	forget []string // symbols whose delta state must be reset
}

type Server struct {
	hub *hub.Hub
	up  websocket.Upgrader

	// feed arrival -> socket write, in microseconds
	latCount atomic.Int64
	latSum   atomic.Int64
	latMax   atomic.Int64
	// payload accounting
	frames atomic.Int64
	bytes  atomic.Int64
	sent   atomic.Int64
}

func New(h *hub.Hub) *Server {
	return &Server{hub: h, up: websocket.Upgrader{
		CheckOrigin:       func(*http.Request) bool { return true },
		EnableCompression: true, // permessage-deflate when the client offers it
	}}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", s.handleWS)
	mux.HandleFunc("/healthz", s.handleHealth)
	return mux
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	n := s.latCount.Load()
	var avgLat, avgFrame int64
	if n > 0 {
		avgLat = s.latSum.Load() / n
	}
	if f := s.frames.Load(); f > 0 {
		avgFrame = s.bytes.Load() / f
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"status":          "ok",
		"clients":         s.hub.Clients(),
		"symbols":         s.hub.Symbols(),
		"updates_sent":    s.sent.Load(),
		"frames_sent":     s.frames.Load(),
		"avg_frame_bytes": avgFrame,
		"latency_avg_us":  avgLat,
		"latency_max_us":  s.latMax.Load(),
	})
}

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := s.up.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	c := s.hub.Register()
	defer s.hub.Unregister(c)

	// one writer goroutine per connection; acks and resets go through ctl
	ctl := make(chan control, 16)
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		s.writeLoop(conn, c, ctl)
	}()
	send := func(e eventMsg, forget []string) {
		b, _ := json.Marshal(e)
		select {
		case ctl <- control{frame: b, forget: forget}:
		case <-writerDone:
		}
	}

	conn.SetReadLimit(maxMsgBytes)
	conn.SetReadDeadline(time.Now().Add(readTimeout))
	conn.SetPongHandler(func(string) error { return conn.SetReadDeadline(time.Now().Add(readTimeout)) })

	for {
		var req request
		if err := conn.ReadJSON(&req); err != nil {
			break // client gone, or garbage: either way we're done
		}
		conn.SetReadDeadline(time.Now().Add(readTimeout))
		switch req.Action {
		case "subscribe":
			s.hub.Subscribe(c, req.Symbols)
			send(eventMsg{Event: "subscribed", Symbols: req.Symbols}, nil)
		case "unsubscribe":
			s.hub.Unsubscribe(c, req.Symbols)
			send(eventMsg{Event: "unsubscribed", Symbols: req.Symbols}, req.Symbols)
		default:
			send(eventMsg{Event: "error", Message: "unknown action: " + req.Action}, nil)
		}
	}
	s.hub.Unregister(c) // closes c.Wait(), which stops the writer
	<-writerDone
}

func (s *Server) writeLoop(conn *websocket.Conn, c *hub.Client, ctl <-chan control) {
	ping := time.NewTicker(pingEvery)
	defer ping.Stop()
	last := map[string]hub.Update{} // what this client has seen, for deltas

	write := func(b []byte) bool {
		conn.SetWriteDeadline(time.Now().Add(writeTimeout))
		if err := conn.WriteMessage(websocket.TextMessage, b); err != nil {
			conn.Close() // makes the read loop exit and clean up
			return false
		}
		return true
	}

	for {
		select {
		case m := <-ctl:
			for _, sym := range m.forget {
				delete(last, sym)
			}
			if !write(m.frame) {
				return
			}
		case _, ok := <-c.Wait():
			if !ok {
				conn.WriteControl(websocket.CloseMessage,
					websocket.FormatCloseMessage(websocket.CloseNormalClosure, "bye"), time.Now().Add(time.Second))
				return
			}
			for {
				batch := c.Take(maxBatch)
				if len(batch) == 0 {
					break
				}
				frame, newest, n := encode(batch, last)
				if n == 0 {
					continue
				}
				if !write(frame) {
					return
				}
				s.sent.Add(int64(n))
				s.frames.Add(1)
				s.bytes.Add(int64(len(frame)))
				s.recordLatency(time.Since(newest))
			}
		case <-ping.C:
			if err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(writeTimeout)); err != nil {
				conn.Close()
				return
			}
		}
	}
}

// encode builds a delta frame against last and updates it. n is the number
// of symbols that actually changed.
func encode(batch []hub.Update, last map[string]hub.Update) (frame []byte, newest time.Time, n int) {
	msg := batchMsg{Updates: make([]deltaMsg, 0, len(batch))}
	for _, u := range batch {
		u.ChangePct = math.Round(u.ChangePct*1e4) / 1e4
		prev, seen := last[u.Symbol]
		d := deltaMsg{Symbol: u.Symbol}
		changed := false
		set := func(dst **float64, cur, old float64) {
			if !seen || cur != old {
				v := cur
				*dst = &v
				changed = true
			}
		}
		set(&d.Bid, u.Bid, prev.Bid)
		set(&d.Ask, u.Ask, prev.Ask)
		set(&d.High, u.High, prev.High)
		set(&d.Low, u.Low, prev.Low)
		set(&d.ChangePct, u.ChangePct, prev.ChangePct)
		if !changed {
			continue
		}
		last[u.Symbol] = u
		msg.Updates = append(msg.Updates, d)
		if u.At.After(newest) {
			newest = u.At
		}
	}
	n = len(msg.Updates)
	if n == 0 {
		return nil, newest, 0
	}
	msg.At = newest.UnixMilli()
	frame, _ = json.Marshal(msg)
	return frame, newest, n
}

func (s *Server) recordLatency(d time.Duration) {
	us := d.Microseconds()
	s.latCount.Add(1)
	s.latSum.Add(us)
	for {
		cur := s.latMax.Load()
		if us <= cur || s.latMax.CompareAndSwap(cur, us) {
			return
		}
	}
}
