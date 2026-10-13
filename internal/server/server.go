// Package server exposes the hub over websocket: clients subscribe to
// symbols and receive our computed updates.
package server

import (
	"encoding/json"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"muhammedshamil8/livefxhub/internal/hub"
)

const (
	clientBuffer = 1024
	writeTimeout = 5 * time.Second
	pingEvery    = 30 * time.Second
	readTimeout  = 90 * time.Second
	maxMsgBytes  = 4096
)

type request struct {
	Action  string   `json:"action"`
	Symbols []string `json:"symbols"`
}

// short keys: payload size is a requirement
type updateMsg struct {
	Symbol    string  `json:"s"`
	Bid       float64 `json:"b"`
	Ask       float64 `json:"a"`
	High      float64 `json:"h"`
	Low       float64 `json:"l"`
	ChangePct float64 `json:"c"`
	At        int64   `json:"t"` // unix ms
}

type eventMsg struct {
	Event   string   `json:"event"`
	Message string   `json:"message,omitempty"`
	Symbols []string `json:"symbols,omitempty"`
}

type Server struct {
	hub *hub.Hub
	up  websocket.Upgrader

	// feed arrival -> socket write, in microseconds
	latCount atomic.Int64
	latSum   atomic.Int64
	latMax   atomic.Int64
}

func New(h *hub.Hub) *Server {
	return &Server{hub: h, up: websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", s.handleWS)
	mux.HandleFunc("/healthz", s.handleHealth)
	return mux
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	n := s.latCount.Load()
	var avg int64
	if n > 0 {
		avg = s.latSum.Load() / n
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"status":         "ok",
		"clients":        s.hub.Clients(),
		"symbols":        s.hub.Symbols(),
		"updates_sent":   n,
		"latency_avg_us": avg,
		"latency_max_us": s.latMax.Load(),
	})
}

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := s.up.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	c := s.hub.Register(clientBuffer)
	defer s.hub.Unregister(c)

	// writer: the only goroutine that writes to conn. Acks go through the
	// same goroutine via events.
	events := make(chan []byte, 16)
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		s.writeLoop(conn, c, events)
	}()
	event := func(e eventMsg) {
		b, _ := json.Marshal(e)
		select {
		case events <- b:
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
			event(eventMsg{Event: "subscribed", Symbols: req.Symbols})
		case "unsubscribe":
			s.hub.Unsubscribe(c, req.Symbols)
			event(eventMsg{Event: "unsubscribed", Symbols: req.Symbols})
		default:
			event(eventMsg{Event: "error", Message: "unknown action: " + req.Action})
		}
	}
	s.hub.Unregister(c) // closes c.Updates(), which stops the writer
	<-writerDone
}

func (s *Server) writeLoop(conn *websocket.Conn, c *hub.Client, events <-chan []byte) {
	ping := time.NewTicker(pingEvery)
	defer ping.Stop()
	for {
		select {
		case b := <-events:
			conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			if err := conn.WriteMessage(websocket.TextMessage, b); err != nil {
				conn.Close()
				return
			}
		case u, ok := <-c.Updates():
			if !ok {
				conn.WriteControl(websocket.CloseMessage,
					websocket.FormatCloseMessage(websocket.CloseNormalClosure, "bye"), time.Now().Add(time.Second))
				return
			}
			b, _ := json.Marshal(updateMsg{
				Symbol: u.Symbol, Bid: u.Bid, Ask: u.Ask, High: u.High, Low: u.Low, ChangePct: u.ChangePct,
				At: u.At.UnixMilli(),
			})
			conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			if err := conn.WriteMessage(websocket.TextMessage, b); err != nil {
				conn.Close() // makes the read loop exit and clean up
				return
			}
			s.recordLatency(time.Since(u.At))
		case <-ping.C:
			if err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(writeTimeout)); err != nil {
				conn.Close()
				return
			}
		}
	}
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
