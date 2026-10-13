// Package hub owns the per-symbol windows and fans computed updates out to
// subscribed clients. One goroutine does all the work, so nothing here locks.
package hub

import (
	"context"
	"sync"
	"time"

	"muhammedshamil8/livefxhub/internal/store"
	"muhammedshamil8/livefxhub/internal/window"
)

type Tick struct {
	Symbol string
	Bid    float64
	Ask    float64
	At     time.Time
}

// Update is what clients receive: latest prices plus our own 24h stats.
type Update struct {
	Symbol    string
	Bid       float64
	Ask       float64
	High      float64
	Low       float64
	ChangePct float64
	At        time.Time // arrival time of the tick behind it, for latency measurement
}

// Client is a subscriber with a coalescing outbox: at most one pending
// update per symbol, newest wins. The hub never blocks on a client and
// memory per client is bounded by its subscription count. A slow client
// just sees fewer, fresher updates.
type Client struct {
	mu        sync.Mutex
	pending   map[string]Update
	order     []string
	coalesced int64
	closed    bool
	wake      chan struct{} // cap 1, closed on unregister

	subs map[string]struct{} // touched only by the hub goroutine
}

// Wait is signalled when the outbox has something to send and closed when
// the client is unregistered.
func (c *Client) Wait() <-chan struct{} { return c.wake }

// Take drains up to max pending updates in arrival order.
func (c *Client) Take(max int) []Update {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := len(c.order)
	if n == 0 {
		return nil
	}
	if n > max {
		n = max
	}
	out := make([]Update, 0, n)
	for _, sym := range c.order[:n] {
		out = append(out, c.pending[sym])
		delete(c.pending, sym)
	}
	c.order = append(c.order[:0], c.order[n:]...)
	if len(c.order) > 0 { // more left, make sure the writer comes back
		select {
		case c.wake <- struct{}{}:
		default:
		}
	}
	return out
}

// Coalesced counts updates that were superseded before being sent.
func (c *Client) Coalesced() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.coalesced
}

func (c *Client) push(u Update) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	if _, ok := c.pending[u.Symbol]; ok {
		c.coalesced++
	} else {
		c.order = append(c.order, u.Symbol)
	}
	c.pending[u.Symbol] = u
	c.mu.Unlock()
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

func (c *Client) shutdown() {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	close(c.wake)
}

type Options struct {
	WindowSpan time.Duration
	BucketSize time.Duration
	TickBuffer int // how many ticks may queue between the feed and the hub
}

type Hub struct {
	opts     Options
	windows  map[string]*window.Window
	last     map[string]Tick // latest tick per symbol, for subscribe snapshots
	clients  map[*Client]struct{}
	bySymbol map[string]map[*Client]struct{}

	ticks chan Tick
	cmds  chan func()
	done  chan struct{} // closed when Run returns
}

func New(windows map[string]*window.Window, opts Options) *Hub {
	if opts.WindowSpan == 0 {
		opts.WindowSpan = 24 * time.Hour
	}
	if opts.BucketSize == 0 {
		opts.BucketSize = time.Minute
	}
	if opts.TickBuffer == 0 {
		opts.TickBuffer = 4096
	}
	if windows == nil {
		windows = map[string]*window.Window{}
	}
	return &Hub{
		opts:     opts,
		windows:  windows,
		last:     map[string]Tick{},
		clients:  map[*Client]struct{}{},
		bySymbol: map[string]map[*Client]struct{}{},
		ticks:    make(chan Tick, opts.TickBuffer),
		cmds:     make(chan func()),
		done:     make(chan struct{}),
	}
}

// Run processes ticks and commands until ctx is done, then closes every
// client's channel.
func (h *Hub) Run(ctx context.Context) {
	defer close(h.done)
	for {
		select {
		case <-ctx.Done():
			for c := range h.clients {
				h.remove(c)
			}
			return
		case t := <-h.ticks:
			h.handle(t)
		case f := <-h.cmds:
			f()
		}
	}
}

// Push queues a tick without blocking. False means the hub is that far
// behind that the buffer is full and the tick was dropped.
func (h *Hub) Push(t Tick) bool {
	select {
	case h.ticks <- t:
		return true
	default:
		return false
	}
}

// do runs f on the hub goroutine and waits for it. After Run has returned
// nobody else touches the state, so f just runs inline.
func (h *Hub) do(f func()) {
	done := make(chan struct{})
	select {
	case h.cmds <- func() { f(); close(done) }:
		<-done
	case <-h.done:
		f()
	}
}

func (h *Hub) handle(t Tick) {
	w := h.windows[t.Symbol]
	if w == nil {
		w = window.New(h.opts.WindowSpan, h.opts.BucketSize)
		h.windows[t.Symbol] = w
	}
	w.Add(t.Bid, t.At)
	h.last[t.Symbol] = t

	subs := h.bySymbol[t.Symbol]
	if len(subs) == 0 {
		return
	}
	u := h.update(t.Symbol, t.Bid, t.Ask, w, t.At)
	for c := range subs {
		h.send(c, u)
	}
}

func (h *Hub) update(sym string, bid, ask float64, w *window.Window, at time.Time) Update {
	st := w.Stats(at)
	return Update{Symbol: sym, Bid: bid, Ask: ask, High: st.High, Low: st.Low, ChangePct: st.ChangePct, At: at}
}

func (h *Hub) send(c *Client, u Update) { c.push(u) }

func (h *Hub) Register() *Client {
	c := &Client{pending: map[string]Update{}, wake: make(chan struct{}, 1), subs: map[string]struct{}{}}
	h.do(func() { h.clients[c] = struct{}{} })
	return c
}

// Unregister removes the client and closes its wake channel.
func (h *Hub) Unregister(c *Client) {
	h.do(func() { h.remove(c) })
}

func (h *Hub) remove(c *Client) {
	if _, ok := h.clients[c]; !ok {
		return
	}
	for sym := range c.subs {
		h.unsub(c, sym)
	}
	delete(h.clients, c)
	c.shutdown()
}

// Subscribe adds symbols and immediately queues the current state of each
// one we already have data for. After a restart only the window is known,
// so until the first live tick ask is reported equal to bid.
func (h *Hub) Subscribe(c *Client, symbols []string) {
	h.do(func() {
		if _, ok := h.clients[c]; !ok {
			return
		}
		now := time.Now()
		for _, sym := range symbols {
			c.subs[sym] = struct{}{}
			if h.bySymbol[sym] == nil {
				h.bySymbol[sym] = map[*Client]struct{}{}
			}
			h.bySymbol[sym][c] = struct{}{}
			w := h.windows[sym]
			if w == nil || w.Len() == 0 {
				continue
			}
			bid, ask := w.Last(), w.Last()
			if lt, ok := h.last[sym]; ok {
				bid, ask = lt.Bid, lt.Ask
			}
			h.send(c, h.update(sym, bid, ask, w, now))
		}
	})
}

func (h *Hub) Unsubscribe(c *Client, symbols []string) {
	h.do(func() {
		for _, sym := range symbols {
			h.unsub(c, sym)
		}
	})
}

func (h *Hub) unsub(c *Client, sym string) {
	delete(c.subs, sym)
	if m := h.bySymbol[sym]; m != nil {
		delete(m, c)
		if len(m) == 0 {
			delete(h.bySymbol, sym)
		}
	}
}

// Snapshot copies the window state for persistence.
func (h *Hub) Snapshot() store.Snapshot {
	var s store.Snapshot
	h.do(func() {
		s = store.Snapshot{Symbols: make(map[string][]window.Bucket, len(h.windows))}
		for sym, w := range h.windows {
			s.Symbols[sym] = w.Buckets()
		}
	})
	return s
}

func (h *Hub) Clients() int {
	var n int
	h.do(func() { n = len(h.clients) })
	return n
}

func (h *Hub) Symbols() int {
	var n int
	h.do(func() { n = len(h.windows) })
	return n
}
