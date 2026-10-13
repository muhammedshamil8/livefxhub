// Package hub owns the per-symbol windows and fans computed updates out to
// subscribed clients. One goroutine does all the work, so nothing here locks.
package hub

import (
	"context"
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

// Client is a subscriber. Reads come from Updates(); the hub never blocks on
// a client, it drops the update and counts it instead.
type Client struct {
	out     chan Update
	subs    map[string]struct{} // touched only by the hub goroutine
	dropped int64
	hub     *Hub
}

func (c *Client) Updates() <-chan Update { return c.out }

// Dropped is the number of updates discarded because the client was slow.
func (c *Client) Dropped() int64 {
	var n int64
	c.hub.do(func() { n = c.dropped })
	return n
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

func (h *Hub) send(c *Client, u Update) {
	select {
	case c.out <- u:
	default:
		c.dropped++
	}
}

// Register adds a client whose outgoing buffer holds buf updates.
func (h *Hub) Register(buf int) *Client {
	c := &Client{out: make(chan Update, buf), subs: map[string]struct{}{}, hub: h}
	h.do(func() { h.clients[c] = struct{}{} })
	return c
}

// Unregister removes the client and closes its channel.
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
	close(c.out)
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
