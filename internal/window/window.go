// Package window keeps a rolling price history per symbol as a ring of
// fixed-size OHLC buckets and derives 24h high/low/change from it.
package window

import (
	"sort"
	"time"
)

type Bucket struct {
	Start time.Time
	Open  float64
	High  float64
	Low   float64
	Close float64
	Count int // 0 = empty slot
}

type Stats struct {
	Last      float64
	High      float64
	Low       float64
	ChangePct float64 // vs the oldest bucket's open
	Buckets   int
}

// Window is not goroutine safe, the owning goroutine serialises calls.
type Window struct {
	size    time.Duration
	span    time.Duration
	buckets []Bucket
	head    int // bucket for the latest slot
	last    float64
	n       int
}

func New(span, size time.Duration) *Window {
	if size <= 0 || span < size {
		panic("window: span must be >= size > 0")
	}
	return &Window{
		size:    size,
		span:    span,
		buckets: make([]Bucket, int(span/size)),
	}
}

func (w *Window) Add(price float64, now time.Time) {
	slot := now.Truncate(w.size)
	cur := &w.buckets[w.head]

	switch {
	case cur.Count == 0:
		*cur = Bucket{Start: slot, Open: price, High: price, Low: price, Close: price, Count: 1}
	case slot.After(cur.Start):
		// move head forward, clearing skipped slots so a quiet feed
		// doesn't leave day-old prices in the ring
		for slot.After(cur.Start) {
			w.head = (w.head + 1) % len(w.buckets)
			next := cur.Start.Add(w.size)
			w.buckets[w.head] = Bucket{Start: next}
			cur = &w.buckets[w.head]
		}
		*cur = Bucket{Start: slot, Open: price, High: price, Low: price, Close: price, Count: 1}
	case slot.Before(cur.Start):
		// late / out of order tick, put it in the bucket it belongs to
		age := cur.Start.Sub(slot)
		if age >= w.span {
			return
		}
		back := int(age / w.size)
		idx := (w.head - back + len(w.buckets)) % len(w.buckets)
		b := &w.buckets[idx]
		if b.Count == 0 || !b.Start.Equal(slot) {
			*b = Bucket{Start: slot, Open: price, High: price, Low: price, Close: price, Count: 1}
		} else {
			b.update(price)
		}
		w.last = price
		w.n++
		return
	default:
		cur.update(price)
	}
	w.last = price
	w.n++
}

func (b *Bucket) update(price float64) {
	if price > b.High {
		b.High = price
	}
	if price < b.Low {
		b.Low = price
	}
	b.Close = price
	b.Count++
}

// Stats ignores buckets older than span relative to now, so a symbol that
// went quiet still ages out correctly.
func (w *Window) Stats(now time.Time) Stats {
	s := Stats{Last: w.last}
	if w.n == 0 {
		return s
	}
	cutoff := now.Add(-w.span)
	var oldest *Bucket
	first := true
	for i := range w.buckets {
		b := &w.buckets[i]
		if b.Count == 0 || !b.Start.After(cutoff) {
			continue
		}
		if first || b.High > s.High {
			s.High = b.High
		}
		if first || b.Low < s.Low {
			s.Low = b.Low
		}
		if oldest == nil || b.Start.Before(oldest.Start) {
			oldest = b
		}
		first = false
		s.Buckets++
	}
	if oldest != nil && oldest.Open != 0 {
		s.ChangePct = (w.last - oldest.Open) / oldest.Open * 100
	}
	return s
}

func (w *Window) Last() float64 { return w.last }

func (w *Window) Len() int {
	n := 0
	for i := range w.buckets {
		if w.buckets[i].Count > 0 {
			n++
		}
	}
	return n
}

// Buckets returns the non-empty buckets oldest first, for snapshots.
func (w *Window) Buckets() []Bucket {
	out := make([]Bucket, 0, w.Len())
	n := len(w.buckets)
	for i := 1; i <= n; i++ {
		b := w.buckets[(w.head+i)%n]
		if b.Count > 0 {
			out = append(out, b)
		}
	}
	return out
}

// Restore replaces the ring with bs (any order), dropping buckets older than span.
func (w *Window) Restore(bs []Bucket, now time.Time) {
	for i := range w.buckets {
		w.buckets[i] = Bucket{}
	}
	w.head, w.last, w.n = 0, 0, 0

	sorted := make([]Bucket, 0, len(bs))
	cutoff := now.Add(-w.span)
	for _, b := range bs {
		if b.Count > 0 && b.Start.After(cutoff) {
			sorted = append(sorted, b)
		}
	}
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Start.Before(sorted[j].Start) })

	for _, b := range sorted {
		b.Start = b.Start.Truncate(w.size)
		cur := &w.buckets[w.head]
		if cur.Count != 0 {
			for b.Start.After(cur.Start) {
				w.head = (w.head + 1) % len(w.buckets)
				next := cur.Start.Add(w.size)
				w.buckets[w.head] = Bucket{Start: next}
				cur = &w.buckets[w.head]
			}
		}
		*cur = b
		w.last = b.Close
		w.n += b.Count
	}
}
