package telemetry

import (
	"sort"
	"sync"
	"time"
)

// Window aggregates event outcomes over a moving time horizon. It is used by
// operators to distinguish a recent outage from a historical counter spike.
type Window struct {
	mu     sync.Mutex
	length time.Duration
	events []Event
	clock  func() time.Time
}

func NewWindow(length time.Duration, clock func() time.Time) *Window {
	if length <= 0 {
		length = time.Minute
	}
	if clock == nil {
		clock = time.Now
	}
	return &Window{length: length, clock: clock}
}

func (w *Window) Add(event Event) {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.events = append(w.events, event)
	w.pruneLocked(w.clock())
}

func (w *Window) pruneLocked(now time.Time) {
	cutoff := now.Add(-w.length)
	first := 0
	for first < len(w.events) && w.events[first].At.Before(cutoff) {
		first++
	}
	if first > 0 {
		copy(w.events, w.events[first:])
		w.events = w.events[:len(w.events)-first]
	}
}

func (w *Window) Snapshot() []Event {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.pruneLocked(w.clock())
	out := append([]Event(nil), w.events...)
	sort.SliceStable(out, func(i, j int) bool { return out[i].At.Before(out[j].At) })
	return out
}

func (w *Window) Counts() (total, failures int) {
	for _, event := range w.Snapshot() {
		total++
		if !event.Succeeded {
			failures++
		}
	}
	return total, failures
}

func (w *Window) FailureRate() float64 {
	total, failures := w.Counts()
	if total == 0 {
		return 0
	}
	return float64(failures) / float64(total)
}

func (w *Window) Healthy(threshold float64) bool {
	if threshold < 0 || threshold > 1 {
		return false
	}
	return w.FailureRate() <= threshold
}

func (w *Window) Length() time.Duration {
	if w == nil {
		return 0
	}
	return w.length
}
