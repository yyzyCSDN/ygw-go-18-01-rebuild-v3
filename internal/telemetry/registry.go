package telemetry

import (
	"sort"
	"sync"
	"time"
)

// Registry keeps process-local operational counters and a bounded event window.
// It is deliberately independent from the journal so health reporting cannot
// mutate snapshot state or block a capture transaction.
type Registry struct {
	mu       sync.RWMutex
	counters map[string]uint64
	latency  map[string]time.Duration
	events   []Event
	limit    int
	window   *Window
}

type Event struct {
	Name      string
	Snapshot  string
	At        time.Time
	Duration  time.Duration
	Succeeded bool
}

func New(limit int) *Registry {
	if limit < 8 {
		limit = 8
	}
	return &Registry{counters: make(map[string]uint64), latency: make(map[string]time.Duration), limit: limit, window: NewWindow(5*time.Minute, time.Now)}
}

func (r *Registry) Observe(name, snapshot string, started time.Time, succeeded bool, now time.Time) {
	if r == nil || name == "" {
		return
	}
	d := now.Sub(started)
	if d < 0 {
		d = 0
	}
	r.mu.Lock()
	r.counters[name]++
	r.latency[name] += d
	r.events = append(r.events, Event{Name: name, Snapshot: snapshot, At: now, Duration: d, Succeeded: succeeded})
	r.window.Add(Event{Name: name, Snapshot: snapshot, At: now, Duration: d, Succeeded: succeeded})
	if len(r.events) > r.limit {
		copy(r.events, r.events[len(r.events)-r.limit:])
		r.events = r.events[:r.limit]
	}
	r.mu.Unlock()
}

func (r *Registry) Count(name string) uint64 {
	if r == nil {
		return 0
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.counters[name]
}

func (r *Registry) AverageLatency(name string) time.Duration {
	if r == nil {
		return 0
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	count := r.counters[name]
	if count == 0 {
		return 0
	}
	return r.latency[name] / time.Duration(count)
}

func (r *Registry) Events() []Event {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	copyOf := make([]Event, len(r.events))
	copy(copyOf, r.events)
	return copyOf
}

func (r *Registry) Names() []string {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.counters))
	for name := range r.counters {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
