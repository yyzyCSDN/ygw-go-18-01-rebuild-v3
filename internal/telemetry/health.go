package telemetry

import (
	"sort"
	"time"
)

type Health struct {
	At             time.Time         `json:"at"`
	EventsInWindow int               `json:"events_in_window"`
	Counters       map[string]uint64 `json:"counters"`
	AverageLatency map[string]int64  `json:"average_latency_ns"`
	RecentFailures []Event           `json:"recent_failures"`
}

// HealthSnapshot is a stable read model for an HTTP probe or command demo.
// It sorts maps and copies events so callers can safely encode the result.
func (r *Registry) HealthSnapshot(now time.Time) Health {
	h := Health{At: now, Counters: map[string]uint64{}, AverageLatency: map[string]int64{}}
	if r == nil {
		return h
	}
	names := r.Names()
	for _, name := range names {
		h.Counters[name] = r.Count(name)
		h.AverageLatency[name] = r.AverageLatency(name).Nanoseconds()
	}
	for _, event := range r.Events() {
		if !event.Succeeded {
			h.RecentFailures = append(h.RecentFailures, event)
		}
	}
	sort.SliceStable(h.RecentFailures, func(i, j int) bool {
		return h.RecentFailures[i].At.Before(h.RecentFailures[j].At)
	})
	h.EventsInWindow = len(r.Events())
	return h
}
