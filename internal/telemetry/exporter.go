package telemetry

import (
	"encoding/json"
	"fmt"
)

// MarshalHealth emits deterministic JSON used by the local status probe.
func MarshalHealth(h Health) ([]byte, error) {
	if h.At.IsZero() {
		return nil, fmt.Errorf("health timestamp is required")
	}
	return json.Marshal(h)
}

func FailureRate(r *Registry, name string) float64 {
	if r == nil || r.Count(name) == 0 {
		return 0
	}
	var failures uint64
	for _, event := range r.Events() {
		if event.Name == name && !event.Succeeded {
			failures++
		}
	}
	return float64(failures) / float64(r.Count(name))
}

func Healthy(r *Registry, name string, threshold float64) bool {
	if threshold < 0 || threshold > 1 {
		return false
	}
	return FailureRate(r, name) <= threshold
}
