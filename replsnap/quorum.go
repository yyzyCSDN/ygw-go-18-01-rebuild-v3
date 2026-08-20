package replsnap

import "sync"

func strictMajority(replicaCount int) int {
	if replicaCount < 1 {
		return 1
	}
	return replicaCount/2 + 1
}

type QuorumTracker struct {
	mu       sync.Mutex
	required int
	acks     map[string]struct{}
}

func NewQuorumTracker(replicaCount int) *QuorumTracker {
	return &QuorumTracker{required: strictMajority(replicaCount), acks: make(map[string]struct{})}
}

func (tracker *QuorumTracker) Acknowledge(replicaID string) {
	if replicaID == "" {
		return
	}
	tracker.mu.Lock()
	tracker.acks[replicaID] = struct{}{}
	tracker.mu.Unlock()
}

func (tracker *QuorumTracker) Status() (acknowledged int, required int, reached bool) {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	return len(tracker.acks), tracker.required, len(tracker.acks) >= tracker.required
}

func HasQuorum(replicaIDs []string, replicaCount int) bool {
	tracker := NewQuorumTracker(replicaCount)
	for _, replicaID := range replicaIDs {
		tracker.Acknowledge(replicaID)
	}
	_, _, reached := tracker.Status()
	return reached
}
