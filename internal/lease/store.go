package lease

import (
	"sync"
	"time"

	"example.com/backupmesh/internal/model"
)

type Store struct {
	mu     sync.Mutex
	leases map[string]model.Lease
	epochs map[string]uint64
}

func New() *Store {
	return &Store{leases: make(map[string]model.Lease), epochs: make(map[string]uint64)}
}

func (s *Store) Acquire(resource, owner string, now time.Time, ttl time.Duration) (model.Lease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, exists := s.leases[resource]
	if exists && !current.Released && now.Before(current.Deadline) {
		return model.Lease{}, model.ErrLeaseHeld
	}
	// Every successful (re)acquisition advances the epoch, even when the owner
	// is unchanged. A re-acquired lease therefore carries a strictly greater
	// epoch than the one stamped on any handle from the superseded capture
	// round, so late operations presented with the old epoch are fenced.
	epoch := s.epochs[resource] + 1
	s.epochs[resource] = epoch
	lease := model.Lease{
		Resource: resource,
		Owner:    owner,
		Epoch:    epoch,
		Deadline: now.Add(ttl),
	}
	s.leases[resource] = lease
	return lease, nil
}

func (s *Store) Validate(resource, owner string, epoch uint64, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	lease, ok := s.leases[resource]
	if !ok {
		return model.ErrLeaseFenced
	}
	if lease.Released {
		return model.ErrLeaseFenced
	}
	// The caller must present the owner and epoch of the currently held
	// lease. A handle carrying a superseded epoch (or a different owner) is
	// fenced regardless of whether the lease deadline has lapsed.
	if lease.Owner != owner || lease.Epoch != epoch {
		return model.ErrLeaseFenced
	}
	if !now.Before(lease.Deadline) {
		return model.ErrLeaseFenced
	}
	return nil
}

func (s *Store) Release(resource, owner string, epoch uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	lease, ok := s.leases[resource]
	if !ok || lease.Owner != owner || lease.Epoch != epoch || lease.Released {
		return model.ErrLeaseFenced
	}
	lease.Released = true
	s.leases[resource] = lease
	return nil
}

func (s *Store) Lease(resource string) (model.Lease, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	lease, ok := s.leases[resource]
	return lease, ok
}
