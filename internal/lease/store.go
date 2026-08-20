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
	epoch := s.epochs[resource]
	if epoch == 0 {
		epoch = 1
		s.epochs[resource] = epoch
	}
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
