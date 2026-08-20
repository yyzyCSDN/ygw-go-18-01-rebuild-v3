package checkpoint

import (
	"errors"
	"sync"
	"time"
)

type State string

const (
	Prepared  State = "prepared"
	Staged    State = "staged"
	Published State = "published"
	Verified  State = "verified"
	Restored  State = "restored"
	Aborted   State = "aborted"
)

var (
	ErrExists     = errors.New("checkpoint: record exists")
	ErrMissing    = errors.New("checkpoint: record missing")
	ErrGeneration = errors.New("checkpoint: generation mismatch")
	ErrTransition = errors.New("checkpoint: invalid transition")
)

type Record struct {
	SnapshotID string
	Operation  string
	Generation uint64
	State      State
	ChunkCount int
	Digest     string
	UpdatedAt  time.Time
}

type Store struct {
	mu      sync.RWMutex
	records map[string]Record
	history map[string][]State
}

func New() *Store {
	return &Store{records: make(map[string]Record), history: make(map[string][]State)}
}

func (s *Store) Start(snapshotID, operation string, generation uint64, now time.Time) (Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.records[snapshotID]; ok {
		return Record{}, ErrExists
	}
	record := Record{SnapshotID: snapshotID, Operation: operation, Generation: generation, State: Prepared, UpdatedAt: now.UTC()}
	s.records[snapshotID] = record
	s.history[snapshotID] = []State{Prepared}
	return record, nil
}

func (s *Store) Advance(snapshotID string, generation uint64, next State, chunks int, digest string, now time.Time) (Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.records[snapshotID]
	if !ok {
		return Record{}, ErrMissing
	}
	if record.Generation != generation {
		return Record{}, ErrGeneration
	}
	if !allowed(record.State, next) {
		return Record{}, ErrTransition
	}
	record.State = next
	record.ChunkCount = chunks
	record.Digest = digest
	record.UpdatedAt = now.UTC()
	s.records[snapshotID] = record
	s.history[snapshotID] = append(s.history[snapshotID], next)
	return record, nil
}

func (s *Store) Abort(snapshotID string, generation uint64, now time.Time) error {
	_, err := s.Advance(snapshotID, generation, Aborted, 0, "", now)
	return err
}

func (s *Store) Get(snapshotID string) (Record, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	record, ok := s.records[snapshotID]
	return record, ok
}

func (s *Store) History(snapshotID string) []State {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]State(nil), s.history[snapshotID]...)
}

func (s *Store) Active() []Record {
	s.mu.RLock()
	defer s.mu.RUnlock()
	active := make([]Record, 0, len(s.records))
	for _, record := range s.records {
		if record.State != Aborted && record.State != Restored {
			active = append(active, record)
		}
	}
	return active
}

func allowed(current, next State) bool {
	switch current {
	case Prepared:
		return next == Staged || next == Aborted
	case Staged:
		return next == Published || next == Aborted
	case Published:
		return next == Verified || next == Restored
	case Verified:
		return next == Restored || next == Aborted
	default:
		return false
	}
}
