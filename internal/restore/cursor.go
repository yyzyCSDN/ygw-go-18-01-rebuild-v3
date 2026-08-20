package restore

import (
	"sort"
	"sync"

	"example.com/backupmesh/internal/model"
)

type CursorStore struct {
	mu      sync.Mutex
	cursors map[string]int
}

func NewCursorStore() *CursorStore { return &CursorStore{cursors: make(map[string]int)} }

func (s *CursorStore) AdvanceContiguous(planID string, received []int) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	values := append([]int(nil), received...)
	sort.Ints(values)
	cursor := s.cursors[planID]
	for _, value := range values {
		if value <= cursor {
			continue
		}
		if value != cursor+1 {
			break
		}
		cursor = value
	}
	s.cursors[planID] = cursor
	return cursor
}

func (s *CursorStore) Cursor(planID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cursors[planID]
}

type Planner struct{}

func (Planner) Build(snapshot model.Snapshot, planID string) (model.RestorePlan, error) {
	if snapshot.State == model.SnapshotExpired {
		return model.RestorePlan{}, model.ErrInvalidTransition
	}
	chunks := make([]int, len(snapshot.Chunks))
	for index, chunk := range snapshot.Chunks {
		chunks[index] = chunk.Index
	}
	sort.Ints(chunks)
	return model.RestorePlan{ID: planID, SnapshotID: snapshot.ID, Chunks: chunks, CreatedAt: snapshot.CreatedAt}, nil
}
