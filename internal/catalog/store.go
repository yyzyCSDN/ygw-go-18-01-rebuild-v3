package catalog

import (
	"sort"
	"sync"

	"example.com/backupmesh/internal/model"
)

type Store struct {
	mu            sync.RWMutex
	snapshots     map[string]model.Snapshot
	staged        map[string]map[int]model.Chunk
	conflicts     map[string][]model.Chunk
	reservations  map[string]string
	restoreRefs   map[string]map[string]struct{}
	maxChunkCount int
}

func New(maxChunkCount int) *Store {
	if maxChunkCount < 1 {
		maxChunkCount = 64
	}
	return &Store{snapshots: make(map[string]model.Snapshot), staged: make(map[string]map[int]model.Chunk), conflicts: make(map[string][]model.Chunk), reservations: make(map[string]string), restoreRefs: make(map[string]map[string]struct{}), maxChunkCount: maxChunkCount}
}

func (s *Store) ReserveDigest(operation, digest string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if owner, ok := s.reservations[digest]; ok && owner != operation {
		return model.ErrConflict
	}
	s.reservations[digest] = operation
	return nil
}

func (s *Store) StageChunk(operation string, chunk model.Chunk) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	chunks := s.staged[operation]
	if chunks == nil {
		chunks = make(map[int]model.Chunk)
		s.staged[operation] = chunks
	}
	if _, exists := chunks[chunk.Index]; !exists && len(chunks) >= s.maxChunkCount {
		return model.ErrCapacity
	}
	if existing, exists := chunks[chunk.Index]; exists && existing.Digest != chunk.Digest {
		// A duplicate index with a different payload is rejected. Record the
		// collision for audit but leave the original block untouched: the
		// conflict path must be atomic so the surviving reservation and the
		// already-staged chunks remain usable for commit.
		s.conflicts[operation] = append(s.conflicts[operation], cloneChunk(existing), cloneChunk(chunk))
		return model.ErrConflict
	}
	chunks[chunk.Index] = cloneChunk(chunk)
	return nil
}

func (s *Store) ConflictHistory(operation string) []model.Chunk {
	s.mu.RLock()
	defer s.mu.RUnlock()
	values := s.conflicts[operation]
	result := make([]model.Chunk, len(values))
	for index, value := range values {
		result[index] = cloneChunk(value)
	}
	return result
}

func (s *Store) ConflictCount(operation string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.conflicts[operation]) / 2
}

func (s *Store) ReleaseDigest(operation, digest string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.reservations[digest] == operation {
		delete(s.reservations, digest)
	}
}

// ReleaseDigestIfUnused releases the reservation for digest held by operation
// only when no staged chunk belonging to that operation still references the
// digest. This keeps the rollback of a rejected StageChunk atomic: a digest
// shared with a surviving staged chunk keeps its reservation, while a digest
// that belonged solely to the rejected chunk is rolled back.
func (s *Store) ReleaseDigestIfUnused(operation, digest string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.reservations[digest] != operation {
		return
	}
	for _, chunk := range s.staged[operation] {
		if chunk.Digest == digest {
			return
		}
	}
	delete(s.reservations, digest)
}

func (s *Store) ForgetOperation(operation string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.staged, operation)
	for digest, owner := range s.reservations {
		if owner == operation {
			delete(s.reservations, digest)
		}
	}
}

func (s *Store) CommitSnapshot(snapshot model.Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.snapshots[snapshot.ID]; exists {
		return model.ErrConflict
	}
	chunks := s.staged[snapshot.Operation]
	if len(chunks) == 0 {
		return model.ErrNotFound
	}
	snapshot.Chunks = orderedChunks(chunks)
	snapshot.State = model.SnapshotPublished
	s.snapshots[snapshot.ID] = cloneSnapshot(snapshot)
	delete(s.staged, snapshot.Operation)
	for digest, owner := range s.reservations {
		if owner == snapshot.Operation {
			delete(s.reservations, digest)
		}
	}
	return nil
}

func (s *Store) Snapshot(id string) (model.Snapshot, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	snapshot, ok := s.snapshots[id]
	return cloneSnapshot(snapshot), ok
}

func (s *Store) SetState(id string, generation uint64, state model.SnapshotState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	snapshot, ok := s.snapshots[id]
	if !ok {
		return model.ErrNotFound
	}
	if snapshot.Generation != generation {
		return model.ErrConflict
	}
	snapshot.State = state
	s.snapshots[id] = snapshot
	return nil
}

func (s *Store) AddRestoreReference(planID, snapshotID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	refs := s.restoreRefs[snapshotID]
	if refs == nil {
		refs = make(map[string]struct{})
		s.restoreRefs[snapshotID] = refs
	}
	refs[planID] = struct{}{}
}

func (s *Store) RemoveRestoreReference(planID, snapshotID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.restoreRefs[snapshotID], planID)
	if len(s.restoreRefs[snapshotID]) == 0 {
		delete(s.restoreRefs, snapshotID)
	}
}

func (s *Store) ExpireUnprotected(keep int) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	all := make([]model.Snapshot, 0, len(s.snapshots))
	for _, snapshot := range s.snapshots {
		all = append(all, snapshot)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].CreatedAt.After(all[j].CreatedAt) })
	expired := make([]string, 0)
	for index, snapshot := range all {
		if index < keep || len(s.restoreRefs[snapshot.ID]) > 0 {
			continue
		}
		snapshot.State = model.SnapshotExpired
		s.snapshots[snapshot.ID] = snapshot
		expired = append(expired, snapshot.ID)
	}
	sort.Strings(expired)
	return expired
}

func (s *Store) LastVerifiedFull(beforeGeneration uint64) (model.Snapshot, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var selected model.Snapshot
	found := false
	for _, snapshot := range s.snapshots {
		if !snapshot.Full || snapshot.State != model.SnapshotVerified || snapshot.Generation >= beforeGeneration {
			continue
		}
		if !found || snapshot.Generation > selected.Generation {
			selected, found = snapshot, true
		}
	}
	return cloneSnapshot(selected), found
}

func orderedChunks(values map[int]model.Chunk) []model.Chunk {
	indexes := make([]int, 0, len(values))
	for index := range values {
		indexes = append(indexes, index)
	}
	sort.Ints(indexes)
	chunks := make([]model.Chunk, 0, len(indexes))
	for _, index := range indexes {
		chunks = append(chunks, cloneChunk(values[index]))
	}
	return chunks
}

func cloneChunk(chunk model.Chunk) model.Chunk {
	chunk.Data = append([]byte(nil), chunk.Data...)
	return chunk
}

func cloneSnapshot(snapshot model.Snapshot) model.Snapshot {
	metadata := make(map[string]string, len(snapshot.Metadata))
	for key, value := range snapshot.Metadata {
		metadata[key] = value
	}
	snapshot.Metadata = metadata
	snapshot.Chunks = append([]model.Chunk(nil), snapshot.Chunks...)
	for index := range snapshot.Chunks {
		snapshot.Chunks[index] = cloneChunk(snapshot.Chunks[index])
	}
	return snapshot
}
