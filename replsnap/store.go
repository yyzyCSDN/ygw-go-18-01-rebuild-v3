package replsnap

import (
	"errors"
	"sort"
	"sync"
)

var (
	ErrSnapshotNotFound = errors.New("snapshot not found")
	ErrInvalidManifest  = errors.New("invalid manifest")
	ErrQuorumNotReached = errors.New("replica quorum not reached")
	ErrNoRecoveryPoint  = errors.New("no recoverable snapshot")
)

type MemoryStore struct {
	mu        sync.RWMutex
	snapshots map[string]Snapshot
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{snapshots: make(map[string]Snapshot)}
}

func (store *MemoryStore) Save(snapshot Snapshot) {
	store.mu.Lock()
	store.snapshots[snapshot.Meta.ID] = cloneSnapshot(snapshot)
	store.mu.Unlock()
}

func (store *MemoryStore) Get(id string) (Snapshot, bool) {
	store.mu.RLock()
	snapshot, ok := store.snapshots[id]
	store.mu.RUnlock()
	return cloneSnapshot(snapshot), ok
}

func (store *MemoryStore) Delete(id string) {
	store.mu.Lock()
	delete(store.snapshots, id)
	store.mu.Unlock()
}

func (store *MemoryStore) Dependencies(id string) []string {
	snapshot, ok := store.Get(id)
	if !ok {
		return nil
	}
	return snapshot.Meta.Dependencies()
}

func (store *MemoryStore) List() []SnapshotMeta {
	store.mu.RLock()
	result := make([]SnapshotMeta, 0, len(store.snapshots))
	for _, snapshot := range store.snapshots {
		result = append(result, snapshot.Meta)
	}
	store.mu.RUnlock()
	sort.Slice(result, func(i, j int) bool {
		if result[i].CreatedAt.Equal(result[j].CreatedAt) {
			return result[i].ID < result[j].ID
		}
		return result[i].CreatedAt.Before(result[j].CreatedAt)
	})
	return result
}

func cloneSnapshot(snapshot Snapshot) Snapshot {
	copySnapshot := snapshot
	copySnapshot.Manifest.Chunks = append([]ChunkMeta(nil), snapshot.Manifest.Chunks...)
	copySnapshot.Chunks = make([][]byte, len(snapshot.Chunks))
	for index, chunk := range snapshot.Chunks {
		copySnapshot.Chunks[index] = append([]byte(nil), chunk...)
	}
	return copySnapshot
}
