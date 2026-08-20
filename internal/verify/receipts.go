package verify

import (
	"crypto/sha256"
	"encoding/hex"
	"sync"
	"time"

	"example.com/backupmesh/internal/model"
)

type Store struct {
	mu       sync.RWMutex
	receipts map[string]model.VerificationReceipt
}

func NewStore() *Store { return &Store{receipts: make(map[string]model.VerificationReceipt)} }

func Digest(snapshot model.Snapshot) string {
	hash := sha256.New()
	for _, chunk := range snapshot.Chunks {
		hash.Write([]byte(chunk.Digest))
		hash.Write(chunk.Data)
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func (s *Store) Prepare(snapshot model.Snapshot, digest string, now time.Time) model.VerificationReceipt {
	return model.VerificationReceipt{SnapshotID: snapshot.ID, Generation: snapshot.Generation, Digest: digest, VerifiedAt: now.UTC()}
}

func (s *Store) Commit(receipt model.VerificationReceipt) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.receipts[receipt.SnapshotID] = receipt
}

func (s *Store) Delete(snapshotID string, generation uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if receipt, ok := s.receipts[snapshotID]; ok && receipt.Generation == generation {
		delete(s.receipts, snapshotID)
	}
}

func (s *Store) Receipt(snapshotID string) (model.VerificationReceipt, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	receipt, ok := s.receipts[snapshotID]
	return receipt, ok
}
