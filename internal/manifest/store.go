package manifest

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"example.com/backupmesh/internal/model"
)

var (
	ErrMissing = errors.New("manifest: missing snapshot")
	ErrDigest  = errors.New("manifest: digest mismatch")
)

type Envelope struct {
	Version   uint64         `json:"version"`
	Snapshot  model.Snapshot `json:"snapshot"`
	Digest    string         `json:"digest"`
	Published time.Time      `json:"published_at"`
}

type Store struct {
	mu       sync.RWMutex
	versions map[string]uint64
	payloads map[string][]byte
	entries  map[string]Envelope
}

func New() *Store {
	return &Store{versions: make(map[string]uint64), payloads: make(map[string][]byte), entries: make(map[string]Envelope)}
}

func (s *Store) Publish(snapshot model.Snapshot, now time.Time) (Envelope, error) {
	payload, digest, err := Encode(snapshot)
	if err != nil {
		return Envelope{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	version := s.versions[snapshot.ID] + 1
	envelope := Envelope{Version: version, Snapshot: snapshot, Digest: digest, Published: now.UTC()}
	s.versions[snapshot.ID] = version
	s.payloads[snapshot.ID] = append([]byte(nil), payload...)
	s.entries[snapshot.ID] = envelope
	return envelope, nil
}

func (s *Store) Load(snapshotID string) (Envelope, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	envelope, ok := s.entries[snapshotID]
	return envelope, ok
}

func (s *Store) Payload(snapshotID string) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	payload, ok := s.payloads[snapshotID]
	return append([]byte(nil), payload...), ok
}

func (s *Store) Verify(snapshotID string) error {
	s.mu.RLock()
	payload, ok := s.payloads[snapshotID]
	envelope := s.entries[snapshotID]
	s.mu.RUnlock()
	if !ok {
		return ErrMissing
	}
	return VerifyPayload(payload, envelope.Digest)
}

func Encode(snapshot model.Snapshot) ([]byte, string, error) {
	payload, err := json.Marshal(snapshot)
	if err != nil {
		return nil, "", err
	}
	hash := sha256.Sum256(payload)
	return payload, hex.EncodeToString(hash[:]), nil
}

func Decode(payload []byte) (model.Snapshot, error) {
	var snapshot model.Snapshot
	if err := json.Unmarshal(payload, &snapshot); err != nil {
		return model.Snapshot{}, err
	}
	return snapshot, nil
}

func VerifyPayload(payload []byte, expected string) error {
	hash := sha256.Sum256(payload)
	actual := hex.EncodeToString(hash[:])
	if actual != expected {
		return ErrDigest
	}
	return nil
}

func EqualPayload(left, right []byte) bool { return bytes.Equal(left, right) }
