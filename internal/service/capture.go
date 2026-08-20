package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/google/go-cmp/cmp"

	"example.com/backupmesh/internal/journal"
	"example.com/backupmesh/internal/model"
)

func (s *Service) BeginCapture(ctx context.Context, snapshotID, owner string, ttl time.Duration) (model.CaptureHandle, error) {
	if err := ctx.Err(); err != nil {
		return model.CaptureHandle{}, err
	}
	if snapshotID == "" || owner == "" || ttl <= 0 {
		return model.CaptureHandle{}, model.ErrConflict
	}
	resource := "capture:" + snapshotID
	acquired, err := s.leases.Acquire(resource, owner, s.clock.Now(), ttl)
	if err != nil {
		return model.CaptureHandle{}, err
	}
	return model.CaptureHandle{Operation: model.NewID("capture"), SnapshotID: snapshotID, Owner: owner, LeaseEpoch: acquired.Epoch, Generation: s.generation.Add(1)}, nil
}

func (s *Service) StageChunk(handle model.CaptureHandle, index int, data []byte) error {
	resource := "capture:" + handle.SnapshotID
	// Fence late operations from a superseded capture round. Validate checks
	// existence, release, deadline, owner, and epoch, so a handle whose epoch
	// predates the currently held lease (e.g. after a re-acquire on the same
	// snapshot) is rejected with ErrLeaseFenced rather than written.
	if err := s.leases.Validate(resource, handle.Owner, handle.LeaseEpoch, s.clock.Now()); err != nil {
		return err
	}
	sum := sha256.Sum256(data)
	digest := hex.EncodeToString(sum[:])
	if err := s.catalog.ReserveDigest(handle.Operation, digest); err != nil {
		return err
	}
	chunk := model.Chunk{Index: index, Digest: digest, Data: append([]byte(nil), data...)}
	if err := s.catalog.StageChunk(handle.Operation, chunk); err != nil {
		s.catalog.ReleaseDigest(handle.Operation, digest)
		return err
	}
	return nil
}

func (s *Service) CommitCapture(handle model.CaptureHandle, baseID string, full bool, metadata map[string]string) (model.Snapshot, error) {
	resource := "capture:" + handle.SnapshotID
	if err := s.leases.Validate(resource, handle.Owner, handle.LeaseEpoch, s.clock.Now()); err != nil {
		return model.Snapshot{}, err
	}
	now := s.clock.Now()
	detail := "initial snapshot"
	if base, ok := s.catalog.Snapshot(baseID); ok {
		detail = cmp.Diff(base.Metadata, metadata)
	}
	entry := journal.Entry{Generation: handle.Generation, Kind: "publish", SnapshotID: handle.SnapshotID, Operation: handle.Operation, Detail: detail, RecordedAt: now}
	if _, err := s.journal.Append(entry); err != nil {
		s.catalog.ForgetOperation(handle.Operation)
		_ = s.leases.Release(resource, handle.Owner, handle.LeaseEpoch)
		return model.Snapshot{}, err
	}
	snapshot := model.Snapshot{ID: handle.SnapshotID, BaseID: baseID, Operation: handle.Operation, Generation: handle.Generation, Full: full, State: model.SnapshotStaged, Metadata: cloneMetadata(metadata), CreatedAt: now}
	if err := s.catalog.CommitSnapshot(snapshot); err != nil {
		s.catalog.ForgetOperation(handle.Operation)
		_ = s.leases.Release(resource, handle.Owner, handle.LeaseEpoch)
		return model.Snapshot{}, err
	}
	if err := s.leases.Release(resource, handle.Owner, handle.LeaseEpoch); err != nil {
		return model.Snapshot{}, err
	}
	committed, _ := s.catalog.Snapshot(handle.SnapshotID)
	return committed, nil
}

func (s *Service) AbortCapture(handle model.CaptureHandle) error {
	s.catalog.ForgetOperation(handle.Operation)
	return s.leases.Release("capture:"+handle.SnapshotID, handle.Owner, handle.LeaseEpoch)
}

func cloneMetadata(values map[string]string) map[string]string {
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}
