package service

import (
	"sync"
	"sync/atomic"
	"time"

	"example.com/backupmesh/internal/audit"
	"example.com/backupmesh/internal/catalog"
	"example.com/backupmesh/internal/checkpoint"
	"example.com/backupmesh/internal/journal"
	"example.com/backupmesh/internal/lease"
	"example.com/backupmesh/internal/manifest"
	"example.com/backupmesh/internal/model"
	"example.com/backupmesh/internal/policy"
	"example.com/backupmesh/internal/restore"
	"example.com/backupmesh/internal/retry"
	"example.com/backupmesh/internal/telemetry"
	"example.com/backupmesh/internal/verify"
)

type Service struct {
	catalog       *catalog.Store
	journal       *journal.Log
	leases        *lease.Store
	retries       *retry.Queue
	cursors       *restore.CursorStore
	verifications *verify.Store
	checkpoints   *checkpoint.Store
	manifests     *manifest.Store
	policies      *policy.Engine
	audit         *audit.Ledger
	telemetry     *telemetry.Registry
	clock         model.Clock

	mu                sync.Mutex
	replayGenerations map[string]uint64
	generation        atomic.Uint64
}

func (s *Service) RestoreCursor(planID string) int { return s.cursors.Cursor(planID) }

func New(maxChunks int, clock model.Clock) *Service {
	if clock == nil {
		clock = model.SystemClock{}
	}
	return &Service{
		catalog:           catalog.New(maxChunks),
		journal:           journal.New(),
		leases:            lease.New(),
		retries:           retry.New(),
		cursors:           restore.NewCursorStore(),
		verifications:     verify.NewStore(),
		checkpoints:       checkpoint.New(),
		manifests:         manifest.New(),
		policies:          policy.New(),
		audit:             audit.New(),
		telemetry:         telemetry.New(64),
		clock:             clock,
		replayGenerations: make(map[string]uint64),
	}
}

func (s *Service) CloseJournal() { s.journal.Close() }
func (s *Service) OpenJournal()  { s.journal.Open() }

func (s *Service) Snapshot(id string) (model.Snapshot, bool) { return s.catalog.Snapshot(id) }

func (s *Service) Receipt(id string) (model.VerificationReceipt, bool) {
	return s.verifications.Receipt(id)
}

func (s *Service) RetryPending(snapshotID string) int { return s.retries.Pending(snapshotID) }

func (s *Service) JournalEntries() []journal.Entry { return s.journal.Entries() }

// ReplayGeneration reports the highest generation replayed for an operation,
// the fence below which stale entries are ignored. Returns false when no entry
// for the operation has been replayed yet.
func (s *Service) ReplayGeneration(operation string) (uint64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	gen, ok := s.replayGenerations[operation]
	return gen, ok
}

func (s *Service) Lease(resource string) (model.Lease, bool) { return s.leases.Lease(resource) }

func (s *Service) MaterializeManifest(snapshotID string) (manifest.Envelope, error) {
	snapshot, ok := s.catalog.Snapshot(snapshotID)
	if !ok {
		return manifest.Envelope{}, model.ErrNotFound
	}
	envelope, err := s.manifests.Publish(snapshot, s.clock.Now())
	if err != nil {
		return manifest.Envelope{}, err
	}
	if _, exists := s.checkpoints.Get(snapshot.ID); !exists {
		if _, err := s.checkpoints.Start(snapshot.ID, snapshot.Operation, snapshot.Generation, s.clock.Now()); err != nil {
			return manifest.Envelope{}, err
		}
		if _, err := s.checkpoints.Advance(snapshot.ID, snapshot.Generation, checkpoint.Staged, len(snapshot.Chunks), envelope.Digest, s.clock.Now()); err != nil {
			return manifest.Envelope{}, err
		}
		if _, err := s.checkpoints.Advance(snapshot.ID, snapshot.Generation, checkpoint.Published, len(snapshot.Chunks), envelope.Digest, s.clock.Now()); err != nil {
			return manifest.Envelope{}, err
		}
	}
	if snapshot.State == model.SnapshotVerified {
		record, _ := s.checkpoints.Get(snapshot.ID)
		if record.State == checkpoint.Published {
			if _, err := s.checkpoints.Advance(snapshot.ID, snapshot.Generation, checkpoint.Verified, len(snapshot.Chunks), envelope.Digest, s.clock.Now()); err != nil {
				return manifest.Envelope{}, err
			}
		}
	}
	s.audit.Record("manifest-published", snapshot.ID, envelope.Digest, s.clock.Now())
	return envelope, nil
}

func (s *Service) VerifyMaterializedManifest(snapshotID string) error {
	if err := s.manifests.Verify(snapshotID); err != nil {
		return err
	}
	s.audit.Record("manifest-verified", snapshotID, "payload digest matched", s.clock.Now())
	return nil
}

func (s *Service) ManifestPayload(snapshotID string) ([]byte, bool) {
	return s.manifests.Payload(snapshotID)
}

func (s *Service) Checkpoint(snapshotID string) (checkpoint.Record, bool) {
	return s.checkpoints.Get(snapshotID)
}

func (s *Service) CheckpointHistory(snapshotID string) []checkpoint.State {
	return s.checkpoints.History(snapshotID)
}

func (s *Service) AuditEvents() []audit.Event { return s.audit.Events() }
func (s *Service) VerifyAudit() error         { return s.audit.Verify() }

func (s *Service) AuditEventsFor(snapshotID string) []audit.Event {
	return s.audit.EventsFor(snapshotID)
}

func (s *Service) Health(now time.Time) telemetry.Health {
	return s.telemetry.HealthSnapshot(now)
}

func (s *Service) HealthJSON(now time.Time) ([]byte, error) {
	return telemetry.MarshalHealth(s.Health(now))
}

func (s *Service) OperationCount(name string) uint64 { return s.telemetry.Count(name) }

func (s *Service) RetentionPolicy() policy.Retention { return s.policies.RetentionPolicy() }
func (s *Service) RetryPolicy() policy.Retry         { return s.policies.RetryPolicy() }

func (s *Service) NextRetry(task model.RetryTask, now time.Time) (time.Time, error) {
	return s.policies.NextRetry(task, now)
}
