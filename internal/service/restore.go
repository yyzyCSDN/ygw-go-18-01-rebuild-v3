package service

import (
	"time"

	"example.com/backupmesh/internal/journal"
	"example.com/backupmesh/internal/model"
	"example.com/backupmesh/internal/restore"
)

func (s *Service) BeginRestore(snapshotID, owner string, ttl time.Duration) (model.RestorePlan, error) {
	snapshot, ok := s.catalog.Snapshot(snapshotID)
	if !ok {
		return model.RestorePlan{}, model.ErrNotFound
	}
	planID := model.NewID("restore")
	resource := "restore:" + snapshotID
	if _, err := s.leases.Acquire(resource, owner, s.clock.Now(), ttl); err != nil {
		return model.RestorePlan{}, err
	}
	plan, err := (restore.Planner{}).Build(snapshot, planID)
	if err != nil {
		lease, _ := s.leases.Lease(resource)
		_ = s.leases.Release(resource, owner, lease.Epoch)
		return model.RestorePlan{}, err
	}
	s.catalog.AddRestoreReference(plan.ID, snapshotID)
	return plan, nil
}

func (s *Service) AdvanceRestore(plan model.RestorePlan, received []int) model.RestorePlan {
	plan.Cursor = s.cursors.AdvanceContiguous(plan.ID, received)
	return plan
}

func (s *Service) FinishRestore(plan model.RestorePlan, owner string) error {
	resource := "restore:" + plan.SnapshotID
	active, ok := s.leases.Lease(resource)
	if !ok {
		return model.ErrNotFound
	}
	if _, err := s.journal.Append(journal.Entry{Generation: active.Epoch, Kind: "restore-complete", SnapshotID: plan.SnapshotID, Operation: plan.ID, RecordedAt: s.clock.Now()}); err != nil {
		return err
	}
	if err := s.leases.Release(resource, owner, active.Epoch); err != nil {
		return err
	}
	s.catalog.RemoveRestoreReference(plan.ID, plan.SnapshotID)
	return nil
}

func (s *Service) ApplyRetention(keep int) []string { return s.catalog.ExpireUnprotected(keep) }

func (s *Service) Rollback(snapshotID string) (model.Snapshot, error) {
	current, ok := s.catalog.Snapshot(snapshotID)
	if !ok {
		return model.Snapshot{}, model.ErrNotFound
	}
	target, ok := s.catalog.LastVerifiedFull(current.Generation)
	if !ok {
		return model.Snapshot{}, model.ErrNotFound
	}
	entry := journal.Entry{Generation: current.Generation, Kind: "rollback", SnapshotID: target.ID, Operation: current.Operation, Detail: target.ID, RecordedAt: s.clock.Now()}
	if _, err := s.journal.Append(entry); err != nil {
		return model.Snapshot{}, err
	}
	return target, nil
}

func (s *Service) Replay(entries []journal.Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, entry := range journal.LatestByOperation(entries) {
		if err := s.applyReplayEntry(entry); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) applyReplayEntry(entry journal.Entry) error {
	current := s.replayGenerations[entry.Operation]
	if entry.Generation < current {
		// Generation fence: a stale generation must not overwrite state
		// established by a newer one, even if its sequence is larger. This also
		// protects batched replays where a later batch carries an old event.
		return nil
	}
	s.replayGenerations[entry.Operation] = entry.Generation
	if entry.Kind != "verify" {
		return nil
	}
	err := s.catalog.SetState(entry.SnapshotID, entry.Generation, model.SnapshotVerified)
	if err == model.ErrNotFound {
		return nil
	}
	return err
}
