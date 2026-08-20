package service

import (
	"example.com/backupmesh/internal/journal"
	"example.com/backupmesh/internal/model"
	"example.com/backupmesh/internal/verify"
)

func (s *Service) VerifySnapshot(snapshotID string) (model.VerificationReceipt, error) {
	snapshot, ok := s.catalog.Snapshot(snapshotID)
	if !ok {
		return model.VerificationReceipt{}, model.ErrNotFound
	}
	if snapshot.State != model.SnapshotPublished {
		return model.VerificationReceipt{}, model.ErrInvalidTransition
	}
	digest := verify.Digest(snapshot)
	receipt := s.verifications.Prepare(snapshot, digest, s.clock.Now())
	entry := journal.Entry{Generation: snapshot.Generation, Kind: "verify", SnapshotID: snapshot.ID, Operation: snapshot.Operation, Detail: digest, RecordedAt: s.clock.Now()}
	if _, err := s.journal.Append(entry); err != nil {
		return model.VerificationReceipt{}, err
	}
	if err := s.catalog.SetState(snapshot.ID, snapshot.Generation, model.SnapshotVerified); err != nil {
		return model.VerificationReceipt{}, err
	}
	s.verifications.Commit(receipt)
	s.retries.Cancel(snapshot.ID, snapshot.Generation)
	return receipt, nil
}
