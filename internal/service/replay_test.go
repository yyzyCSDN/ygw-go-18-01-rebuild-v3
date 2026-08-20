package service_test

import (
	"context"
	"testing"
	"time"

	"example.com/backupmesh/internal/journal"
	"example.com/backupmesh/internal/model"
	"example.com/backupmesh/internal/service"
)

// stagePublishedSnapshot captures and publishes a snapshot so the catalog holds
// snapID at generation 1 in the published state, and returns the capture
// operation under which its journal entries are recorded.
func stagePublishedSnapshot(t *testing.T, svc *service.Service, snapID string) string {
	t.Helper()
	handle, err := svc.BeginCapture(context.Background(), snapID, "owner", time.Minute)
	if err != nil {
		t.Fatalf("begin capture: %v", err)
	}
	if err := svc.StageChunk(handle, 0, []byte("payload")); err != nil {
		t.Fatalf("stage chunk: %v", err)
	}
	if _, err := svc.CommitCapture(handle, "", true, nil); err != nil {
		t.Fatalf("commit capture: %v", err)
	}
	if handle.Generation != 1 {
		t.Fatalf("handle generation = %d, want 1", handle.Generation)
	}
	return handle.Operation
}

func assertSnapshotState(t *testing.T, svc *service.Service, snapID string, want model.SnapshotState) {
	t.Helper()
	snap, ok := svc.Snapshot(snapID)
	if !ok {
		t.Fatalf("snapshot %s not found", snapID)
	}
	if snap.State != want {
		t.Fatalf("snapshot %s state = %s, want %s", snapID, snap.State, want)
	}
}

func assertReplayGeneration(t *testing.T, svc *service.Service, operation string, want uint64) {
	t.Helper()
	got, ok := svc.ReplayGeneration(operation)
	if !ok {
		t.Fatalf("replay generation for %s not recorded", operation)
	}
	if got != want {
		t.Fatalf("replay generation for %s = %d, want %d", operation, got, want)
	}
}

// TestReplayFencesStaleVerifyByGenerationSingleCall reproduces the reported bug
// within a single replay: the same operation first sees a generation-2
// checkpoint (lower sequence) and then a stale generation-1 verify with a
// larger sequence. The newer generation must win, so the stale verify is fenced
// out and the snapshot stays published.
func TestReplayFencesStaleVerifyByGenerationSingleCall(t *testing.T) {
	svc := service.New(4, fixedClock{now: time.Date(2026, 8, 19, 10, 0, 0, 0, time.UTC)})
	op := stagePublishedSnapshot(t, svc, "snap-1")
	assertSnapshotState(t, svc, "snap-1", model.SnapshotPublished)

	entries := []journal.Entry{
		{Sequence: 3, Generation: 2, Kind: "publish", SnapshotID: "snap-1", Operation: op},
		{Sequence: 4, Generation: 1, Kind: "verify", SnapshotID: "snap-1", Operation: op},
	}
	if err := svc.Replay(entries); err != nil {
		t.Fatalf("replay: %v", err)
	}
	assertSnapshotState(t, svc, "snap-1", model.SnapshotPublished)
	assertReplayGeneration(t, svc, op, 2)
}

// TestReplayFencesStaleVerifyByGenerationBatched covers the batched replay
// scenario. The generation-2 checkpoint in the first batch raises the fence;
// replaying an old generation-1 verify in a later batch (with a larger
// sequence) must not lower the fence or mark the snapshot verified, even when
// replayed again.
func TestReplayFencesStaleVerifyByGenerationBatched(t *testing.T) {
	svc := service.New(4, fixedClock{now: time.Date(2026, 8, 19, 10, 0, 0, 0, time.UTC)})
	op := stagePublishedSnapshot(t, svc, "snap-1")
	assertSnapshotState(t, svc, "snap-1", model.SnapshotPublished)

	// Batch 1: the generation-2 checkpoint raises the fence to 2.
	if err := svc.Replay([]journal.Entry{
		{Sequence: 3, Generation: 2, Kind: "publish", SnapshotID: "snap-1", Operation: op},
	}); err != nil {
		t.Fatalf("replay batch 1: %v", err)
	}
	assertSnapshotState(t, svc, "snap-1", model.SnapshotPublished)
	assertReplayGeneration(t, svc, op, 2)

	// Batch 2: a stale generation-1 verify with a larger sequence must not lower
	// the fence or mark the snapshot verified.
	if err := svc.Replay([]journal.Entry{
		{Sequence: 4, Generation: 1, Kind: "verify", SnapshotID: "snap-1", Operation: op},
	}); err != nil {
		t.Fatalf("replay batch 2: %v", err)
	}
	assertSnapshotState(t, svc, "snap-1", model.SnapshotPublished)
	assertReplayGeneration(t, svc, op, 2)

	// Batch 3: replaying the stale generation-1 verify again is still a no-op.
	if err := svc.Replay([]journal.Entry{
		{Sequence: 5, Generation: 1, Kind: "verify", SnapshotID: "snap-1", Operation: op},
	}); err != nil {
		t.Fatalf("replay batch 3: %v", err)
	}
	assertSnapshotState(t, svc, "snap-1", model.SnapshotPublished)
	assertReplayGeneration(t, svc, op, 2)
}

// TestReplayAppliesVerifyWithinSameGeneration ensures the fence does not reject
// equal generations: within a generation the higher-sequence verify is the
// latest entry and must be applied normally.
func TestReplayAppliesVerifyWithinSameGeneration(t *testing.T) {
	svc := service.New(4, fixedClock{now: time.Date(2026, 8, 19, 10, 0, 0, 0, time.UTC)})
	op := stagePublishedSnapshot(t, svc, "snap-1")
	assertSnapshotState(t, svc, "snap-1", model.SnapshotPublished)

	entries := []journal.Entry{
		{Sequence: 1, Generation: 1, Kind: "publish", SnapshotID: "snap-1", Operation: op},
		{Sequence: 2, Generation: 1, Kind: "verify", SnapshotID: "snap-1", Operation: op},
	}
	if err := svc.Replay(entries); err != nil {
		t.Fatalf("replay: %v", err)
	}
	assertSnapshotState(t, svc, "snap-1", model.SnapshotVerified)
	assertReplayGeneration(t, svc, op, 1)
}
