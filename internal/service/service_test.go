package service_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"example.com/backupmesh/internal/model"
	"example.com/backupmesh/internal/service"
)

type fixedClock struct{ now time.Time }

func (clock fixedClock) Now() time.Time { return clock.now }

// manualClock is a mutable clock used to advance time across lease lifetimes.
type manualClock struct{ now time.Time }

func (clock *manualClock) Now() time.Time { return clock.now }

func TestCaptureVerifyAndRestore(t *testing.T) {
	now := time.Date(2026, 8, 18, 10, 0, 0, 0, time.UTC)
	svc := service.New(4, fixedClock{now: now})
	handle, err := svc.BeginCapture(context.Background(), "snap-1", "node-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.StageChunk(handle, 1, []byte("alpha")); err != nil {
		t.Fatal(err)
	}
	snapshot, err := svc.CommitCapture(handle, "", true, map[string]string{"tier": "full"})
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.State != model.SnapshotPublished {
		t.Fatalf("state = %s", snapshot.State)
	}
	if _, err := svc.VerifySnapshot(snapshot.ID); err != nil {
		t.Fatal(err)
	}
	plan, err := svc.BeginRestore(snapshot.ID, "restore-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	plan = svc.AdvanceRestore(plan, []int{1})
	if plan.Cursor != 1 {
		t.Fatalf("cursor = %d", plan.Cursor)
	}
}

// TestCaptureLeaseFencingAfterReacquire reproduces the fencing regression:
// after owner-a's capture lease on a snapshot expires and the same owner
// re-acquires it, a late StageChunk/CommitCapture from the old CaptureHandle
// must be rejected with ErrLeaseFenced (even though the owner is unchanged),
// while the fresh handle can still complete the capture.
func TestCaptureLeaseFencingAfterReacquire(t *testing.T) {
	clock := &manualClock{now: time.Date(2026, 8, 18, 10, 0, 0, 0, time.UTC)}
	svc := service.New(4, clock)

	// owner-a begins capturing snap-a and stages a chunk while the lease is live.
	first, err := svc.BeginCapture(context.Background(), "snap-a", "node-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.StageChunk(first, 1, []byte("alpha")); err != nil {
		t.Fatalf("live StageChunk err = %v", err)
	}

	// The lease expires. owner-a re-acquires the same snapshot. Even though the
	// owner has not changed, the new capture round must carry a fresh epoch so
	// the old handle is fenced.
	clock.now = clock.now.Add(2 * time.Minute)
	second, err := svc.BeginCapture(context.Background(), "snap-a", "node-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if second.LeaseEpoch <= first.LeaseEpoch {
		t.Fatalf("epoch did not advance on re-acquire: first=%d second=%d", first.LeaseEpoch, second.LeaseEpoch)
	}

	// A late StageChunk from the superseded handle must be rejected, not written
	// into the new capture round.
	if err := svc.StageChunk(first, 2, []byte("stale")); !errors.Is(err, model.ErrLeaseFenced) {
		t.Fatalf("stale StageChunk err = %v, want ErrLeaseFenced", err)
	}
	// A late CommitCapture from the superseded handle must likewise be fenced.
	if _, err := svc.CommitCapture(first, "", true, nil); !errors.Is(err, model.ErrLeaseFenced) {
		t.Fatalf("stale CommitCapture err = %v, want ErrLeaseFenced", err)
	}

	// The fresh handle can still stage and complete the capture normally.
	if err := svc.StageChunk(second, 1, []byte("fresh")); err != nil {
		t.Fatalf("fresh StageChunk err = %v", err)
	}
	snapshot, err := svc.CommitCapture(second, "", true, map[string]string{"tier": "full"})
	if err != nil {
		t.Fatalf("fresh CommitCapture err = %v", err)
	}
	if snapshot.State != model.SnapshotPublished {
		t.Fatalf("snapshot state = %s, want published", snapshot.State)
	}
}
