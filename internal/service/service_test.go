package service_test

import (
	"context"
	"testing"
	"time"

	"example.com/backupmesh/internal/model"
	"example.com/backupmesh/internal/service"
)

type fixedClock struct{ now time.Time }

func (clock fixedClock) Now() time.Time { return clock.now }

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
