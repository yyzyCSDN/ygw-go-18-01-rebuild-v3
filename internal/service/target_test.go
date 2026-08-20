package service_test

import (
	"context"
	"testing"
	"time"

	"example.com/backupmesh/internal/model"
	"example.com/backupmesh/internal/service"
)

type oracleClock07 struct{ now time.Time }

func (c *oracleClock07) Now() time.Time { return c.now }

func captureVerified07(t *testing.T, svc *service.Service, id string) {
	t.Helper()
	handle, err := svc.BeginCapture(context.Background(), id, "owner-"+id, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.StageChunk(handle, 1, []byte(id)); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.CommitCapture(handle, "", true, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.VerifySnapshot(id); err != nil {
		t.Fatal(err)
	}
}

func TestRetentionPreservesActiveRestore(t *testing.T) {
	clock := &oracleClock07{now: time.Date(2026, 8, 18, 7, 0, 0, 0, time.UTC)}
	svc := service.New(2, clock)
	captureVerified07(t, svc, "snap-old")
	clock.now = clock.now.Add(time.Minute)
	captureVerified07(t, svc, "snap-new")
	if _, err := svc.BeginRestore("snap-old", "restore", time.Minute); err != nil {
		t.Fatal(err)
	}
	for _, id := range svc.ApplyRetention(1) {
		if id == "snap-old" {
			t.Fatal("active restore snapshot expired")
		}
	}
	snapshot, _ := svc.Snapshot("snap-old")
	if snapshot.State == model.SnapshotExpired {
		t.Fatal("active snapshot state is expired")
	}
}
