package service_test

import (
	"context"
	"testing"
	"time"

	"example.com/backupmesh/internal/journal"
	"example.com/backupmesh/internal/model"
	"example.com/backupmesh/internal/service"
)

type oracleClock08 struct{ now time.Time }

func (c oracleClock08) Now() time.Time { return c.now }

func TestReplayFencesOlderGeneration(t *testing.T) {
	svc := service.New(2, oracleClock08{time.Date(2026, 8, 18, 8, 0, 0, 0, time.UTC)})
	handle, _ := svc.BeginCapture(context.Background(), "snap-a", "owner", time.Minute)
	if err := svc.StageChunk(handle, 1, []byte("payload")); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.CommitCapture(handle, "", true, nil); err != nil {
		t.Fatal(err)
	}
	batch := []journal.Entry{
		{Sequence: 1, Generation: 2, Kind: "checkpoint", SnapshotID: "snap-a", Operation: "recovery"},
		{Sequence: 2, Generation: 1, Kind: "verify", SnapshotID: "snap-a", Operation: "recovery"},
	}
	if err := svc.Replay(batch); err != nil {
		t.Fatal(err)
	}
	if err := svc.Replay([]journal.Entry{{Sequence: 3, Generation: 1, Kind: "verify", SnapshotID: "snap-a", Operation: "recovery"}}); err != nil {
		t.Fatal(err)
	}
	snapshot, _ := svc.Snapshot("snap-a")
	if snapshot.State != model.SnapshotPublished {
		t.Fatalf("state downgraded by old replay: %s", snapshot.State)
	}
}
