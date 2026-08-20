package replsnap

import (
	"bytes"
	"context"
	"testing"
	"time"
)

func TestSnapshotStorageOwnsChunkBytes(t *testing.T) {
	store := NewMemoryStore()
	input := []byte("abcdef")
	service := NewService(store)
	if _, err := service.Capture(context.Background(), SnapshotMeta{ID: "s1"}, bytes.NewReader(input), 3); err != nil {
		t.Fatal(err)
	}
	input[0] = 'z'
	snapshot, _ := store.Get("s1")
	snapshot.Chunks[0][0] = 'x'
	again, _ := store.Get("s1")
	if string(again.Chunks[0]) != "abc" {
		t.Fatalf("chunk=%q", again.Chunks[0])
	}
}

func TestCapturePromoteAndPlan(t *testing.T) {
	store := NewMemoryStore()
	service := NewService(store)
	created := time.Date(2026, 8, 16, 0, 0, 0, 0, time.UTC)
	snapshot, err := service.Capture(context.Background(), SnapshotMeta{ID: "s2", CreatedAt: created}, bytes.NewReader([]byte("abcdefgh")), 4)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyManifest(snapshot.Manifest, snapshot.Chunks); err != nil {
		t.Fatal(err)
	}
	receipt, err := service.Promote("s2", 3, []string{"a", "b"})
	if err != nil || !receipt.Promoted {
		t.Fatalf("receipt=%+v err=%v", receipt, err)
	}
	plan, err := service.PlanRestore([]ReplicaInventory{{ReplicaID: "a", SnapshotIDs: []string{"s2"}}, {ReplicaID: "b", SnapshotIDs: []string{"s2"}}}, RecoveryPolicy{MinimumCopies: 2})
	if err != nil || plan.SnapshotID != "s2" || plan.Copies != 2 {
		t.Fatalf("plan=%+v err=%v", plan, err)
	}
}
