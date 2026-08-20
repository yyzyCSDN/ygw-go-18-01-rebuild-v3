package service_test

import (
	"context"
	"testing"
	"time"

	"example.com/backupmesh/internal/manifest"
	"example.com/backupmesh/internal/model"
	"example.com/backupmesh/internal/service"
)

// TestStageDuplicateIndexConflictPreservesOriginal reproduces the conflict-path
// atomicity bug: staging payload "first" at index 0, then a different payload
// "second" at the same index must be rejected with ErrConflict, but the
// rejection must not destroy the original block or the remaining reservations.
// Commit must therefore still succeed and keep the original payload, and the
// materialized manifest must reflect "first", not the rejected "second".
func TestStageDuplicateIndexConflictPreservesOriginal(t *testing.T) {
	now := time.Date(2026, 8, 18, 10, 0, 0, 0, time.UTC)
	svc := service.New(4, fixedClock{now: now})
	handle, err := svc.BeginCapture(context.Background(), "snap-conflict", "node-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.StageChunk(handle, 0, []byte("first")); err != nil {
		t.Fatalf("stage first: %v", err)
	}
	// Staging a different payload at the same index must be rejected.
	if err := svc.StageChunk(handle, 0, []byte("second")); err != model.ErrConflict {
		t.Fatalf("stage second: want ErrConflict, got %v", err)
	}
	// Commit must still succeed (no not-found) and keep the original payload.
	snapshot, err := svc.CommitCapture(handle, "", true, map[string]string{"tier": "full"})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if len(snapshot.Chunks) != 1 {
		t.Fatalf("chunks = %d, want 1", len(snapshot.Chunks))
	}
	if snapshot.Chunks[0].Index != 0 {
		t.Fatalf("chunk index = %d, want 0", snapshot.Chunks[0].Index)
	}
	if got := string(snapshot.Chunks[0].Data); got != "first" {
		t.Fatalf("chunk payload = %q, want %q", got, "first")
	}
	// The manifest must reflect the original payload, not the rejected one.
	if _, err := svc.MaterializeManifest(snapshot.ID); err != nil {
		t.Fatalf("materialize: %v", err)
	}
	payload, ok := svc.ManifestPayload(snapshot.ID)
	if !ok {
		t.Fatal("manifest payload missing")
	}
	decoded, err := manifest.Decode(payload)
	if err != nil {
		t.Fatalf("decode manifest: %v", err)
	}
	if len(decoded.Chunks) != 1 {
		t.Fatalf("manifest chunks = %d, want 1", len(decoded.Chunks))
	}
	if got := string(decoded.Chunks[0].Data); got != "first" {
		t.Fatalf("manifest payload = %q, want %q", got, "first")
	}
}

// TestStageDuplicateIndexConflictPreservesRemainingReservation ensures that
// rejecting a duplicate index leaves the other reservations and staged chunks
// intact, so the whole capture still commits with every surviving block.
func TestStageDuplicateIndexConflictPreservesRemainingReservation(t *testing.T) {
	now := time.Date(2026, 8, 18, 10, 0, 0, 0, time.UTC)
	svc := service.New(4, fixedClock{now: now})
	handle, err := svc.BeginCapture(context.Background(), "snap-conflict-multi", "node-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.StageChunk(handle, 0, []byte("first")); err != nil {
		t.Fatalf("stage first: %v", err)
	}
	if err := svc.StageChunk(handle, 1, []byte("other")); err != nil {
		t.Fatalf("stage other: %v", err)
	}
	// Reject a duplicate at index 0; index 1 and the original block must survive.
	if err := svc.StageChunk(handle, 0, []byte("second")); err != model.ErrConflict {
		t.Fatalf("stage second: want ErrConflict, got %v", err)
	}
	snapshot, err := svc.CommitCapture(handle, "", true, map[string]string{"tier": "full"})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if len(snapshot.Chunks) != 2 {
		t.Fatalf("chunks = %d, want 2", len(snapshot.Chunks))
	}
	if got := string(snapshot.Chunks[0].Data); got != "first" {
		t.Fatalf("chunk 0 = %q, want %q", got, "first")
	}
	if got := string(snapshot.Chunks[1].Data); got != "other" {
		t.Fatalf("chunk 1 = %q, want %q", got, "other")
	}
}

// TestStageDuplicateIndexIdempentRestage ensures that re-staging the SAME
// payload at the same index is not treated as a conflict (idempotent) and does
// not corrupt state. This guards against the fix over-rejecting.
func TestStageDuplicateIndexIdempotentRestage(t *testing.T) {
	now := time.Date(2026, 8, 18, 10, 0, 0, 0, time.UTC)
	svc := service.New(4, fixedClock{now: now})
	handle, err := svc.BeginCapture(context.Background(), "snap-idempotent", "node-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.StageChunk(handle, 0, []byte("first")); err != nil {
		t.Fatalf("stage first: %v", err)
	}
	// Re-staging identical bytes at the same index must succeed (no conflict).
	if err := svc.StageChunk(handle, 0, []byte("first")); err != nil {
		t.Fatalf("restage identical: %v", err)
	}
	snapshot, err := svc.CommitCapture(handle, "", true, map[string]string{"tier": "full"})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if len(snapshot.Chunks) != 1 || string(snapshot.Chunks[0].Data) != "first" {
		t.Fatalf("commit result = %+v", snapshot.Chunks)
	}
}
