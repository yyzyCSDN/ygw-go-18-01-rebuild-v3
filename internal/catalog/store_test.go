package catalog

import (
	"testing"

	"example.com/backupmesh/internal/model"
)

// TestStageChunkConflictPreservesOriginal verifies the catalog-level atomicity
// fix: rejecting a duplicate index with a different payload must record the
// collision but leave the original block untouched, so commit still materializes
// the original chunk.
func TestStageChunkConflictPreservesOriginal(t *testing.T) {
	store := New(4)
	op := "op-1"

	original := model.Chunk{Index: 0, Digest: "d-first", Data: []byte("first")}
	if err := store.StageChunk(op, original); err != nil {
		t.Fatalf("stage original: %v", err)
	}
	duplicate := model.Chunk{Index: 0, Digest: "d-second", Data: []byte("second")}
	if err := store.StageChunk(op, duplicate); err != model.ErrConflict {
		t.Fatalf("stage duplicate: want ErrConflict, got %v", err)
	}
	// The collision must be recorded exactly once.
	if got := store.ConflictCount(op); got != 1 {
		t.Fatalf("conflict count = %d, want 1", got)
	}
	history := store.ConflictHistory(op)
	if len(history) != 2 {
		t.Fatalf("conflict history len = %d, want 2", len(history))
	}
	if string(history[0].Data) != "first" || string(history[1].Data) != "second" {
		t.Fatalf("conflict history = %q / %q", history[0].Data, history[1].Data)
	}
	// Commit must succeed and keep the original block, not the rejected one.
	snapshot := model.Snapshot{ID: "snap", Operation: op, State: model.SnapshotStaged}
	if err := store.CommitSnapshot(snapshot); err != nil {
		t.Fatalf("commit: %v", err)
	}
	committed, ok := store.Snapshot("snap")
	if !ok {
		t.Fatal("snapshot not found after commit")
	}
	if len(committed.Chunks) != 1 {
		t.Fatalf("chunks = %d, want 1", len(committed.Chunks))
	}
	if committed.Chunks[0].Index != 0 {
		t.Fatalf("chunk index = %d, want 0", committed.Chunks[0].Index)
	}
	if got := committed.Chunks[0].Digest; got != "d-first" {
		t.Fatalf("chunk digest = %q, want %q", got, "d-first")
	}
	if got := string(committed.Chunks[0].Data); got != "first" {
		t.Fatalf("chunk data = %q, want %q", got, "first")
	}
}

// TestStageChunkConflictKeepsOtherIndices ensures that a conflict at one index
// leaves the other staged chunks untouched.
func TestStageChunkConflictKeepsOtherIndices(t *testing.T) {
	store := New(4)
	op := "op-1"
	if err := store.StageChunk(op, model.Chunk{Index: 0, Digest: "d-first", Data: []byte("first")}); err != nil {
		t.Fatalf("stage 0: %v", err)
	}
	if err := store.StageChunk(op, model.Chunk{Index: 1, Digest: "d-other", Data: []byte("other")}); err != nil {
		t.Fatalf("stage 1: %v", err)
	}
	if err := store.StageChunk(op, model.Chunk{Index: 0, Digest: "d-second", Data: []byte("second")}); err != model.ErrConflict {
		t.Fatalf("stage duplicate: want ErrConflict, got %v", err)
	}
	snapshot := model.Snapshot{ID: "snap-multi", Operation: op, State: model.SnapshotStaged}
	if err := store.CommitSnapshot(snapshot); err != nil {
		t.Fatalf("commit: %v", err)
	}
	committed, _ := store.Snapshot("snap-multi")
	if len(committed.Chunks) != 2 {
		t.Fatalf("chunks = %d, want 2", len(committed.Chunks))
	}
	if string(committed.Chunks[0].Data) != "first" || committed.Chunks[0].Index != 0 {
		t.Fatalf("chunk 0 = %+v", committed.Chunks[0])
	}
	if string(committed.Chunks[1].Data) != "other" || committed.Chunks[1].Index != 1 {
		t.Fatalf("chunk 1 = %+v", committed.Chunks[1])
	}
}

// TestStageChunkIdempotentRestage verifies that re-staging identical bytes at
// the same index is not treated as a conflict.
func TestStageChunkIdempotentRestage(t *testing.T) {
	store := New(4)
	op := "op-1"
	first := model.Chunk{Index: 0, Digest: "d-first", Data: []byte("first")}
	if err := store.StageChunk(op, first); err != nil {
		t.Fatalf("stage first: %v", err)
	}
	if err := store.StageChunk(op, first); err != nil {
		t.Fatalf("restage identical: %v", err)
	}
	if got := store.ConflictCount(op); got != 0 {
		t.Fatalf("conflict count = %d, want 0", got)
	}
}

// TestReleaseDigestIfUnused verifies that the reservation rollback used by the
// conflict path releases a digest that no surviving staged chunk references,
// while preserving a reservation that is shared with a surviving chunk.
func TestReleaseDigestIfUnused(t *testing.T) {
	store := New(4)
	op := "op-1"
	if err := store.ReserveDigest(op, "d-shared"); err != nil {
		t.Fatalf("reserve shared: %v", err)
	}
	if err := store.ReserveDigest(op, "d-solo"); err != nil {
		t.Fatalf("reserve solo: %v", err)
	}
	// Stage a chunk that claims the shared digest; it must survive rollback.
	if err := store.StageChunk(op, model.Chunk{Index: 0, Digest: "d-shared", Data: []byte("x")}); err != nil {
		t.Fatalf("stage shared: %v", err)
	}
	// A digest with no surviving staged chunk is released, so another operation
	// may now claim it.
	store.ReleaseDigestIfUnused(op, "d-solo")
	if err := store.ReserveDigest("op-2", "d-solo"); err != nil {
		t.Fatalf("reserve solo by op-2 after rollback: %v", err)
	}
	// A digest shared with a surviving staged chunk is kept, so a second
	// operation cannot claim it while the first is still active.
	store.ReleaseDigestIfUnused(op, "d-shared")
	if err := store.ReserveDigest("op-2", "d-shared"); err != model.ErrConflict {
		t.Fatalf("reserve shared by op-2: want ErrConflict, got %v", err)
	}
}

// TestReleaseDigestIfUnusedIgnoresOtherOwner verifies the method does not
// release a reservation owned by a different operation.
func TestReleaseDigestIfUnusedIgnoresOtherOwner(t *testing.T) {
	store := New(4)
	if err := store.ReserveDigest("op-1", "d"); err != nil {
		t.Fatal(err)
	}
	store.ReleaseDigestIfUnused("op-2", "d")
	if err := store.ReserveDigest("op-2", "d"); err != model.ErrConflict {
		t.Fatalf("reserve by op-2: want ErrConflict, got %v", err)
	}
}
