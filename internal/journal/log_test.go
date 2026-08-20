package journal

import "testing"

func TestLatestByOperationFencesStaleGeneration(t *testing.T) {
	entries := []Entry{
		{Sequence: 3, Generation: 2, Kind: "publish", Operation: "op-1"},
		{Sequence: 4, Generation: 1, Kind: "verify", Operation: "op-1"},
	}
	got := LatestByOperation(entries)
	if len(got) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(got))
	}
	if got[0].Generation != 2 {
		t.Fatalf("generation = %d, want 2 (newer generation must fence out stale verify with larger sequence)", got[0].Generation)
	}
	if got[0].Kind != "publish" {
		t.Fatalf("kind = %s, want publish", got[0].Kind)
	}
}

func TestLatestByOperationPreservesSameGenerationLatestSequence(t *testing.T) {
	entries := []Entry{
		{Sequence: 1, Generation: 1, Kind: "publish", Operation: "op-1"},
		{Sequence: 2, Generation: 1, Kind: "verify", Operation: "op-1"},
	}
	got := LatestByOperation(entries)
	if len(got) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(got))
	}
	if got[0].Generation != 1 {
		t.Fatalf("generation = %d, want 1", got[0].Generation)
	}
	if got[0].Sequence != 2 {
		t.Fatalf("sequence = %d, want 2 (within a generation the larger sequence wins)", got[0].Sequence)
	}
	if got[0].Kind != "verify" {
		t.Fatalf("kind = %s, want verify", got[0].Kind)
	}
}

func TestLatestByOperationKeepsOnePerOperation(t *testing.T) {
	entries := []Entry{
		{Sequence: 1, Generation: 1, Kind: "publish", Operation: "op-1"},
		{Sequence: 2, Generation: 1, Kind: "verify", Operation: "op-2"},
	}
	got := LatestByOperation(entries)
	if len(got) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(got))
	}
}

func TestLatestByOperationNewerGenerationBeatsLargerSequenceOutOfOrder(t *testing.T) {
	// The verify arrives later (larger sequence) but belongs to an older
	// generation; it must not replace the newer-generation publish.
	entries := []Entry{
		{Sequence: 4, Generation: 1, Kind: "verify", Operation: "op-1"},
		{Sequence: 3, Generation: 2, Kind: "publish", Operation: "op-1"},
	}
	got := LatestByOperation(entries)
	if len(got) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(got))
	}
	if got[0].Generation != 2 {
		t.Fatalf("generation = %d, want 2", got[0].Generation)
	}
	if got[0].Kind != "publish" {
		t.Fatalf("kind = %s, want publish", got[0].Kind)
	}
}
