package lease

import (
	"errors"
	"testing"
	"time"

	"example.com/backupmesh/internal/model"
)

func TestAcquireAdvancesEpoch(t *testing.T) {
	store := New()
	now := time.Date(2026, 8, 18, 10, 0, 0, 0, time.UTC)

	// First acquisition seeds the epoch at 1.
	first, err := store.Acquire("capture:snap", "node-a", now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if first.Epoch != 1 {
		t.Fatalf("first epoch = %d, want 1", first.Epoch)
	}

	// Re-acquiring after expiry advances the epoch even for the same owner, so
	// handles stamped with the prior epoch are fenced.
	now = now.Add(2 * time.Minute)
	second, err := store.Acquire("capture:snap", "node-a", now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if second.Epoch != 2 {
		t.Fatalf("second epoch = %d, want 2", second.Epoch)
	}

	// The superseded epoch must no longer validate, while the current one does.
	if err := store.Validate("capture:snap", "node-a", first.Epoch, now); !errors.Is(err, model.ErrLeaseFenced) {
		t.Fatalf("stale epoch validate err = %v, want ErrLeaseFenced", err)
	}
	if err := store.Validate("capture:snap", "node-a", second.Epoch, now); err != nil {
		t.Fatalf("current epoch validate err = %v", err)
	}
}

func TestAcquireHeldWhileLive(t *testing.T) {
	store := New()
	now := time.Date(2026, 8, 18, 10, 0, 0, 0, time.UTC)
	if _, err := store.Acquire("capture:snap", "node-a", now, time.Minute); err != nil {
		t.Fatal(err)
	}
	// A live lease blocks re-acquisition even by the same owner.
	if _, err := store.Acquire("capture:snap", "node-a", now, time.Minute); !errors.Is(err, model.ErrLeaseHeld) {
		t.Fatalf("live re-acquire err = %v, want ErrLeaseHeld", err)
	}
}

func TestValidateOwnerFencing(t *testing.T) {
	store := New()
	now := time.Date(2026, 8, 18, 10, 0, 0, 0, time.UTC)
	lease, err := store.Acquire("capture:snap", "node-a", now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	// A different owner presenting the correct epoch is still fenced.
	if err := store.Validate("capture:snap", "node-b", lease.Epoch, now); !errors.Is(err, model.ErrLeaseFenced) {
		t.Fatalf("wrong owner validate err = %v, want ErrLeaseFenced", err)
	}
}
