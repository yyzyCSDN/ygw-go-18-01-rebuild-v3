package service_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"example.com/backupmesh/internal/model"
	"example.com/backupmesh/internal/service"
)

// TestRunRetryCancelDuringSenderDoesNotRevive reproduces the lifecycle hazard
// where a retry worker is blocked inside the sender while its snapshot
// generation is cancelled. Once the barrier releases and the sender reports a
// late failure (sink unavailable), the worker must neither return success nor
// reschedule the cancelled task back into the queue. The cancelled generation
// must stay isolated from the out-of-order failure outcome.
func TestRunRetryCancelDuringSenderDoesNotRevive(t *testing.T) {
	now := time.Date(2026, 8, 18, 10, 0, 0, 0, time.UTC)
	svc := service.New(4, fixedClock{now: now})

	if _, err := svc.ScheduleRetry("snap-a", 3, now); err != nil {
		t.Fatalf("schedule: %v", err)
	}
	if pending := svc.RetryPending("snap-a"); pending != 1 {
		t.Fatalf("pending before run = %d, want 1", pending)
	}

	// Sync barrier: the sender signals once it has been entered so the test
	// knows Due already handed the task to the worker, then blocks until the
	// test releases it. This models a worker parked inside the sender while a
	// cancellation lands.
	entered := make(chan struct{})
	release := make(chan struct{})
	sender := func(ctx context.Context, task model.RetryTask) error {
		close(entered)
		<-release
		return errors.New("sink unavailable")
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- svc.RunRetry(context.Background(), sender, time.Second)
	}()

	// Wait until the worker is parked inside the sender, then cancel the
	// generation while the failure result is still in flight.
	<-entered
	svc.CancelRetries("snap-a", 3)

	// Release the barrier so the sender returns its late failure.
	close(release)

	select {
	case err := <-errCh:
		// The cancelled generation must surface as a cancellation, not nil
		// ("success"). A nil return here is the revival bug.
		if err == nil {
			t.Fatal("RunRetry returned nil for a cancelled task; expected ErrCancelled")
		}
		if !errors.Is(err, model.ErrCancelled) {
			t.Fatalf("RunRetry error = %v, want ErrCancelled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("RunRetry did not return after barrier release")
	}

	// The cancelled task must not have been revived in the queue.
	if pending := svc.RetryPending("snap-a"); pending != 0 {
		t.Fatalf("pending after cancel = %d, want 0 (cancelled task revived)", pending)
	}
}

// TestRunRetryFailureReschedules confirms the normal failure path still
// reschedules a task when the generation has not been cancelled, so the
// lifecycle isolation does not suppress legitimate retries.
func TestRunRetryFailureReschedules(t *testing.T) {
	now := time.Date(2026, 8, 18, 10, 0, 0, 0, time.UTC)
	svc := service.New(4, fixedClock{now: now})

	if _, err := svc.ScheduleRetry("snap-b", 1, now); err != nil {
		t.Fatalf("schedule: %v", err)
	}

	sender := func(ctx context.Context, task model.RetryTask) error {
		return errors.New("sink unavailable")
	}
	if err := svc.RunRetry(context.Background(), sender, time.Second); err != nil {
		t.Fatalf("RunRetry = %v, want nil (rescheduled)", err)
	}
	if pending := svc.RetryPending("snap-b"); pending != 1 {
		t.Fatalf("pending after failure = %d, want 1 (rescheduled)", pending)
	}
}
