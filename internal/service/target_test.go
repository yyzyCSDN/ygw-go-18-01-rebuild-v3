package service_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"example.com/backupmesh/internal/model"
	"example.com/backupmesh/internal/service"
)

type oracleClock05 struct{ now time.Time }

func (c oracleClock05) Now() time.Time { return c.now }

func TestCancelledRetryDoesNotResurrect(t *testing.T) {
	now := time.Date(2026, 8, 18, 5, 0, 0, 0, time.UTC)
	svc := service.New(2, oracleClock05{now})
	if _, err := svc.ScheduleRetry("snap-a", 3, now); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- svc.RunRetry(context.Background(), func(context.Context, model.RetryTask) error {
			close(entered)
			<-release
			return errors.New("sink unavailable")
		}, time.Minute)
	}()
	<-entered
	svc.CancelRetries("snap-a", 3)
	close(release)
	if err := <-done; !errors.Is(err, model.ErrCancelled) {
		t.Fatalf("worker error = %v", err)
	}
	if pending := svc.RetryPending("snap-a"); pending != 0 {
		t.Fatalf("pending = %d", pending)
	}
}
