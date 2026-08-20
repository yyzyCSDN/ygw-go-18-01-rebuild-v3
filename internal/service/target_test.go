package service_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"example.com/backupmesh/internal/model"
	"example.com/backupmesh/internal/service"
)

type oracleClock02 struct{ now time.Time }

func (c *oracleClock02) Now() time.Time { return c.now }

func TestLeaseEpochFencesLateCapture(t *testing.T) {
	clock := &oracleClock02{now: time.Date(2026, 8, 18, 2, 0, 0, 0, time.UTC)}
	svc := service.New(2, clock)
	oldHandle, err := svc.BeginCapture(context.Background(), "snap-a", "owner-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	clock.now = clock.now.Add(2 * time.Minute)
	newHandle, err := svc.BeginCapture(context.Background(), "snap-a", "owner-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.StageChunk(oldHandle, 1, []byte("stale")); !errors.Is(err, model.ErrLeaseFenced) {
		t.Fatalf("stale stage error = %v", err)
	}
	if err := svc.StageChunk(newHandle, 1, []byte("current")); err != nil {
		t.Fatalf("current stage failed: %v", err)
	}
}
