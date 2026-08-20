package service_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"example.com/backupmesh/internal/model"
	"example.com/backupmesh/internal/service"
)

type oracleClock09 struct{ now time.Time }

func (c oracleClock09) Now() time.Time { return c.now }

func TestDuplicateChunkIndexPreservesOriginalPayload(t *testing.T) {
	svc := service.New(2, oracleClock09{time.Date(2026, 8, 18, 9, 0, 0, 0, time.UTC)})
	first, _ := svc.BeginCapture(context.Background(), "snap-a", "owner-a", time.Minute)
	if err := svc.StageChunk(first, 1, []byte("original")); err != nil {
		t.Fatal(err)
	}
	if err := svc.StageChunk(first, 1, []byte("replacement")); !errors.Is(err, model.ErrConflict) {
		t.Fatalf("duplicate index error = %v", err)
	}
	snapshot, err := svc.CommitCapture(first, "", true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Chunks) != 1 || string(snapshot.Chunks[0].Data) != "original" {
		t.Fatalf("committed chunks = %+v", snapshot.Chunks)
	}
}
