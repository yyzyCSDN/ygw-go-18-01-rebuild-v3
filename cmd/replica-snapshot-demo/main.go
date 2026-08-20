package main

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"example.com/backupmesh/internal/model"
	servicepkg "example.com/backupmesh/internal/service"
	"example.com/backupmesh/replsnap"
)

func main() {
	store := replsnap.NewMemoryStore()
	service := replsnap.NewService(store)
	snapshot, err := service.Capture(context.Background(), replsnap.SnapshotMeta{ID: "demo"}, bytes.NewReader([]byte("replica snapshot")), 8)
	if err != nil {
		fmt.Printf("capture err=%v\n", err)
		return
	}
	receipt, err := service.Promote("demo", 3, []string{"replica-a", "replica-b"})
	fmt.Printf("snapshot=%s chunks=%d root=%s promoted=%v err=%v\n", snapshot.Meta.ID, len(snapshot.Chunks), snapshot.Manifest.MerkleRoot[:12], receipt.Promoted, err)

	orchestrator := servicepkg.New(64, model.SystemClock{})
	handle, err := orchestrator.BeginCapture(context.Background(), "demo-orchestrated", "demo", time.Minute)
	if err != nil {
		fmt.Printf("begin orchestrated capture err=%v\n", err)
		return
	}
	if err := orchestrator.StageChunk(handle, 0, []byte("replica snapshot")); err != nil {
		fmt.Printf("stage orchestrated capture err=%v\n", err)
		return
	}
	if _, err := orchestrator.CommitCapture(handle, "", true, map[string]string{"source": "demo"}); err != nil {
		fmt.Printf("commit orchestrated capture err=%v\n", err)
		return
	}
	envelope, err := orchestrator.MaterializeManifest("demo-orchestrated")
	if err != nil {
		fmt.Printf("materialize manifest err=%v\n", err)
		return
	}
	fmt.Printf("manifest=%s version=%d audit=%d\n", envelope.Digest[:12], envelope.Version, len(orchestrator.AuditEvents()))
}
