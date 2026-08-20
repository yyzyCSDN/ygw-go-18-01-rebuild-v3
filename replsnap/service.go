package replsnap

import (
	"context"
	"fmt"
	"io"
)

type Service struct {
	store *MemoryStore
}

func NewService(store *MemoryStore) *Service {
	return &Service{store: store}
}

func (service *Service) Capture(ctx context.Context, meta SnapshotMeta, reader io.Reader, blockSize int) (Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	if meta.ID == "" {
		return Snapshot{}, fmt.Errorf("%w: missing snapshot id", ErrInvalidManifest)
	}
	frames, err := ReadFrames(reader, blockSize)
	if err != nil {
		return Snapshot{}, err
	}
	manifest, chunks, err := BuildManifest(meta.ID, frames)
	if err != nil {
		return Snapshot{}, err
	}
	if err := VerifyManifest(manifest, chunks); err != nil {
		return Snapshot{}, err
	}
	snapshot := Snapshot{Meta: meta, Manifest: manifest, Chunks: chunks}
	service.store.Save(snapshot)
	return cloneSnapshot(snapshot), nil
}

func (service *Service) Promote(snapshotID string, replicaCount int, acknowledgements []string) (PromotionReceipt, error) {
	if _, ok := service.store.Get(snapshotID); !ok {
		return PromotionReceipt{}, ErrSnapshotNotFound
	}
	policy := ReplicationPolicy{ReplicaCount: replicaCount}
	receipt := PromotionReceipt{SnapshotID: snapshotID, AckCount: uniqueCount(acknowledgements), Required: policy.RequiredAcks()}
	if !HasQuorum(acknowledgements, replicaCount) {
		return receipt, ErrQuorumNotReached
	}
	receipt.Promoted = true
	return receipt, nil
}

func (service *Service) PlanRestore(inventories []ReplicaInventory, policy RecoveryPolicy) (RestorePlan, error) {
	availability := CountAvailability(inventories)
	selected, copies, err := ChooseLatestAvailable(service.store.List(), availability, policy)
	if err != nil {
		return RestorePlan{}, err
	}
	chain, err := ResolveChain(service.store, selected.ID)
	if err != nil {
		return RestorePlan{}, err
	}
	ids := make([]string, len(chain))
	for index, snapshot := range chain {
		ids[index] = snapshot.Meta.ID
	}
	return RestorePlan{SnapshotID: selected.ID, Copies: copies, Chain: ids}, nil
}

func (service *Service) Prune(keepNewest int) []string {
	return service.store.ApplyRetention(keepNewest)
}

func uniqueCount(values []string) int {
	seen := make(map[string]struct{})
	for _, value := range values {
		if value != "" {
			seen[value] = struct{}{}
		}
	}
	return len(seen)
}
