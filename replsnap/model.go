package replsnap

import "time"

type ChunkMeta struct {
	Index  int
	Size   int
	Digest string
	Final  bool
}

type Manifest struct {
	SnapshotID string
	Chunks     []ChunkMeta
	MerkleRoot string
}

type SnapshotMeta struct {
	ID        string
	BaseID    string
	CreatedAt time.Time
}

func (meta SnapshotMeta) Dependencies() []string {
	if meta.BaseID == "" {
		return nil
	}
	return []string{meta.BaseID}
}

type Snapshot struct {
	Meta     SnapshotMeta
	Manifest Manifest
	Chunks   [][]byte
}

type ReplicationPolicy struct {
	ReplicaCount int
}

func (policy ReplicationPolicy) RequiredAcks() int {
	return strictMajority(policy.ReplicaCount)
}

type PromotionReceipt struct {
	SnapshotID string
	AckCount   int
	Required   int
	Promoted   bool
}

type ReplicaInventory struct {
	ReplicaID   string
	SnapshotIDs []string
}

type RecoveryPolicy struct {
	MinimumCopies int
}

func (policy RecoveryPolicy) Eligible(copies int) bool {
	minimum := policy.MinimumCopies
	if minimum < 1 {
		minimum = 1
	}
	return copies >= minimum
}

type RestorePlan struct {
	SnapshotID string
	Copies     int
	Chain      []string
}
