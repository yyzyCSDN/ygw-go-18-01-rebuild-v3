package model

import "time"

type SnapshotState string

const (
	SnapshotStaged    SnapshotState = "staged"
	SnapshotPublished SnapshotState = "published"
	SnapshotVerified  SnapshotState = "verified"
	SnapshotExpired   SnapshotState = "expired"
)

type Chunk struct {
	Index  int
	Digest string
	Data   []byte
}

type Snapshot struct {
	ID         string
	BaseID     string
	Operation  string
	Generation uint64
	Full       bool
	State      SnapshotState
	Metadata   map[string]string
	Chunks     []Chunk
	CreatedAt  time.Time
}

type CaptureHandle struct {
	Operation  string
	SnapshotID string
	Owner      string
	LeaseEpoch uint64
	Generation uint64
}

type Lease struct {
	Resource string
	Owner    string
	Epoch    uint64
	Deadline time.Time
	Released bool
}

type VerificationReceipt struct {
	SnapshotID string
	Generation uint64
	Digest     string
	VerifiedAt time.Time
}

type RetryTask struct {
	ID         string
	SnapshotID string
	Generation uint64
	Attempt    int
	DueAt      time.Time
}

type RestorePlan struct {
	ID         string
	SnapshotID string
	Chunks     []int
	Cursor     int
	CreatedAt  time.Time
}

type Clock interface {
	Now() time.Time
}

type SystemClock struct{}

func (SystemClock) Now() time.Time { return time.Now().UTC() }
