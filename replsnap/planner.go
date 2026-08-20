package replsnap

import (
	"sort"
)

func ChooseLatestAvailable(metadata []SnapshotMeta, copies map[string]int, policy RecoveryPolicy) (SnapshotMeta, int, error) {
	candidates := make([]SnapshotMeta, 0, len(metadata))
	for _, meta := range metadata {
		if policy.Eligible(copies[meta.ID]) {
			candidates = append(candidates, meta)
		}
	}
	if len(candidates) == 0 {
		return SnapshotMeta{}, 0, ErrNoRecoveryPoint
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].CreatedAt.Equal(candidates[j].CreatedAt) {
			return candidates[i].ID > candidates[j].ID
		}
		return candidates[i].CreatedAt.After(candidates[j].CreatedAt)
	})
	selected := candidates[0]
	return selected, copies[selected.ID], nil
}
