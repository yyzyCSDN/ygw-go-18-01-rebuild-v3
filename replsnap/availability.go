package replsnap

func CountAvailability(inventories []ReplicaInventory) map[string]int {
	counts := make(map[string]int)
	for _, inventory := range inventories {
		seen := make(map[string]struct{})
		for _, snapshotID := range inventory.SnapshotIDs {
			if snapshotID == "" {
				continue
			}
			if _, duplicate := seen[snapshotID]; duplicate {
				continue
			}
			seen[snapshotID] = struct{}{}
			counts[snapshotID]++
		}
	}
	return counts
}

func AvailableSnapshots(inventories []ReplicaInventory, policy RecoveryPolicy) map[string]int {
	result := make(map[string]int)
	for snapshotID, copies := range CountAvailability(inventories) {
		if policy.Eligible(copies) {
			result[snapshotID] = copies
		}
	}
	return result
}

func HasReplicaMajority(copies int, replicaCount int) bool {
	return copies >= strictMajority(replicaCount)
}
