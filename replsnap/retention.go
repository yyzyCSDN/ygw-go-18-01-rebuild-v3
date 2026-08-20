package replsnap

import "sort"

func RetainedIDs(metadata []SnapshotMeta, keepNewest int) map[string]struct{} {
	if keepNewest < 0 {
		keepNewest = 0
	}
	ordered := append([]SnapshotMeta(nil), metadata...)
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].CreatedAt.Equal(ordered[j].CreatedAt) {
			return ordered[i].ID > ordered[j].ID
		}
		return ordered[i].CreatedAt.After(ordered[j].CreatedAt)
	})
	retained := make(map[string]struct{})
	byID := make(map[string]SnapshotMeta, len(metadata))
	for _, meta := range metadata {
		byID[meta.ID] = meta
	}
	for index := 0; index < len(ordered) && index < keepNewest; index++ {
		retained[ordered[index].ID] = struct{}{}
	}
	for changed := true; changed; {
		changed = false
		for id := range retained {
			for _, dependency := range byID[id].Dependencies() {
				if _, found := retained[dependency]; !found {
					retained[dependency] = struct{}{}
					changed = true
				}
			}
		}
	}
	return retained
}

func (store *MemoryStore) ApplyRetention(keepNewest int) []string {
	metadata := store.List()
	retained := RetainedIDs(metadata, keepNewest)
	removed := make([]string, 0)
	for _, meta := range metadata {
		if _, keep := retained[meta.ID]; !keep {
			store.Delete(meta.ID)
			removed = append(removed, meta.ID)
		}
	}
	sort.Strings(removed)
	return removed
}
