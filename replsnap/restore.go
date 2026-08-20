package replsnap

import "fmt"

func ResolveChain(store *MemoryStore, snapshotID string) ([]Snapshot, error) {
	visited := make(map[string]struct{})
	chain := make([]Snapshot, 0)
	current := snapshotID
	for current != "" {
		if _, duplicate := visited[current]; duplicate {
			return nil, fmt.Errorf("%w: dependency cycle", ErrInvalidManifest)
		}
		visited[current] = struct{}{}
		snapshot, ok := store.Get(current)
		if !ok {
			return nil, fmt.Errorf("%w: %s", ErrSnapshotNotFound, current)
		}
		chain = append(chain, snapshot)
		current = snapshot.Meta.BaseID
	}
	for left, right := 0, len(chain)-1; left < right; left, right = left+1, right-1 {
		chain[left], chain[right] = chain[right], chain[left]
	}
	return chain, nil
}

func RestoreBytes(chain []Snapshot) []byte {
	result := make([]byte, 0)
	for _, snapshot := range chain {
		for _, chunk := range snapshot.Chunks {
			result = append(result, chunk...)
		}
	}
	return result
}
