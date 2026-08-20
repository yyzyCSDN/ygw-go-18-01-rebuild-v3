package journal

import (
	"sync"
	"time"

	"example.com/backupmesh/internal/model"
)

type Entry struct {
	Sequence   uint64
	Generation uint64
	Kind       string
	SnapshotID string
	Operation  string
	Detail     string
	RecordedAt time.Time
}

type Log struct {
	mu      sync.RWMutex
	closed  bool
	next    uint64
	entries []Entry
}

func New() *Log { return &Log{next: 1} }

func (l *Log) Append(entry Entry) (Entry, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return Entry{}, model.ErrJournalClosed
	}
	entry.Sequence = l.next
	l.next++
	entry.RecordedAt = entry.RecordedAt.UTC()
	l.entries = append(l.entries, entry)
	return entry, nil
}

func (l *Log) AppendBatch(entries []Entry) ([]Entry, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil, model.ErrJournalClosed
	}
	committed := make([]Entry, len(entries))
	for index, entry := range entries {
		entry.Sequence = l.next
		l.next++
		entry.RecordedAt = entry.RecordedAt.UTC()
		committed[index] = entry
	}
	l.entries = append(l.entries, committed...)
	return append([]Entry(nil), committed...), nil
}

func (l *Log) Entries() []Entry {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return append([]Entry(nil), l.entries...)
}

func (l *Log) Close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.closed = true
}

func (l *Log) Open() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.closed = false
}

// LatestByOperation reduces entries to the latest one per operation. "Latest"
// is fenced by generation first: an entry from an older generation never
// supersedes one from a newer generation, regardless of sequence. Within the
// same generation the higher sequence wins. This prevents a stale generation
// that happens to carry a larger sequence from overwriting newer state.
func LatestByOperation(entries []Entry) []Entry {
	latest := make(map[string]Entry)
	order := make([]string, 0)
	for _, entry := range entries {
		current, exists := latest[entry.Operation]
		if !exists {
			order = append(order, entry.Operation)
		}
		latest[entry.Operation] = preferLatest(current, entry, exists)
	}
	result := make([]Entry, 0, len(order))
	for _, operation := range order {
		result = append(result, latest[operation])
	}
	return result
}

func preferLatest(current, candidate Entry, exists bool) Entry {
	if !exists {
		return candidate
	}
	switch {
	case candidate.Generation > current.Generation:
		return candidate
	case candidate.Generation < current.Generation:
		return current
	default:
		if candidate.Sequence >= current.Sequence {
			return candidate
		}
		return current
	}
}

func OperationSequence(entries []Entry) map[string]uint64 {
	result := make(map[string]uint64)
	for _, entry := range entries {
		if entry.Sequence > result[entry.Operation] {
			result[entry.Operation] = entry.Sequence
		}
	}
	return result
}

func EntriesAtOrBefore(entries []Entry, sequence uint64) []Entry {
	result := make([]Entry, 0, len(entries))
	for _, entry := range entries {
		if entry.Sequence <= sequence {
			result = append(result, entry)
		}
	}
	return result
}
