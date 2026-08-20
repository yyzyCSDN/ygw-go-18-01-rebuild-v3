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

func LatestByOperation(entries []Entry) []Entry {
	latest := make(map[string]Entry)
	order := make([]string, 0)
	for _, entry := range entries {
		current, exists := latest[entry.Operation]
		if !exists {
			order = append(order, entry.Operation)
		}
		if !exists || entry.Generation > current.Generation || entry.Generation == current.Generation && entry.Sequence > current.Sequence {
			latest[entry.Operation] = entry
		}
	}
	result := make([]Entry, 0, len(order))
	for _, operation := range order {
		result = append(result, latest[operation])
	}
	return result
}
