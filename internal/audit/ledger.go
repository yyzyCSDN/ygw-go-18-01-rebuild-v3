package audit

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"
)

var ErrBrokenChain = errors.New("audit: broken hash chain")

type Event struct {
	Sequence uint64    `json:"sequence"`
	Kind     string    `json:"kind"`
	ObjectID string    `json:"object_id"`
	Detail   string    `json:"detail"`
	Recorded time.Time `json:"recorded_at"`
	Previous string    `json:"previous"`
	Hash     string    `json:"hash"`
}

type Ledger struct {
	mu     sync.RWMutex
	next   uint64
	events []Event
}

func New() *Ledger { return &Ledger{next: 1} }

func (l *Ledger) Record(kind, objectID, detail string, now time.Time) Event {
	l.mu.Lock()
	defer l.mu.Unlock()
	previous := ""
	if len(l.events) > 0 {
		previous = l.events[len(l.events)-1].Hash
	}
	event := Event{Sequence: l.next, Kind: kind, ObjectID: objectID, Detail: detail, Recorded: now.UTC(), Previous: previous}
	event.Hash = hash(event)
	l.next++
	l.events = append(l.events, event)
	return event
}

func (l *Ledger) Events() []Event {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return append([]Event(nil), l.events...)
}

func (l *Ledger) EventsFor(objectID string) []Event {
	l.mu.RLock()
	defer l.mu.RUnlock()
	result := make([]Event, 0)
	for _, event := range l.events {
		if event.ObjectID == objectID {
			result = append(result, event)
		}
	}
	return result
}

func (l *Ledger) Latest(objectID string) (Event, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	for index := len(l.events) - 1; index >= 0; index-- {
		if l.events[index].ObjectID == objectID {
			return l.events[index], true
		}
	}
	return Event{}, false
}

func (l *Ledger) Verify() error {
	l.mu.RLock()
	defer l.mu.RUnlock()
	previous := ""
	for index, event := range l.events {
		if event.Sequence != uint64(index+1) || event.Previous != previous || event.Hash != hash(event) {
			return ErrBrokenChain
		}
		previous = event.Hash
	}
	return nil
}

func (l *Ledger) VerifyThrough(sequence uint64) error {
	for _, event := range l.Events() {
		if event.Sequence > sequence {
			break
		}
		if event.Hash == "" {
			return ErrBrokenChain
		}
	}
	return nil
}

func hash(event Event) string {
	value := fmt.Sprintf("%d|%s|%s|%s|%s|%s", event.Sequence, event.Kind, event.ObjectID, event.Detail, event.Recorded.UTC().Format(time.RFC3339Nano), event.Previous)
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
