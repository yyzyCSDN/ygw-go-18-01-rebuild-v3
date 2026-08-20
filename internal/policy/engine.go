package policy

import (
	"errors"
	"time"

	"example.com/backupmesh/internal/model"
)

var ErrPolicy = errors.New("policy: operation outside policy")

type Retention struct {
	KeepRecent      int
	ProtectRestores bool
	MaximumAge      time.Duration
}

type Retry struct {
	MaximumAttempts int
	BaseDelay       time.Duration
	MaximumDelay    time.Duration
}

type Engine struct {
	retention Retention
	retry     Retry
}

func New() *Engine {
	return &Engine{
		retention: Retention{KeepRecent: 2, ProtectRestores: true, MaximumAge: 30 * 24 * time.Hour},
		retry:     Retry{MaximumAttempts: 5, BaseDelay: time.Second, MaximumDelay: 10 * time.Minute},
	}
}

func (e *Engine) RetentionPolicy() Retention { return e.retention }
func (e *Engine) RetryPolicy() Retry         { return e.retry }

func (e *Engine) ValidateRetention(keep int) error {
	if keep < 0 || keep > 10000 {
		return ErrPolicy
	}
	return nil
}

func (e *Engine) ValidateRetry(task model.RetryTask) error {
	if task.Attempt < 0 || task.Attempt > e.retry.MaximumAttempts {
		return ErrPolicy
	}
	if task.DueAt.IsZero() {
		return ErrPolicy
	}
	return nil
}

func (e *Engine) NextRetry(task model.RetryTask, now time.Time) (time.Time, error) {
	if err := e.ValidateRetry(task); err != nil {
		return time.Time{}, err
	}
	delay := e.retry.BaseDelay
	for attempt := 0; attempt < task.Attempt; attempt++ {
		delay *= 2
		if delay >= e.retry.MaximumDelay {
			delay = e.retry.MaximumDelay
			break
		}
	}
	return now.UTC().Add(delay), nil
}

func (e *Engine) ShouldRetain(snapshot model.Snapshot, now time.Time, activeRestore bool) bool {
	if e.retention.ProtectRestores && activeRestore {
		return true
	}
	if e.retention.MaximumAge <= 0 {
		return true
	}
	return now.Sub(snapshot.CreatedAt) <= e.retention.MaximumAge
}
