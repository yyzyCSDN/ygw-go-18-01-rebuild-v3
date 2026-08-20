package service

import (
	"context"
	"time"

	"example.com/backupmesh/internal/model"
)

type Sender func(context.Context, model.RetryTask) error

func (s *Service) ScheduleRetry(snapshotID string, generation uint64, due time.Time) (model.RetryTask, error) {
	task := model.RetryTask{ID: model.NewID("retry"), SnapshotID: snapshotID, Generation: generation, DueAt: due.UTC()}
	if err := s.retries.Schedule(task); err != nil {
		return model.RetryTask{}, err
	}
	return task, nil
}

func (s *Service) CancelRetries(snapshotID string, generation uint64) {
	s.retries.Cancel(snapshotID, generation)
}

func (s *Service) RunRetry(ctx context.Context, sender Sender, delay time.Duration) error {
	due := s.retries.Due(s.clock.Now())
	if len(due) == 0 {
		return model.ErrNotFound
	}
	task := due[0]
	if err := sender(ctx, task); err != nil {
		return s.retries.RescheduleAfterFailure(task, s.clock.Now().Add(delay))
	}
	s.retries.Complete(task)
	return nil
}
