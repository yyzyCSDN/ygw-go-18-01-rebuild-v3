package retry

import (
	"sort"
	"sync"
	"time"

	"example.com/backupmesh/internal/model"
)

type Queue struct {
	mu        sync.Mutex
	tasks     map[string]model.RetryTask
	cancelled map[string]uint64
	history   map[string][]model.RetryTask
}

func New() *Queue {
	return &Queue{tasks: make(map[string]model.RetryTask), cancelled: make(map[string]uint64), history: make(map[string][]model.RetryTask)}
}

func (q *Queue) Schedule(task model.RetryTask) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if generation := q.cancelled[task.SnapshotID]; generation >= task.Generation {
		return model.ErrCancelled
	}
	q.tasks[task.ID] = task
	return nil
}

func (q *Queue) Cancel(snapshotID string, generation uint64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if generation > q.cancelled[snapshotID] {
		q.cancelled[snapshotID] = generation
	}
	for id, task := range q.tasks {
		if task.SnapshotID == snapshotID && task.Generation <= generation {
			delete(q.tasks, id)
		}
	}
}

func (q *Queue) Due(now time.Time) []model.RetryTask {
	q.mu.Lock()
	defer q.mu.Unlock()
	result := make([]model.RetryTask, 0)
	for _, task := range q.tasks {
		if !task.DueAt.After(now) && q.cancelled[task.SnapshotID] < task.Generation {
			result = append(result, task)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].DueAt.Equal(result[j].DueAt) {
			return result[i].ID < result[j].ID
		}
		return result[i].DueAt.Before(result[j].DueAt)
	})
	return result
}

func (q *Queue) Complete(task model.RetryTask) {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.tasks, task.ID)
}

func (q *Queue) RescheduleAfterFailure(task model.RetryTask, due time.Time) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	// A generation cancelled while the worker was still inside the sender
	// must not be revived by the late failure result. This mirrors the
	// cancellation guard in Schedule and keeps the cancelled generation
	// isolated from out-of-order failure outcomes.
	if generation := q.cancelled[task.SnapshotID]; generation >= task.Generation {
		return model.ErrCancelled
	}
	task.Attempt++
	task.DueAt = due
	q.history[task.SnapshotID] = append(q.history[task.SnapshotID], task)
	q.tasks[task.ID] = task
	return nil
}

func (q *Queue) History(snapshotID string) []model.RetryTask {
	q.mu.Lock()
	defer q.mu.Unlock()
	values := q.history[snapshotID]
	result := make([]model.RetryTask, len(values))
	copy(result, values)
	return result
}

func (q *Queue) Pending(snapshotID string) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	count := 0
	for _, task := range q.tasks {
		if task.SnapshotID == snapshotID {
			count++
		}
	}
	return count
}
