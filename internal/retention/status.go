package retention

import (
	"container/list"
	"sync"
	"time"
)

// SweepStatus is the most recent worker attempt, not an estimate of remaining events.
type SweepStatus struct {
	CompletedAt time.Time `json:"completed_at"`
	Deleted     int       `json:"deleted"`
	BatchFull   bool      `json:"batch_full"`
	Failed      bool      `json:"failed"`
}

type statusEntry struct {
	deployment string
	status     SweepStatus
}

// Tracker bounds in-memory diagnostic state and never persists tenant activity.
type Tracker struct {
	mu      sync.Mutex
	entries map[string]*list.Element
	order   list.List
	limit   int
}

func NewTracker(limit int) *Tracker {
	if limit < 1 {
		panic("retention status limit must be positive")
	}
	return &Tracker{entries: make(map[string]*list.Element), limit: limit}
}

func (t *Tracker) Record(deployment string, status SweepStatus) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if entry := t.entries[deployment]; entry != nil {
		entry.Value = statusEntry{deployment, status}
		t.order.MoveToBack(entry)
		return
	}
	if t.order.Len() == t.limit {
		oldest := t.order.Front()
		delete(t.entries, oldest.Value.(statusEntry).deployment)
		t.order.Remove(oldest)
	}
	t.entries[deployment] = t.order.PushBack(statusEntry{deployment, status})
}

func (t *Tracker) Get(deployment string) *SweepStatus {
	t.mu.Lock()
	defer t.mu.Unlock()
	entry := t.entries[deployment]
	if entry == nil {
		return nil
	}
	status := entry.Value.(statusEntry).status
	return &status
}
