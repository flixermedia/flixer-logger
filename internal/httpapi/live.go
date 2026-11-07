package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/flixermedia/flixer-logger/internal/auth"
	"github.com/flixermedia/flixer-logger/internal/store"
)

const (
	maxSubscribers              = 100
	maxSubscribersPerDeployment = 10
	liveQueueSize               = 1024
	liveQueueBytes              = 4 * 1024 * 1024
)

var errDeploymentSubscriberLimit = errors.New("deployment live subscriber limit reached")

type subscription struct {
	deployment  string
	filters     liveFilters
	events      chan liveEvent
	overflow    chan struct{}
	queuedBytes int
}

type liveFilters map[string]string

func parseLiveFilters(values url.Values) (liveFilters, error) {
	filters := make(liveFilters, len(values))
	for key, entries := range values {
		switch key {
		case "app", "instance", "level", "source", "event", "trace", "request", "task", "task_queue":
		default:
			return nil, errors.New("unsupported live filter")
		}
		if len(entries) != 1 || entries[0] == "" || len(entries[0]) > 1024 || !utf8.ValidString(entries[0]) {
			return nil, errors.New("invalid live filter")
		}
		filters[key] = entries[0]
	}
	if filters["task_queue"] != "" && filters["task"] == "" {
		return nil, errors.New("task_queue requires task")
	}
	return filters, nil
}

func (f liveFilters) matches(event store.Event) bool {
	for key, value := range f {
		var actual string
		switch key {
		case "app":
			actual = event.App
		case "instance":
			actual = event.InstanceID
		case "level":
			actual = event.Level
		case "source":
			actual = event.Source
		case "event":
			actual = event.Code
		case "trace":
			actual = event.TraceID
		case "request":
			actual = event.RequestID
		case "task":
			actual = event.TaskID
		case "task_queue":
			actual, _ = event.Fields["task_queue"].(string)
		}
		if actual != value {
			return false
		}
	}
	return true
}

type liveEvent struct {
	cursor string
	data   []byte
}

type liveHub struct {
	mu          sync.Mutex
	subscribers map[*subscription]struct{}
}

func newLiveHub() *liveHub {
	return &liveHub{subscribers: make(map[*subscription]struct{})}
}

func (h *liveHub) subscribe(deployment string, filters liveFilters) (*subscription, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	count := 0
	for subscriber := range h.subscribers {
		if subscriber.deployment == deployment {
			count++
		}
	}
	if count >= maxSubscribersPerDeployment {
		return nil, errDeploymentSubscriberLimit
	}
	if len(h.subscribers) >= maxSubscribers {
		return nil, errors.New("live subscription limit reached")
	}
	s := &subscription{deployment: deployment, filters: filters, events: make(chan liveEvent, liveQueueSize), overflow: make(chan struct{})}
	h.subscribers[s] = struct{}{}
	return s, nil
}

func (h *liveHub) unsubscribe(s *subscription) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.subscribers, s)
}

func (h *liveHub) count(deployment string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	count := 0
	for subscriber := range h.subscribers {
		if subscriber.deployment == deployment {
			count++
		}
	}
	return count
}

func (h *liveHub) consumed(s *subscription, event liveEvent) {
	h.mu.Lock()
	defer h.mu.Unlock()
	s.queuedBytes -= len(event.cursor) + len(event.data)
}

// Publish is called only after the durable commit, and never waits on clients.
func (h *liveHub) publish(events []store.Event) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, event := range events {
		var item liveEvent
		var size int
		for subscriber := range h.subscribers {
			if subscriber.deployment != event.DeploymentID || !subscriber.filters.matches(event) {
				continue
			}
			if item.data == nil {
				var err error
				item.data, err = json.Marshal(event)
				if err != nil {
					for s := range h.subscribers {
						if s.deployment == event.DeploymentID {
							close(s.overflow)
							delete(h.subscribers, s)
						}
					}
					return fmt.Errorf("serialize live event: %w", err)
				}
				item.cursor = store.LiveCursor(event)
				size = len(item.cursor) + len(item.data)
			}
			if size > liveQueueBytes-subscriber.queuedBytes {
				close(subscriber.overflow)
				delete(h.subscribers, subscriber)
				continue
			}
			select {
			case subscriber.events <- item:
				subscriber.queuedBytes += size
			default:
				close(subscriber.overflow)
				delete(h.subscribers, subscriber)
			}
		}
	}
	return nil
}

func (h *Handler) stream(w http.ResponseWriter, r *http.Request) {
	identity, err := h.authorize(r, auth.Query)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid live filters")
		return
	}
	filters, err := parseLiveFilters(values)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid live filters")
		return
	}
	if _, ok := w.(http.Flusher); !ok {
		writeError(w, http.StatusInternalServerError, "streaming unavailable")
		return
	}
	h.liveOrdering.Lock()
	sub, err := h.live.subscribe(identity.DeploymentID, filters)
	if err != nil {
		h.liveOrdering.Unlock()
		if errors.Is(err, errDeploymentSubscriberLimit) {
			w.Header().Set("Retry-After", "1")
			writeError(w, http.StatusTooManyRequests, "deployment live subscriber limit reached")
			return
		}
		writeError(w, http.StatusServiceUnavailable, "live subscriber limit reached")
		return
	}
	var snapshot *store.ReplaySnapshot
	if cursor := r.Header.Get("Last-Event-ID"); cursor != "" {
		snapshot, err = h.store.NewReplaySnapshot(identity.DeploymentID, cursor)
	}
	h.liveOrdering.Unlock()
	defer h.live.unsubscribe(sub)
	if errors.Is(err, store.ErrInvalidInput) {
		writeError(w, http.StatusBadRequest, "invalid live cursor")
		return
	}
	if errors.Is(err, store.ErrReplayUnavailable) {
		writeError(w, http.StatusGone, "live replay unavailable; use historical search")
		return
	}
	if err != nil {
		slog.Error("live replay snapshot failed", "error", err)
		writeError(w, http.StatusServiceUnavailable, "live replay unavailable")
		return
	}
	var replay []store.Event
	if snapshot != nil {
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		replay, err = snapshot.Events(ctx, 1000)
		cancel()
		closeErr := snapshot.Close()
		err = errors.Join(err, closeErr)
		if errors.Is(err, store.ErrReplayUnavailable) {
			writeError(w, http.StatusGone, "live replay limit exceeded; use historical search")
			return
		}
		if err != nil {
			slog.Error("live replay failed", "error", err)
			writeError(w, http.StatusServiceUnavailable, "live replay unavailable")
			return
		}
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("X-Accel-Buffering", "no")
	controller := http.NewResponseController(w)
	if err := writeLive(controller, w, ": connected\n\n"); err != nil {
		return
	}
	for _, event := range replay {
		if !filters.matches(event) {
			continue
		}
		current, err := h.authorize(r, auth.Query)
		if err != nil || current.DeploymentID != identity.DeploymentID {
			return
		}
		data, err := json.Marshal(event)
		if err != nil {
			slog.Error("live replay serialization failed", "error", err)
			return
		}
		if writeLiveEvent(controller, w, liveEvent{cursor: store.LiveCursor(event), data: data}) != nil {
			return
		}
	}
	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-sub.overflow:
			if err := writeLive(controller, w, "event: overflow\ndata: {}\n\n"); err != nil {
				return
			}
			return
		case event := <-sub.events:
			h.live.consumed(sub, event)
			current, err := h.authorize(r, auth.Query)
			if err != nil || current.DeploymentID != identity.DeploymentID {
				return
			}
			if err := writeLiveEvent(controller, w, event); err != nil {
				return
			}
		case <-heartbeat.C:
			current, err := h.authorize(r, auth.Query)
			if err != nil || current.DeploymentID != identity.DeploymentID {
				return
			}
			if err := writeLive(controller, w, ": heartbeat\n\n"); err != nil {
				return
			}
		}
	}
}

func writeLiveEvent(controller *http.ResponseController, w http.ResponseWriter, event liveEvent) error {
	return writeLive(controller, w, "id: "+event.cursor+"\nevent: log\ndata: "+string(event.data)+"\n\n")
}

func writeLive(controller *http.ResponseController, w http.ResponseWriter, text string) error {
	if err := controller.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil && !errors.Is(err, http.ErrNotSupported) {
		return err
	}
	if _, err := fmt.Fprint(w, text); err != nil {
		return err
	}
	return controller.Flush()
}
