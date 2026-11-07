// Package store provides durable, deployment-scoped log storage.
package store

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/cockroachdb/pebble"
)

const (
	maxBatchSize       = 1000
	maxEventSize       = 64 * 1024
	maxBatchTextTokens = 128 * maxBatchSize
	maxInlineTokenSize = 128
)

var ErrInvalidInput = errors.New("invalid input")

type ErrorDetails struct {
	Type  string `json:"type,omitempty"`
	Code  string `json:"code,omitempty"`
	Stack string `json:"stack,omitempty"`
}

// Event is the stored representation. DeploymentID and IngestedAt are assigned
// by the server, not trusted from a producer.
type Event struct {
	SchemaVersion int            `json:"schema_version"`
	EventID       string         `json:"event_id"`
	Time          time.Time      `json:"time"`
	IngestedAt    time.Time      `json:"ingested_at"`
	Level         string         `json:"level"`
	DeploymentID  string         `json:"deployment_id"`
	InstanceID    string         `json:"instance_id"`
	HostID        string         `json:"host_id,omitempty"`
	App           string         `json:"app"`
	Source        string         `json:"source,omitempty"`
	Code          string         `json:"event,omitempty"`
	Message       string         `json:"message"`
	TraceID       string         `json:"trace_id,omitempty"`
	SpanID        string         `json:"span_id,omitempty"`
	RequestID     string         `json:"request_id,omitempty"`
	TaskID        string         `json:"task_id,omitempty"`
	DurationMS    *float64       `json:"duration_ms,omitempty"`
	Fields        map[string]any `json:"fields,omitempty"`
	Error         *ErrorDetails  `json:"error,omitempty"`
}

// Store serializes writes so checking deduplication and committing a batch is atomic.
type Store struct {
	db             *pebble.DB
	path           string
	capacityPolicy CapacityPolicy
	mu             sync.Mutex
	lastIngestNS   int64
}

func Open(dataDir string) (*Store, error) {
	if dataDir == "" {
		return nil, errors.New("data directory is required")
	}
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return nil, fmt.Errorf("create data directory: %w", err)
	}
	if err := checkStoreDirectory(dataDir, false, false); err != nil {
		return nil, err
	}
	path := dataDir
	for _, name := range []string{"logs", "logger", "pebble"} {
		path = filepath.Join(path, name)
		if err := os.Mkdir(path, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
			return nil, fmt.Errorf("create log store directory: %w", err)
		}
		if err := checkStoreDirectory(path, name == "logger", name != "logs"); err != nil {
			return nil, err
		}
	}
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("open log store: %w", err)
	}
	if err := rebuildTextIndex(db); err != nil {
		closeErr := db.Close()
		return nil, errors.Join(fmt.Errorf("rebuild text index: %w", err), closeErr)
	}
	if err := rebuildAggregates(db); err != nil {
		closeErr := db.Close()
		return nil, errors.Join(fmt.Errorf("rebuild aggregates: %w", err), closeErr)
	}
	if err := rebuildSourceAggregates(db); err != nil {
		closeErr := db.Close()
		return nil, errors.Join(fmt.Errorf("rebuild source aggregates: %w", err), closeErr)
	}
	if err := rebuildUsage(db); err != nil {
		closeErr := db.Close()
		return nil, errors.Join(fmt.Errorf("rebuild logical usage: %w", err), closeErr)
	}
	lastIngestNS, err := latestIngestTime(db)
	if err != nil {
		closeErr := db.Close()
		return nil, errors.Join(fmt.Errorf("read latest ingestion time: %w", err), closeErr)
	}
	policy, err := readCapacityPolicy(db)
	if err != nil {
		closeErr := db.Close()
		return nil, errors.Join(fmt.Errorf("read capacity policy: %w", err), closeErr)
	}
	return &Store{db: db, path: path, lastIngestNS: lastIngestNS, capacityPolicy: policy}, nil
}

func checkStoreDirectory(path string, private, requireOwner bool) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("inspect log store directory %q: %w", path, err)
	}
	owner, ok := info.Sys().(*syscall.Stat_t)
	if !info.IsDir() || !ok || (owner.Uid != uint32(os.Geteuid()) && owner.Uid != 0) ||
		info.Mode().Perm()&0o022 != 0 || (requireOwner && owner.Uid != uint32(os.Geteuid())) ||
		(private && info.Mode().Perm()&0o077 != 0) {
		return fmt.Errorf("unsafe log store directory %q: data and logs must be owned by the logger user or root and not writable by other users; logger and pebble must be owned by the logger user, and logger must be private", path)
	}
	return nil
}

func (s *Store) Close() error { return s.db.Close() }

func latestIngestTime(db *pebble.DB) (int64, error) {
	value, closer, err := db.Get([]byte("metadata/last-ingest-ns"))
	if err == nil {
		n, parseErr := strconv.ParseInt(string(value), 10, 64)
		return n, errors.Join(parseErr, closer.Close())
	}
	if !errors.Is(err, pebble.ErrNotFound) {
		return 0, err
	}
	iter, err := db.NewIter(&pebble.IterOptions{LowerBound: []byte("events/"), UpperBound: []byte("events0")})
	if err != nil {
		return 0, err
	}
	defer iter.Close()
	var latest int64
	for valid := iter.First(); valid; valid = iter.Next() {
		parts := strings.Split(string(iter.Key()), "/")
		if len(parts) != 4 {
			return 0, errors.New("invalid event key")
		}
		n, err := strconv.ParseInt(parts[2], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid event key time: %w", err)
		}
		if n > latest {
			latest = n
		}
	}
	return latest, iter.Error()
}

// Ready verifies that the store's required index metadata is readable.
func (s *Store) Ready() error {
	for _, key := range []string{"metadata/text-index-v2", aggregateMarker, sourceAggregateMarker, quotaMarker} {
		value, closer, err := s.db.Get([]byte(key))
		if err != nil {
			return fmt.Errorf("read store metadata: %w", err)
		}
		valid := string(value) == "1"
		if err := closer.Close(); err != nil {
			return err
		}
		if !valid {
			return errors.New("invalid store metadata")
		}
	}
	return nil
}

// Rebuilding is resumable: existing postings are overwritten idempotently and
// the marker is written only once every batch has been committed.
func rebuildTextIndex(db *pebble.DB) error {
	const marker = "metadata/text-index-v2"
	_, closer, err := db.Get([]byte(marker))
	if err == nil {
		return closer.Close()
	}
	if !errors.Is(err, pebble.ErrNotFound) {
		return err
	}
	iter, err := db.NewIter(&pebble.IterOptions{LowerBound: []byte("events/"), UpperBound: []byte("events0")})
	if err != nil {
		return err
	}
	defer iter.Close()
	batch := db.NewBatch()
	defer func() { batch.Close() }()
	count := 0
	for valid := iter.First(); valid; valid = iter.Next() {
		var event Event
		if err := json.Unmarshal(iter.Value(), &event); err != nil {
			return fmt.Errorf("decode event: %w", err)
		}
		tokens, err := eventTextTokens(event)
		if err != nil {
			return fmt.Errorf("event %q cannot be indexed: %w", event.EventID, err)
		}
		for _, token := range tokens {
			if err := batch.Set(indexKey(event, "text", token), iter.Key(), nil); err != nil {
				return err
			}
		}
		count += len(tokens) + 1
		if count >= 4096 {
			if err := batch.Commit(pebble.Sync); err != nil {
				return err
			}
			if err := batch.Close(); err != nil {
				return err
			}
			batch = db.NewBatch()
			count = 0
		}
	}
	if err := iter.Error(); err != nil {
		return err
	}
	if err := batch.Set([]byte(marker), []byte("1"), nil); err != nil {
		return err
	}
	return batch.Commit(pebble.Sync)
}

func segment(value string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(value))
}

func prefix(deployment string) string { return "events/" + segment(deployment) + "/" }
func idKey(deployment, id string) []byte {
	return []byte("ids/" + segment(deployment) + "/" + segment(id))
}

func indexes(event Event) map[string]string {
	return map[string]string{
		"app": event.App, "instance": event.InstanceID, "level": event.Level,
		"source": event.Source, "event": event.Code, "trace": event.TraceID,
		"request": event.RequestID, "task": event.TaskID,
	}
}

func indexPrefix(deployment, field, value string) string {
	if field == "text" && len(value) > maxInlineTokenSize {
		digest := sha256.Sum256([]byte(value))
		value = fmt.Sprintf("_sha256_%x", digest)
	}
	return "index/" + segment(deployment) + "/" + field + "/" + segment(value) + "/"
}

func indexKey(event Event, field, value string) []byte {
	return []byte(fmt.Sprintf("%s%020d/%s", indexPrefix(event.DeploymentID, field, value),
		event.IngestedAt.UnixNano(), segment(event.EventID)))
}

// textTokens uses exact, case-insensitive Unicode letter/number tokens.
func textTokens(text string) ([]string, error) {
	if !utf8.ValidString(text) {
		return nil, errors.New("invalid UTF-8 in text")
	}
	seen := make(map[string]struct{})
	var tokens []string
	for _, token := range strings.FieldsFunc(strings.ToLower(text), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsNumber(r)
	}) {
		if _, ok := seen[token]; ok {
			continue
		}
		seen[token] = struct{}{}
		tokens = append(tokens, token)
	}
	return tokens, nil
}

func eventTextTokens(event Event) ([]string, error) {
	text := event.Message
	if event.Error != nil {
		text += " " + event.Error.Type + " " + event.Error.Code + " " + event.Error.Stack
	}
	return textTokens(text)
}

func validate(event Event) error {
	if event.SchemaVersion != 1 || event.EventID == "" || len(event.EventID) > 128 ||
		event.Time.IsZero() || event.InstanceID == "" || event.App == "" || event.Message == "" {
		return errors.New("event requires schema version 1, ID, timestamp, instance, app and message")
	}
	if !utf8.ValidString(event.EventID) || !utf8.ValidString(event.Message) {
		return errors.New("event contains invalid UTF-8")
	}
	if !utf8.ValidString(event.HostID) || len(event.HostID) > 1024 ||
		!utf8.ValidString(event.SpanID) || len(event.SpanID) > 1024 ||
		(event.DurationMS != nil && *event.DurationMS < 0) {
		return errors.New("invalid event metadata")
	}
	for field, value := range indexes(event) {
		if len(value) > 1024 || !utf8.ValidString(value) {
			return fmt.Errorf("invalid %s index value", field)
		}
	}
	switch event.Level {
	case "trace", "debug", "info", "warn", "error", "fatal":
	default:
		return errors.New("invalid event level")
	}
	return nil
}

// PutBatch validates the entire batch before durably storing it. Repeated IDs
// within a deployment are ignored, including retries in later batches.
func (s *Store) PutBatch(deployment string, events []Event) (int, error) {
	inserted, err := s.PutBatchEvents(deployment, events)
	return len(inserted), err
}

// PutBatchEvents returns only events newly committed by this batch.
func (s *Store) PutBatchEvents(deployment string, events []Event) ([]Event, error) {
	if deployment == "" || len(deployment) > 128 || !utf8.ValidString(deployment) {
		return nil, fmt.Errorf("%w: invalid deployment", ErrInvalidInput)
	}
	if len(events) == 0 || len(events) > maxBatchSize {
		return nil, fmt.Errorf("%w: invalid batch size", ErrInvalidInput)
	}
	normalized := make([]Event, len(events))
	textTokens := make([][]string, len(events))
	tokenCount := 0
	for i := range events {
		normalized[i] = events[i]
		normalized[i].DeploymentID = deployment
		if err := validate(normalized[i]); err != nil {
			return nil, fmt.Errorf("%w: event %d: %v", ErrInvalidInput, i, err)
		}
		encoded, err := json.Marshal(normalized[i])
		if err != nil {
			return nil, fmt.Errorf("%w: serialize event %d: %v", ErrInvalidInput, i, err)
		}
		if len(encoded) > maxEventSize {
			return nil, fmt.Errorf("%w: event %d exceeds size limit", ErrInvalidInput, i)
		}
		textTokens[i], err = eventTextTokens(normalized[i])
		if err != nil {
			return nil, fmt.Errorf("%w: index event %d: %v", ErrInvalidInput, i, err)
		}
		tokenCount += len(textTokens[i])
		if tokenCount > maxBatchTextTokens {
			return nil, fmt.Errorf("%w: batch text index budget exceeded; use smaller batches", ErrInvalidInput)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	nowNS := time.Now().UnixNano()
	if nowNS <= s.lastIngestNS {
		if s.lastIngestNS == int64(^uint64(0)>>1) {
			return nil, errors.New("ingestion timestamp exhausted")
		}
		nowNS = s.lastIngestNS + 1
	}
	now := time.Unix(0, nowNS).UTC()
	encoded := make([][]byte, len(events))
	for i := range events {
		normalized[i].IngestedAt = now
		var err error
		encoded[i], err = json.Marshal(normalized[i])
		if err != nil {
			return nil, fmt.Errorf("%w: serialize event %d: %v", ErrInvalidInput, i, err)
		}
		if len(encoded[i]) > maxEventSize {
			return nil, fmt.Errorf("%w: event %d exceeds size limit", ErrInvalidInput, i)
		}
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	seen := make(map[string]bool, len(events))
	inserted := make([]Event, 0, len(events))
	counts := make(map[string]int64)
	indexPrefixes := make(map[string]string)
	eventPrefix := prefix(deployment)
	timestamp := fmt.Sprintf("%020d/", nowNS)
	indexFor := func(field, value string) string {
		key := field + "\x00" + value
		p, ok := indexPrefixes[key]
		if !ok {
			p = indexPrefix(deployment, field, value)
			indexPrefixes[key] = p
		}
		return p
	}
	var addedBytes uint64
	for i, event := range normalized {
		if seen[event.EventID] {
			continue
		}
		seen[event.EventID] = true
		key := idKey(deployment, event.EventID)
		_, closer, err := s.db.Get(key)
		if err == nil {
			if err := closer.Close(); err != nil {
				return nil, err
			}
			continue
		}
		if !errors.Is(err, pebble.ErrNotFound) {
			return nil, fmt.Errorf("check event ID: %w", err)
		}
		// Ingestion time avoids producer clock skew changing storage order.
		suffix := timestamp + segment(event.EventID)
		eventKey := []byte(eventPrefix + suffix)
		if err := batch.Set(eventKey, encoded[i], nil); err != nil {
			return nil, err
		}
		if err := batch.Set(key, eventKey, nil); err != nil {
			return nil, err
		}
		for field, value := range indexes(event) {
			if value == "" {
				continue
			}
			if err := batch.Set([]byte(indexFor(field, value)+suffix), eventKey, nil); err != nil {
				return nil, err
			}
		}
		for _, token := range textTokens[i] {
			if err := batch.Set([]byte(indexFor("text", token)+suffix), eventKey, nil); err != nil {
				return nil, err
			}
		}
		inserted = append(inserted, event)
		addedBytes += uint64(len(encoded[i]))
		changeEventCounts(counts, event, 1)
	}
	usage, err := s.LogicalUsage(deployment)
	if err != nil {
		return nil, fmt.Errorf("read logical usage: %w", err)
	}
	policy, err := s.QuotaPolicy(deployment)
	if err != nil {
		return nil, fmt.Errorf("read quota policy: %w", err)
	}
	if ^uint64(0)-usage < addedBytes {
		return nil, errors.New("deployment usage overflow")
	}
	if addedBytes > 0 && policy.MaxBytes > 0 && usage+addedBytes > policy.MaxBytes {
		return nil, ErrQuotaExceeded
	}
	if err := setUsage(batch, deployment, usage+addedBytes); err != nil {
		return nil, err
	}
	if err := applyCounts(s.db, batch, counts); err != nil {
		return nil, fmt.Errorf("update aggregates: %w", err)
	}
	if err := batch.Set([]byte("metadata/last-ingest-ns"), []byte(strconv.FormatInt(nowNS, 10)), nil); err != nil {
		return nil, err
	}
	if addedBytes > 0 {
		if err := s.checkCapacityLocked(uint64(batch.Len())); err != nil {
			return nil, err
		}
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return nil, fmt.Errorf("commit log batch: %w", err)
	}
	s.lastIngestNS = nowNS
	sort.Slice(inserted, func(i, j int) bool {
		return segment(inserted[i].EventID) < segment(inserted[j].EventID)
	})
	return inserted, nil
}

// QueryIndex returns newest-first matches for an indexed field. The cursor is
// opaque and scoped to the requested deployment and field/value pair.
func (s *Store) QueryIndex(deployment, field, value, cursor string, limit int) ([]Event, string, error) {
	switch field {
	case "app", "instance", "level", "source", "event", "trace", "request", "task", "text":
	default:
		return nil, "", fmt.Errorf("%w: unsupported index field", ErrInvalidInput)
	}
	if deployment == "" || len(deployment) > 128 || value == "" || len(value) > 1024 || limit < 1 || limit > 1000 {
		return nil, "", fmt.Errorf("%w: invalid index query", ErrInvalidInput)
	}
	if field == "text" {
		tokens, err := textTokens(value)
		if err != nil || len(tokens) != 1 || strings.ToLower(value) != tokens[0] {
			return nil, "", fmt.Errorf("%w: text query must be one token", ErrInvalidInput)
		}
		value = tokens[0]
	}
	p := indexPrefix(deployment, field, value)
	var before []byte
	if cursor != "" {
		var err error
		before, err = base64.RawURLEncoding.DecodeString(cursor)
		if err != nil || !strings.HasPrefix(string(before), p) || len(before) > 4096 {
			return nil, "", fmt.Errorf("%w: invalid query cursor", ErrInvalidInput)
		}
	}
	snapshot := s.db.NewSnapshot()
	defer snapshot.Close()
	iter, err := snapshot.NewIter(&pebble.IterOptions{LowerBound: []byte(p), UpperBound: []byte(p + "~")})
	if err != nil {
		return nil, "", err
	}
	defer iter.Close()
	result := make([]Event, 0, limit)
	var last []byte
	valid := iter.Last()
	if before != nil {
		valid = iter.SeekLT(before)
	}
	for valid && len(result) < limit {
		last = append(last[:0], iter.Key()...)
		data, closer, err := snapshot.Get(iter.Value())
		if err != nil {
			return nil, "", fmt.Errorf("read indexed event: %w", err)
		}
		var event Event
		decodeErr := json.Unmarshal(data, &event)
		closeErr := closer.Close()
		if decodeErr != nil {
			return nil, "", fmt.Errorf("decode indexed event: %w", decodeErr)
		}
		if closeErr != nil {
			return nil, "", closeErr
		}
		result = append(result, event)
		valid = iter.Prev()
	}
	if err := iter.Error(); err != nil {
		return nil, "", fmt.Errorf("iterate index: %w", err)
	}
	next := ""
	if valid && len(last) > 0 {
		next = base64.RawURLEncoding.EncodeToString(last)
	}
	return result, next, nil
}

// DeleteBefore removes a bounded batch of old events and associated indexes.
// The cutoff uses ingestion time, not the producer's potentially skewed clock.
func (s *Store) DeleteBefore(deployment string, cutoff time.Time, limit int) (int, error) {
	if deployment == "" || len(deployment) > 128 || cutoff.IsZero() || limit < 1 || limit > 1000 {
		return 0, errors.New("invalid retention request")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.deleteBeforeLocked(deployment, cutoff, limit)
}

func (s *Store) deleteBeforeLocked(deployment string, cutoff time.Time, limit int) (int, error) {
	p := prefix(deployment)
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte(p), UpperBound: []byte(p + "~")})
	if err != nil {
		return 0, err
	}
	defer iter.Close()
	batch := s.db.NewBatch()
	defer batch.Close()
	deleted := 0
	var deletedBytes uint64
	counts := make(map[string]int64)
	for valid := iter.First(); valid && deleted < limit; valid = iter.Next() {
		var event Event
		if err := json.Unmarshal(iter.Value(), &event); err != nil {
			return 0, fmt.Errorf("decode retained event: %w", err)
		}
		if event.DeploymentID != deployment {
			return 0, errors.New("retained event deployment does not match key")
		}
		if !event.IngestedAt.Before(cutoff) {
			break
		}
		if err := deleteRetainedEvent(batch, iter.Key(), event); err != nil {
			return 0, err
		}
		deletedBytes += uint64(len(iter.Value()))
		deleted++
		changeEventCounts(counts, event, -1)
	}
	if err := iter.Error(); err != nil {
		return 0, err
	}
	if err := s.commitRetention(batch, deployment, deletedBytes, counts); err != nil {
		return 0, err
	}
	return deleted, nil
}

func deleteRetainedEvent(batch *pebble.Batch, key []byte, event Event) error {
	if err := batch.Delete(key, nil); err != nil {
		return err
	}
	if err := batch.Delete(idKey(event.DeploymentID, event.EventID), nil); err != nil {
		return err
	}
	for field, value := range indexes(event) {
		if value != "" {
			if err := batch.Delete(indexKey(event, field, value), nil); err != nil {
				return err
			}
		}
	}
	tokens, err := eventTextTokens(event)
	if err != nil {
		return fmt.Errorf("decode retained event tokens: %w", err)
	}
	for _, token := range tokens {
		if err := batch.Delete(indexKey(event, "text", token), nil); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) commitRetention(batch *pebble.Batch, deployment string, deletedBytes uint64, counts map[string]int64) error {
	usage, err := s.LogicalUsage(deployment)
	if err != nil {
		return fmt.Errorf("read logical usage: %w", err)
	}
	if deletedBytes > usage {
		return errors.New("deployment usage underflow")
	}
	if err := setUsage(batch, deployment, usage-deletedBytes); err != nil {
		return err
	}
	if err := applyCounts(s.db, batch, counts); err != nil {
		return fmt.Errorf("update retained aggregates: %w", err)
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("commit retention: %w", err)
	}
	return nil
}

// Recent returns at most limit events, newest first, for one deployment.
func (s *Store) Recent(deployment string, limit int) ([]Event, error) {
	if deployment == "" || len(deployment) > 128 || limit < 1 || limit > 1000 {
		return nil, errors.New("invalid deployment or limit")
	}
	p := prefix(deployment)
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte(p), UpperBound: []byte(p + "~")})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	result := make([]Event, 0, limit)
	for valid := iter.Last(); valid && len(result) < limit; valid = iter.Prev() {
		if !strings.HasPrefix(string(iter.Key()), p) {
			break
		}
		var event Event
		if err := json.Unmarshal(iter.Value(), &event); err != nil {
			return nil, fmt.Errorf("decode stored event: %w", err)
		}
		result = append(result, event)
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterate logs: %w", err)
	}
	return result, nil
}
