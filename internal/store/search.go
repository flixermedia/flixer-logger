package store

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/cockroachdb/pebble"
)

var ErrQueryBudget = errors.New("query candidate budget exceeded")

// SearchRequest selects events by ingestion time and intersects exact indexes.
// Text modes are all tokens (default), phrase, bounded substring and regex.
type SearchRequest struct {
	Start    time.Time         `json:"start"`
	End      time.Time         `json:"end"`
	Fields   map[string]string `json:"fields,omitempty"`
	Text     string            `json:"text,omitempty"`
	TextMode string            `json:"text_mode,omitempty"`
	Limit    int               `json:"limit"`
	Cursor   string            `json:"cursor,omitempty"`
	Order    string            `json:"order,omitempty"`
	Follow   bool              `json:"follow,omitempty"`
}

type searchTerm struct {
	Field string `json:"field"`
	Value string `json:"value"`
}

type searchCursor struct {
	Fingerprint [32]byte `json:"fingerprint"`
	Key         string   `json:"key"`
}

// Search intersects postings before loading event bodies and fails explicitly
// rather than performing an unbounded scan.
func (s *Store) Search(ctx context.Context, deployment string, request SearchRequest) ([]Event, string, error) {
	if deployment == "" || len(deployment) > 128 || !utf8.ValidString(deployment) || request.Start.IsZero() || request.End.IsZero() ||
		!request.Start.Before(request.End) || request.End.Sub(request.Start) > 31*24*time.Hour ||
		request.Limit < 1 || request.Limit > 200 || len(request.Fields) > 8 || len(request.Cursor) > 8192 {
		return nil, "", fmt.Errorf("%w: invalid search bounds", ErrInvalidInput)
	}
	terms := make([]searchTerm, 0, len(request.Fields)+8)
	queue := request.Fields["task_queue"]
	if queue != "" && request.Fields["task"] == "" {
		return nil, "", fmt.Errorf("%w: task_queue requires task", ErrInvalidInput)
	}
	if request.Order != "" && request.Order != "newest" && request.Order != "oldest" {
		return nil, "", fmt.Errorf("%w: invalid search order", ErrInvalidInput)
	}
	if request.Follow && request.Order != "oldest" {
		return nil, "", fmt.Errorf("%w: follow requires oldest order", ErrInvalidInput)
	}
	for field, value := range request.Fields {
		switch field {
		case "app", "instance", "level", "source", "event", "trace", "request", "task", "task_queue":
		default:
			return nil, "", fmt.Errorf("%w: unsupported search field", ErrInvalidInput)
		}
		if value == "" || len(value) > 1024 || !utf8.ValidString(value) {
			return nil, "", fmt.Errorf("%w: invalid search value", ErrInvalidInput)
		}
		if field != "task_queue" {
			terms = append(terms, searchTerm{field, value})
		}
	}
	mode := request.TextMode
	if mode == "" {
		mode = "all"
	}
	if mode != "all" && mode != "phrase" && mode != "substring" && mode != "regex" {
		return nil, "", fmt.Errorf("%w: invalid search text mode", ErrInvalidInput)
	}
	var pattern *regexp.Regexp
	if request.Text != "" {
		if !utf8.ValidString(request.Text) || len(request.Text) > 1024 {
			return nil, "", fmt.Errorf("%w: invalid search text", ErrInvalidInput)
		}
		if mode == "regex" {
			if len(request.Text) > 256 {
				return nil, "", fmt.Errorf("%w: regular expression exceeds 256 bytes", ErrInvalidInput)
			}
			var err error
			pattern, err = regexp.Compile(request.Text)
			if err != nil {
				return nil, "", fmt.Errorf("%w: invalid regular expression", ErrInvalidInput)
			}
		} else {
			tokens, err := textTokens(request.Text)
			if err != nil || len(tokens) == 0 || len(tokens) > 8 {
				return nil, "", fmt.Errorf("%w: invalid search text", ErrInvalidInput)
			}
			if mode != "substring" {
				for _, token := range tokens {
					terms = append(terms, searchTerm{"text", token})
				}
			}
		}
	}
	priority := map[string]int{"trace": 0, "request": 1, "task": 2, "event": 3, "text": 4, "source": 5, "instance": 6, "level": 7, "app": 8}
	sort.Slice(terms, func(i, j int) bool {
		if terms[i].Field == terms[j].Field {
			return terms[i].Value < terms[j].Value
		}
		return priority[terms[i].Field] < priority[terms[j].Field]
	})
	identityTerms := terms
	if queue != "" || request.Order == "oldest" {
		identityTerms = append([]searchTerm(nil), terms...)
		if queue != "" {
			identityTerms = append(identityTerms, searchTerm{"task_queue", queue})
		}
		if request.Order == "oldest" {
			identityTerms = append(identityTerms, searchTerm{"order", "oldest"})
		}
	}
	identity, err := json.Marshal(struct {
		Deployment string
		Start, End time.Time
		Terms      []searchTerm
		Text       string
		TextMode   string
	}{deployment, request.Start.UTC(), request.End.UTC(), identityTerms, request.Text, mode})
	if err != nil {
		return nil, "", fmt.Errorf("encode search identity: %w", err)
	}
	fingerprint := sha256.Sum256(identity)
	p := prefix(deployment)
	if len(terms) > 0 {
		p = indexPrefix(deployment, terms[0].Field, terms[0].Value)
	}
	if request.Start.Before(time.Unix(0, 0)) || request.End.After(time.Unix(0, 1<<63-1)) {
		return nil, "", fmt.Errorf("%w: unsupported search time", ErrInvalidInput)
	}
	start := fmt.Sprintf("%s%020d/", p, request.Start.UnixNano())
	end := fmt.Sprintf("%s%020d/", p, request.End.UnixNano())
	if request.Cursor != "" {
		data, err := base64.RawURLEncoding.DecodeString(request.Cursor)
		var cursor searchCursor
		if err != nil || json.Unmarshal(data, &cursor) != nil || cursor.Fingerprint != fingerprint ||
			!strings.HasPrefix(cursor.Key, p) || cursor.Key < start || cursor.Key >= end {
			return nil, "", fmt.Errorf("%w: invalid search cursor", ErrInvalidInput)
		}
		if request.Order == "oldest" {
			start = cursor.Key + "\x00"
		} else {
			end = cursor.Key
		}
	}
	snapshot := s.db.NewSnapshot()
	defer snapshot.Close()
	iter, err := snapshot.NewIter(&pebble.IterOptions{LowerBound: []byte(start), UpperBound: []byte(end)})
	if err != nil {
		return nil, "", err
	}
	defer iter.Close()
	result := make([]Event, 0, request.Limit)
	var last string
	const maxCandidates = 10000
	count := 0
	first, nextEntry := iter.Last, iter.Prev
	if request.Order == "oldest" {
		first, nextEntry = iter.First, iter.Next
	}
	for valid := first(); valid; valid = nextEntry() {
		if err := ctx.Err(); err != nil {
			return nil, "", err
		}
		count++
		if count > maxCandidates {
			return nil, "", ErrQueryBudget
		}
		key := string(iter.Key())
		suffix := strings.TrimPrefix(key, p)
		matches := true
		remaining := terms
		if len(terms) > 0 {
			remaining = terms[1:]
		}
		for _, term := range remaining {
			_, closer, err := snapshot.Get([]byte(indexPrefix(deployment, term.Field, term.Value) + suffix))
			if errors.Is(err, pebble.ErrNotFound) {
				matches = false
				break
			}
			if err != nil {
				return nil, "", fmt.Errorf("read search index: %w", err)
			}
			if err := closer.Close(); err != nil {
				return nil, "", err
			}
		}
		if !matches {
			continue
		}
		var event Event
		if len(terms) == 0 {
			if err := json.Unmarshal(iter.Value(), &event); err != nil {
				return nil, "", fmt.Errorf("decode search event: %w", err)
			}
		} else {
			data, closer, err := snapshot.Get(iter.Value())
			if err != nil {
				return nil, "", fmt.Errorf("read search event: %w", err)
			}
			decodeErr := json.Unmarshal(data, &event)
			closeErr := closer.Close()
			if decodeErr != nil || closeErr != nil {
				return nil, "", errors.Join(decodeErr, closeErr)
			}
		}
		if queue != "" {
			actual, ok := event.Fields["task_queue"].(string)
			if !ok || actual != queue {
				continue
			}
		}
		if request.Text != "" && mode != "all" {
			text := event.Message
			if event.Error != nil {
				text += " " + event.Error.Type + " " + event.Error.Code + " " + event.Error.Stack
			}
			if pattern != nil && !pattern.MatchString(text) {
				continue
			}
			if pattern == nil && !strings.Contains(strings.ToLower(text), strings.ToLower(request.Text)) {
				continue
			}
		}
		result = append(result, event)
		last = key
		if len(result) == request.Limit {
			break
		}
	}
	if err := iter.Error(); err != nil {
		return nil, "", err
	}
	next := ""
	if last != "" && (request.Follow || (len(result) == request.Limit && nextEntry())) {
		data, err := json.Marshal(searchCursor{Fingerprint: fingerprint, Key: last})
		if err != nil {
			return nil, "", err
		}
		next = base64.RawURLEncoding.EncodeToString(data)
	}
	if err := iter.Error(); err != nil {
		return nil, "", err
	}
	return result, next, nil
}
