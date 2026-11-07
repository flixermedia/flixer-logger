package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"time"
	"unicode/utf8"
)

type ErrorGroup struct {
	Fingerprint string    `json:"fingerprint"`
	App         string    `json:"app"`
	Source      string    `json:"source,omitempty"`
	Type        string    `json:"type,omitempty"`
	Code        string    `json:"code,omitempty"`
	Message     string    `json:"message"`
	Count       int       `json:"count"`
	LastSeen    time.Time `json:"last_seen"`
}

// ErrorGroups aggregates indexed error and fatal events within a bounded window.
func (s *Store) ErrorGroups(ctx context.Context, deployment string, start, end time.Time) ([]ErrorGroup, error) {
	groups := make(map[string]*ErrorGroup)
	count := 0
	for _, level := range []string{"error", "fatal"} {
		request := SearchRequest{Start: start, End: end, Fields: map[string]string{"level": level}, Limit: 200}
		for {
			events, next, err := s.Search(ctx, deployment, request)
			if err != nil {
				return nil, err
			}
			for _, event := range events {
				count++
				if count > 10000 {
					return nil, ErrQueryBudget
				}
				var errorType, code string
				if event.Error != nil {
					errorType, code = event.Error.Type, event.Error.Code
				}
				identity, err := json.Marshal([]string{event.App, event.Source, errorType, code, event.Message})
				if err != nil {
					return nil, fmt.Errorf("encode error fingerprint: %w", err)
				}
				digest := sha256.Sum256(identity)
				key := hex.EncodeToString(digest[:])
				group := groups[key]
				if group == nil {
					if len(groups) >= 1000 {
						return nil, ErrQueryBudget
					}
					message := event.Message
					if len(message) > 1024 {
						message = message[:1024]
						for !utf8.ValidString(message) {
							message = message[:len(message)-1]
						}
						message += "…"
					}
					group = &ErrorGroup{
						Fingerprint: key, App: event.App, Source: event.Source, Type: errorType,
						Code: code, Message: message,
					}
					groups[key] = group
				}
				group.Count++
				if event.IngestedAt.After(group.LastSeen) {
					group.LastSeen = event.IngestedAt
				}
			}
			if next == "" {
				break
			}
			request.Cursor = next
		}
	}
	result := make([]ErrorGroup, 0, len(groups))
	for _, group := range groups {
		result = append(result, *group)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Count != result[j].Count {
			return result[i].Count > result[j].Count
		}
		return result[i].Fingerprint < result[j].Fingerprint
	})
	return result, nil
}
