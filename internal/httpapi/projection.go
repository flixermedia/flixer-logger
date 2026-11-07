package httpapi

import (
	"encoding/json"
	"errors"

	"github.com/flixermedia/flixer-logger/internal/store"
)

var projectionFields = map[string]bool{
	"schema_version": true, "event_id": true, "time": true, "ingested_at": true,
	"deployment_id": true, "level": true, "instance_id": true, "host_id": true,
	"app": true, "source": true, "event": true, "message": true, "trace_id": true,
	"span_id": true, "request_id": true, "task_id": true, "duration_ms": true,
	"fields": true, "error": true,
}

func validateProjection(fields []string) error {
	if len(fields) > len(projectionFields) {
		return errors.New("too many projection fields")
	}
	seen := make(map[string]bool, len(fields))
	for _, field := range fields {
		if !projectionFields[field] || seen[field] {
			return errors.New("invalid or duplicate projection field")
		}
		seen[field] = true
	}
	return nil
}

// projectEvents preserves the normal JSON representation, including omitted
// optional fields, and always includes the identifier for correlation.
func projectEvents(events []store.Event, fields []string) ([]map[string]json.RawMessage, error) {
	projected := make([]map[string]json.RawMessage, 0, len(events))
	for _, event := range events {
		data, err := json.Marshal(event)
		if err != nil {
			return nil, err
		}
		var values map[string]json.RawMessage
		if err := json.Unmarshal(data, &values); err != nil {
			return nil, err
		}
		result := make(map[string]json.RawMessage, len(fields)+1)
		result["event_id"] = values["event_id"]
		for _, field := range fields {
			if value, ok := values[field]; ok {
				result[field] = value
			}
		}
		projected = append(projected, result)
	}
	return projected, nil
}
