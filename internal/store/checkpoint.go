package store

import (
	"errors"
	"fmt"
	"path/filepath"

	"github.com/cockroachdb/pebble"
)

// Checkpoint writes a consistent snapshot of every deployment to a new path.
// Its caller must restrict access because snapshots contain all tenants' data.
func (s *Store) Checkpoint(destination string) error {
	if !filepath.IsAbs(destination) || filepath.Clean(destination) == string(filepath.Separator) {
		return errors.New("checkpoint destination must be an absolute non-root path")
	}
	if err := s.db.Checkpoint(destination, pebble.WithFlushedWAL()); err != nil {
		return fmt.Errorf("create Pebble checkpoint: %w", err)
	}
	return nil
}
