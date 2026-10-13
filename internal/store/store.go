// Package store saves and loads the per-symbol window state as a JSON file.
package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"muhammedshamil8/livefxhub/internal/window"
)

const version = 1

type Snapshot struct {
	Version int                        `json:"version"`
	SavedAt time.Time                  `json:"saved_at"`
	Symbols map[string][]window.Bucket `json:"symbols"`
}

// Save writes to a temp file then renames, so a crash mid-write never
// leaves a half-written snapshot behind.
func Save(path string, s Snapshot) error {
	s.Version = version
	s.SavedAt = time.Now()

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".snapshot-*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())

	if err := json.NewEncoder(tmp).Encode(s); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}

// Load returns an empty snapshot (not an error) when the file doesn't exist yet.
func Load(path string) (Snapshot, error) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return Snapshot{Symbols: map[string][]window.Bucket{}}, nil
	}
	if err != nil {
		return Snapshot{}, err
	}
	defer f.Close()

	var s Snapshot
	if err := json.NewDecoder(f).Decode(&s); err != nil {
		return Snapshot{}, fmt.Errorf("decode %s: %w", path, err)
	}
	if s.Version != version {
		return Snapshot{}, fmt.Errorf("%s: unsupported snapshot version %d", path, s.Version)
	}
	if s.Symbols == nil {
		s.Symbols = map[string][]window.Bucket{}
	}
	return s, nil
}
