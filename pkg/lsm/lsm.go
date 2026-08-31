// Package lsm provides a compact educational LSM-style key-value store.
package lsm

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/nickemma/lattice/pkg/wal"
)

var ErrNotFound = errors.New("lsm: key not found")

type entry struct {
	Key     string `json:"key"`
	Value   []byte `json:"value,omitempty"`
	Deleted bool   `json:"deleted,omitempty"`
}

type Store struct {
	mu     sync.RWMutex
	dir    string
	log    *wal.Log
	mem    map[string]entry
	tables []string // oldest to newest
}

func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	log, err := wal.Open(filepath.Join(dir, "wal.log"))
	if err != nil {
		return nil, err
	}
	s := &Store{dir: dir, log: log, mem: make(map[string]entry)}
	entries, err := os.ReadDir(dir)
	if err != nil {
		_ = log.Close()
		return nil, err
	}
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".sst" {
			s.tables = append(s.tables, filepath.Join(dir, e.Name()))
		}
	}
	sort.Strings(s.tables)
	records, err := log.Replay()
	if err != nil {
		_ = log.Close()
		return nil, err
	}
	for _, r := range records {
		s.mem[r.Key] = entry{Key: r.Key, Value: append([]byte(nil), r.Value...), Deleted: r.Op == "delete"}
	}
	return s, nil
}

func (s *Store) Put(key string, value []byte) error {
	if key == "" {
		return fmt.Errorf("lsm: empty key")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.log.Append(wal.Record{Op: "put", Key: key, Value: value}); err != nil {
		return err
	}
	s.mem[key] = entry{Key: key, Value: append([]byte(nil), value...)}
	return nil
}

func (s *Store) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.log.Append(wal.Record{Op: "delete", Key: key}); err != nil {
		return err
	}
	s.mem[key] = entry{Key: key, Deleted: true}
	return nil
}

func (s *Store) Get(key string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if e, ok := s.mem[key]; ok {
		if e.Deleted {
			return nil, ErrNotFound
		}
		return append([]byte(nil), e.Value...), nil
	}
	for i := len(s.tables) - 1; i >= 0; i-- {
		e, ok, err := readTableKey(s.tables[i], key)
		if err != nil {
			return nil, err
		}
		if ok {
			if e.Deleted {
				return nil, ErrNotFound
			}
			return append([]byte(nil), e.Value...), nil
		}
	}
	return nil, ErrNotFound
}

func (s *Store) Scan(prefix string) (map[string][]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	all := make(map[string]entry)
	for _, path := range s.tables {
		entries, err := readTable(path)
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			all[e.Key] = e
		}
	}
	for k, e := range s.mem {
		all[k] = e
	}
	result := make(map[string][]byte)
	for k, e := range all {
		if len(prefix) > 0 && len(k) >= len(prefix) && k[:len(prefix)] != prefix {
			continue
		}
		if len(prefix) > 0 && len(k) < len(prefix) {
			continue
		}
		if !e.Deleted {
			result[k] = append([]byte(nil), e.Value...)
		}
	}
	return result, nil
}

func (s *Store) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.mem) == 0 {
		return nil
	}
	keys := make([]string, 0, len(s.mem))
	for k := range s.mem {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	path := filepath.Join(s.dir, fmt.Sprintf("%020d.sst", len(s.tables)+1))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	for _, k := range keys {
		if err := enc.Encode(s.mem[k]); err != nil {
			_ = f.Close()
			return err
		}
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	s.tables = append(s.tables, path)
	s.mem = make(map[string]entry)
	return nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.flushLocked(); err != nil {
		return err
	}
	return s.log.Close()
}

func (s *Store) flushLocked() error {
	if len(s.mem) == 0 {
		return nil
	}
	keys := make([]string, 0, len(s.mem))
	for k := range s.mem {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	path := filepath.Join(s.dir, fmt.Sprintf("%020d.sst", len(s.tables)+1))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	for _, k := range keys {
		if err := enc.Encode(s.mem[k]); err != nil {
			_ = f.Close()
			return err
		}
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	s.tables = append(s.tables, path)
	s.mem = make(map[string]entry)
	return nil
}

func (s *Store) Compact() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	merged := make(map[string]entry)
	for _, path := range s.tables {
		entries, err := readTable(path)
		if err != nil {
			return err
		}
		for _, e := range entries {
			merged[e.Key] = e
		}
	}
	for k, e := range s.mem {
		merged[k] = e
	}
	keys := make([]string, 0, len(merged))
	for k := range merged {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	path := filepath.Join(s.dir, "compact.sst")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	for _, k := range keys {
		if err := enc.Encode(merged[k]); err != nil {
			_ = f.Close()
			return err
		}
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	for _, old := range s.tables {
		if err := os.Remove(old); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	final := filepath.Join(s.dir, "00000000000000000001.sst")
	if err := os.Rename(path, final); err != nil {
		return err
	}
	s.tables = []string{final}
	s.mem = make(map[string]entry)
	return nil
}

func readTable(path string) ([]entry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var result []entry
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var e entry
		if err := json.Unmarshal(scanner.Bytes(), &e); err != nil {
			return nil, err
		}
		result = append(result, e)
	}
	return result, scanner.Err()
}

func readTableKey(path, key string) (entry, bool, error) {
	entries, err := readTable(path)
	if err != nil {
		return entry{}, false, err
	}
	for _, e := range entries {
		if e.Key == key {
			return e, true, nil
		}
	}
	return entry{}, false, nil
}
