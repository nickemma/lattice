package lsm

import "sync"

type Version struct {
	Timestamp uint64
	Value     []byte
	Deleted   bool
}

type Transaction struct {
	store    *MVCC
	snapshot uint64
	pending  map[string]Version
}

// MVCC is the small transactional layer used to demonstrate snapshot reads.
// The durable LSM store remains the source of truth for the non-transactional
// path; this layer makes visibility rules explicit before distribution.
type MVCC struct {
	mu     sync.RWMutex
	clock  uint64
	values map[string][]Version
}

func NewMVCC() *MVCC { return &MVCC{values: make(map[string][]Version)} }

func (m *MVCC) Begin() *Transaction {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return &Transaction{store: m, snapshot: m.clock, pending: make(map[string]Version)}
}

func (t *Transaction) Put(key string, value []byte) {
	t.pending[key] = Version{Value: append([]byte(nil), value...)}
}

func (t *Transaction) Delete(key string) { t.pending[key] = Version{Deleted: true} }

func (t *Transaction) Get(key string) ([]byte, bool) {
	if version, ok := t.pending[key]; ok {
		if version.Deleted {
			return nil, false
		}
		return append([]byte(nil), version.Value...), true
	}
	t.store.mu.RLock()
	defer t.store.mu.RUnlock()
	versions := t.store.values[key]
	for n := len(versions) - 1; n >= 0; n-- {
		if versions[n].Timestamp <= t.snapshot {
			if versions[n].Deleted {
				return nil, false
			}
			return append([]byte(nil), versions[n].Value...), true
		}
	}
	return nil, false
}

func (t *Transaction) Commit() uint64 {
	t.store.mu.Lock()
	defer t.store.mu.Unlock()
	t.store.clock++
	commit := t.store.clock
	for key, version := range t.pending {
		version.Timestamp = commit
		t.store.values[key] = append(t.store.values[key], version)
	}
	t.snapshot = commit
	return commit
}
