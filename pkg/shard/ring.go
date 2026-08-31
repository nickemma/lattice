// Package shard implements consistent-hash placement and a replicated,
// quorum-read/write facade over Raft groups. Rebalancing is copy-then-swap:
// keys continue to be served by the old ring while moved keys are dual-written
// and copied, then ownership changes atomically.
package shard

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/nickemma/lattice/pkg/raft"
)

var (
	ErrNoOwner             = errors.New("shard: no owner")
	ErrNoQuorum            = raft.ErrNoQuorum
	ErrMigrationInProgress = errors.New("shard: migration in progress")
)

type point struct {
	hash uint64
	id   int
}

type Ring struct {
	points []point
}

func NewRing(shardIDs []int, virtualNodes int) *Ring {
	if virtualNodes < 1 {
		virtualNodes = 1
	}
	r := &Ring{}
	for _, id := range shardIDs {
		for v := 0; v < virtualNodes; v++ {
			r.points = append(r.points, point{hash: hash(fmt.Sprintf("%d:%d", id, v)), id: id})
		}
	}
	sort.Slice(r.points, func(i, j int) bool {
		if r.points[i].hash == r.points[j].hash {
			return r.points[i].id < r.points[j].id
		}
		return r.points[i].hash < r.points[j].hash
	})
	return r
}

func (r *Ring) Shard(key string) int {
	if len(r.points) == 0 {
		return -1
	}
	h := hash(key)
	i := sort.Search(len(r.points), func(i int) bool { return r.points[i].hash >= h })
	if i == len(r.points) {
		i = 0
	}
	return r.points[i].id
}

func hash(value string) uint64 {
	sum := sha256.Sum256([]byte(value))
	return binary.BigEndian.Uint64(sum[:8])
}

type migrationPlan struct {
	from     *Ring
	to       *Ring
	toShards map[int]*raft.Cluster
}

type MigrationReport struct {
	FromShards int           `json:"from_shards"`
	ToShards   int           `json:"to_shards"`
	MovedKeys  int           `json:"moved_keys"`
	Duration   time.Duration `json:"duration"`
	Complete   bool          `json:"complete"`
}

type Store struct {
	mu            sync.RWMutex
	rebalanceMu   sync.RWMutex
	ring          *Ring
	shards        map[int]*raft.Cluster
	replicas      int
	pending       *migrationPlan
	generation    uint64
	migrationHook func()
}

func NewStore(shardIDs []int, replicas int) *Store {
	if replicas < 1 {
		replicas = 1
	}
	s := &Store{ring: NewRing(shardIDs, 32), shards: make(map[int]*raft.Cluster), replicas: replicas}
	for _, id := range shardIDs {
		s.shards[id] = newCluster(id, replicas)
	}
	return s
}

func newCluster(shardID, replicas int) *raft.Cluster {
	ids := make([]int, replicas)
	for i := range ids {
		ids[i] = shardID*100 + i + 1
	}
	cluster := raft.NewCluster(ids...)
	if len(ids) > 0 {
		_ = cluster.ElectLeader(ids[0])
	}
	return cluster
}

func (s *Store) Put(key, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	plan := s.pending
	activeRing := s.ring
	activeCluster := s.shards[activeRing.Shard(key)]
	var oldCluster, newCluster *raft.Cluster
	if plan != nil {
		oldCluster = s.shards[plan.from.Shard(key)]
		newCluster = plan.toShards[plan.to.Shard(key)]
	}
	if activeCluster == nil {
		return fmt.Errorf("%w for key %q", ErrNoOwner, key)
	}
	if plan == nil || oldCluster == newCluster {
		return propose(activeCluster, raft.Command{Key: key, Value: value})
	}
	if err := propose(oldCluster, raft.Command{Key: key, Value: value}); err != nil {
		return err
	}
	return propose(newCluster, raft.Command{Key: key, Value: value})
}

func (s *Store) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	plan := s.pending
	activeRing := s.ring
	activeCluster := s.shards[activeRing.Shard(key)]
	var oldCluster, newCluster *raft.Cluster
	if plan != nil {
		oldCluster = s.shards[plan.from.Shard(key)]
		newCluster = plan.toShards[plan.to.Shard(key)]
	}
	if activeCluster == nil {
		return fmt.Errorf("%w for key %q", ErrNoOwner, key)
	}
	if plan == nil || oldCluster == newCluster {
		return propose(activeCluster, raft.Command{Key: key, Delete: true})
	}
	if err := propose(oldCluster, raft.Command{Key: key, Delete: true}); err != nil {
		return err
	}
	return propose(newCluster, raft.Command{Key: key, Delete: true})
}

func (s *Store) Get(key string) (string, bool, error) {
	s.mu.RLock()
	plan := s.pending
	activeRing := s.ring
	activeCluster := s.shards[activeRing.Shard(key)]
	var oldCluster, newCluster *raft.Cluster
	if plan != nil {
		oldCluster = s.shards[plan.from.Shard(key)]
		newCluster = plan.toShards[plan.to.Shard(key)]
	}
	s.mu.RUnlock()
	if activeCluster == nil {
		return "", false, fmt.Errorf("%w for key %q", ErrNoOwner, key)
	}
	if plan != nil && oldCluster != newCluster {
		value, found, err := quorumGet(newCluster, key)
		if err == nil && found {
			return value, true, nil
		}
		oldValue, oldFound, oldErr := quorumGet(oldCluster, key)
		if oldErr == nil {
			return oldValue, oldFound, nil
		}
		if err != nil {
			return "", false, err
		}
		return "", false, oldErr
	}
	return quorumGet(activeCluster, key)
}

// Rebalance performs a live copy-then-swap migration. While it is copying,
// Put and Delete write moved keys to both the source and destination groups;
// once the new ring is installed all reads route only to the destination.
func (s *Store) Rebalance(shardIDs []int) (MigrationReport, error) {
	started := time.Now()
	if len(shardIDs) == 0 {
		return MigrationReport{}, ErrNoOwner
	}
	s.rebalanceMu.Lock()
	defer s.rebalanceMu.Unlock()

	s.mu.Lock()
	from := s.ring
	to := NewRing(append([]int(nil), shardIDs...), 32)
	if sameRing(from, to) {
		s.mu.Unlock()
		return MigrationReport{FromShards: len(uniqueShardIDs(from)), ToShards: len(uniqueShardIDs(to)), Duration: time.Since(started), Complete: true}, nil
	}
	nextShards := make(map[int]*raft.Cluster, len(shardIDs))
	for _, id := range shardIDs {
		if existing := s.shards[id]; existing != nil {
			nextShards[id] = existing
		} else {
			nextShards[id] = newCluster(id, s.replicas)
		}
	}
	s.pending = &migrationPlan{from: from, to: to, toShards: nextShards}
	s.mu.Unlock()

	keys := make(map[string]string)
	s.mu.RLock()
	for shardID, cluster := range s.shards {
		for key, value := range snapshotCluster(cluster) {
			if _, exists := keys[key]; !exists {
				keys[key] = value
			}
		}
		_ = shardID
	}
	s.mu.RUnlock()
	if s.migrationHook != nil {
		s.migrationHook()
	}
	moved := 0
	for key := range keys {
		if from.Shard(key) == to.Shard(key) {
			continue
		}
		s.mu.RLock()
		source := s.shards[from.Shard(key)]
		target := nextShards[to.Shard(key)]
		currentValue, found := snapshotValue(source, key)
		if target == nil {
			s.mu.RUnlock()
			s.clearPending()
			return MigrationReport{MovedKeys: moved, Duration: time.Since(started)}, fmt.Errorf("%w for key %q", ErrNoOwner, key)
		}
		command := raft.Command{Key: key, Value: currentValue}
		if !found {
			command = raft.Command{Key: key, Delete: true}
		}
		err := propose(target, command)
		s.mu.RUnlock()
		if err != nil {
			s.clearPending()
			return MigrationReport{MovedKeys: moved, Duration: time.Since(started)}, err
		}
		moved++
	}

	s.mu.Lock()
	s.ring = to
	s.shards = nextShards
	s.pending = nil
	s.generation++
	s.mu.Unlock()
	return MigrationReport{FromShards: len(uniqueShardIDs(from)), ToShards: len(uniqueShardIDs(to)), MovedKeys: moved, Duration: time.Since(started), Complete: true}, nil
}

func (s *Store) clearPending() {
	s.mu.Lock()
	s.pending = nil
	s.mu.Unlock()
}

func (s *Store) ShardFor(key string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ring.Shard(key)
}

func (s *Store) ClusterFor(shardID int) *raft.Cluster {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.shards[shardID]
}

func (s *Store) Generation() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.generation
}

func propose(cluster *raft.Cluster, command raft.Command) error {
	if cluster == nil {
		return ErrNoOwner
	}
	_, err := cluster.Propose(command)
	return err
}

func quorumGet(cluster *raft.Cluster, key string) (string, bool, error) {
	if cluster == nil {
		return "", false, ErrNoOwner
	}
	nodes := cluster.Snapshot()
	quorum := len(nodes)/2 + 1
	alive := 0
	values := make(map[string]int)
	for _, node := range nodes {
		if !node.Alive {
			continue
		}
		alive++
		if value, ok := node.KV[key]; ok {
			values[value]++
		}
	}
	if alive < quorum {
		return "", false, ErrNoQuorum
	}
	bestValue, bestCount := "", 0
	for value, count := range values {
		if count > bestCount {
			bestValue, bestCount = value, count
		}
	}
	if bestCount == 0 {
		return "", false, nil
	}
	if bestCount < quorum {
		return "", false, ErrNoQuorum
	}
	return bestValue, true, nil
}

func snapshotCluster(cluster *raft.Cluster) map[string]string {
	result := make(map[string]string)
	for _, node := range cluster.Snapshot() {
		if !node.Alive {
			continue
		}
		for key, value := range node.KV {
			result[key] = value
		}
		break
	}
	return result
}

func snapshotValue(cluster *raft.Cluster, key string) (string, bool) {
	if cluster == nil {
		return "", false
	}
	for _, node := range cluster.Snapshot() {
		if !node.Alive {
			continue
		}
		value, ok := node.KV[key]
		return value, ok
	}
	return "", false
}

func uniqueShardIDs(r *Ring) []int {
	seen := make(map[int]struct{})
	for _, point := range r.points {
		seen[point.id] = struct{}{}
	}
	ids := make([]int, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	return ids
}

func sameRing(a, b *Ring) bool {
	left, right := uniqueShardIDs(a), uniqueShardIDs(b)
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
