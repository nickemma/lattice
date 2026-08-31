package shard

import (
	"errors"
	"testing"

	"github.com/nickemma/lattice/pkg/raft"
)

func TestRingPlacementIsDeterministic(t *testing.T) {
	r := NewRing([]int{1, 2, 3}, 16)
	for _, key := range []string{"a", "b", "c", "d"} {
		if r.Shard(key) != r.Shard(key) {
			t.Fatalf("placement changed for %q", key)
		}
	}
}

func TestReplicatedShardedStore(t *testing.T) {
	s := NewStore([]int{1, 2}, 3)
	if err := s.Put("user:1", "alice"); err != nil {
		t.Fatal(err)
	}
	v, ok, err := s.Get("user:1")
	if err != nil || !ok || v != "alice" {
		t.Fatalf("get = %q, %v, %v", v, ok, err)
	}
	if s.ShardFor("user:1") < 1 {
		t.Fatal("missing shard placement")
	}
}

func TestQuorumReadDoesNotServeWithMinority(t *testing.T) {
	s := NewStore([]int{1}, 3)
	if err := s.Put("account", "alice"); err != nil {
		t.Fatal(err)
	}
	cluster := s.ClusterFor(s.ShardFor("account"))
	nodes := cluster.Snapshot()
	if err := cluster.Kill(nodes[1].ID); err != nil {
		t.Fatal(err)
	}
	if err := cluster.Kill(nodes[2].ID); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Get("account"); !errors.Is(err, raft.ErrNoQuorum) {
		t.Fatalf("minority read error = %v", err)
	}
}

func TestLiveRebalancePreservesKeys(t *testing.T) {
	s := NewStore([]int{1, 2}, 3)
	keys := []string{"a", "b", "c", "d", "e", "f", "g", "h"}
	for _, key := range keys {
		if err := s.Put(key, "value-"+key); err != nil {
			t.Fatal(err)
		}
	}
	before := s.Generation()
	report, err := s.Rebalance([]int{1, 2, 3})
	if err != nil || !report.Complete || report.MovedKeys == 0 {
		t.Fatalf("rebalance report = %+v, %v", report, err)
	}
	if s.Generation() != before+1 {
		t.Fatalf("generation = %d, before = %d", s.Generation(), before)
	}
	for _, key := range keys {
		value, ok, err := s.Get(key)
		if err != nil || !ok || value != "value-"+key {
			t.Fatalf("key %q after rebalance = %q, %v, %v", key, value, ok, err)
		}
	}
}

func TestRebalanceMovesDeletesAndUpdates(t *testing.T) {
	s := NewStore([]int{1, 2}, 3)
	if err := s.Put("keep", "old"); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("remove", "gone"); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete("remove"); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("keep", "new"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Rebalance([]int{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	value, ok, err := s.Get("keep")
	if err != nil || !ok || value != "new" {
		t.Fatalf("updated value = %q, %v, %v", value, ok, err)
	}
	if _, ok, err := s.Get("remove"); err != nil || ok {
		t.Fatalf("deleted value = %v, %v", ok, err)
	}
}

func TestWritesDuringMigrationReachNewOwner(t *testing.T) {
	s := NewStore([]int{1, 2}, 3)
	from := NewRing([]int{1, 2}, 32)
	to := NewRing([]int{1, 2, 3}, 32)
	key := "during-migration"
	for n := 0; from.Shard(key) == to.Shard(key); n++ {
		key = "during-migration-" + string(rune('a'+n))
	}
	hold := make(chan struct{})
	entered := make(chan struct{})
	s.migrationHook = func() {
		close(entered)
		<-hold
	}
	result := make(chan error, 1)
	go func() {
		_, err := s.Rebalance([]int{1, 2, 3})
		result <- err
	}()
	<-entered
	if err := s.Put(key, "written-during-migration"); err != nil {
		t.Fatal(err)
	}
	close(hold)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	value, ok, err := s.Get(key)
	if err != nil || !ok || value != "written-during-migration" {
		t.Fatalf("migrated concurrent write = %q, %v, %v", value, ok, err)
	}
}
