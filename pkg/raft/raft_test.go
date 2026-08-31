package raft

import "testing"

func TestReplicatesOnlyWithQuorum(t *testing.T) {
	c := NewCluster(1, 2, 3)
	if err := c.ElectLeader(1); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Propose(Command{Key: "a", Value: "one"}); err != nil {
		t.Fatal(err)
	}
	for _, id := range []int{1, 2, 3} {
		value, ok, err := c.Read(id, "a")
		if err != nil || !ok || value != "one" {
			t.Fatalf("node %d read = %q, %v, %v", id, value, ok, err)
		}
	}
	_ = c.Kill(2)
	_ = c.Kill(3)
	if _, err := c.Propose(Command{Key: "b", Value: "no quorum"}); err != ErrNoQuorum {
		t.Fatalf("proposal error = %v", err)
	}
}

func TestLeaderFailureAndRecovery(t *testing.T) {
	c := NewCluster(1, 2, 3)
	if err := c.ElectLeader(1); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Propose(Command{Key: "a", Value: "one"}); err != nil {
		t.Fatal(err)
	}
	if err := c.Kill(1); err != nil {
		t.Fatal(err)
	}
	if err := c.ElectLeader(2); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Propose(Command{Key: "b", Value: "two"}); err != nil {
		t.Fatal(err)
	}
	if err := c.Start(1); err != nil {
		t.Fatal(err)
	}
	for _, id := range []int{2, 3} {
		if v, ok, _ := c.Read(id, "b"); !ok || v != "two" {
			t.Fatalf("node %d missing committed value", id)
		}
	}
}

func TestPartitionBlocksCommit(t *testing.T) {
	c := NewCluster(1, 2, 3)
	if err := c.ElectLeader(1); err != nil {
		t.Fatal(err)
	}
	c.Partition(1, 2, true)
	c.Partition(1, 3, true)
	if _, err := c.Propose(Command{Key: "x", Value: "uncommitted"}); err != ErrNoQuorum {
		t.Fatalf("partition proposal error = %v", err)
	}
}

func TestElectionRunsThroughLogicalTicks(t *testing.T) {
	c := NewCluster(1, 2, 3)
	for tick := 0; tick < 5; tick++ {
		c.Tick()
	}
	if leader, ok := c.Leader(); !ok || leader < 1 || leader > 3 {
		t.Fatalf("leader after ticks = %d, %v", leader, ok)
	}
	if _, err := c.Propose(Command{Key: "tick", Value: "elected"}); err != nil {
		t.Fatal(err)
	}
}

func TestPersistentRestartReplaysCommittedState(t *testing.T) {
	directory := t.TempDir()
	c, err := NewPersistentCluster(directory, 1, 2, 3)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.ElectLeader(1); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Propose(Command{Key: "durable", Value: "yes"}); err != nil {
		t.Fatal(err)
	}

	restarted, err := NewPersistentCluster(directory, 1, 2, 3)
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.ElectLeader(2); err != nil {
		t.Fatal(err)
	}
	for _, id := range []int{1, 2, 3} {
		value, ok, err := restarted.Read(id, "durable")
		if err != nil || !ok || value != "yes" {
			t.Fatalf("node %d after restart = %q, %v, %v", id, value, ok, err)
		}
	}
}

func TestCompactedLeaderRepairsLaggingFollowerWithSnapshot(t *testing.T) {
	c := NewCluster(1, 2, 3)
	if err := c.ElectLeader(1); err != nil {
		t.Fatal(err)
	}
	if err := c.Kill(3); err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"one", "two"} {
		if _, err := c.Propose(Command{Key: value, Value: value}); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Compact(1, 2); err != nil {
		t.Fatal(err)
	}
	if err := c.Start(3); err != nil {
		t.Fatal(err)
	}
	c.Tick()
	value, ok, err := c.Read(3, "two")
	if err != nil || !ok || value != "two" {
		t.Fatalf("snapshot-repaired follower = %q, %v, %v", value, ok, err)
	}
	state := c.Snapshot()[2]
	if state.SnapshotIndex != 2 {
		t.Fatalf("follower snapshot index = %d", state.SnapshotIndex)
	}
}

func TestUncommittedConflictIsOverwrittenAfterPartition(t *testing.T) {
	c := NewCluster(1, 2, 3)
	if err := c.ElectLeader(1); err != nil {
		t.Fatal(err)
	}
	c.Partition(1, 2, true)
	c.Partition(1, 3, true)
	if _, err := c.Propose(Command{Key: "conflict", Value: "old"}); err != ErrNoQuorum {
		t.Fatalf("partition proposal error = %v", err)
	}
	c.Partition(1, 2, false)
	c.Partition(1, 3, false)
	if err := c.ElectLeader(2); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Propose(Command{Key: "conflict", Value: "new"}); err != nil {
		t.Fatal(err)
	}
	for _, id := range []int{1, 2, 3} {
		value, ok, err := c.Read(id, "conflict")
		if err != nil || !ok || value != "new" {
			t.Fatalf("node %d conflict value = %q, %v, %v", id, value, ok, err)
		}
	}
}
