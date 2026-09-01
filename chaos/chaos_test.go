package chaos

import (
	"testing"

	"github.com/nickemma/lattice/pkg/raft"
)

func TestLeaderKillStillCommitsWithMajority(t *testing.T) {
	c := raft.NewCluster(1, 2, 3)
	if err := c.ElectLeader(1); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Propose(raft.Command{Key: "before", Value: "ok"}); err != nil {
		t.Fatal(err)
	}
	if err := c.Kill(1); err != nil {
		t.Fatal(err)
	}
	if err := c.ElectLeader(2); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Propose(raft.Command{Key: "after", Value: "ok"}); err != nil {
		t.Fatal(err)
	}
	for _, id := range []int{2, 3} {
		if value, ok, _ := c.Read(id, "after"); !ok || value != "ok" {
			t.Fatalf("node %d lost committed value", id)
		}
	}
}

func TestPartitionCannotCommit(t *testing.T) {
	c := raft.NewCluster(1, 2, 3)
	if err := c.ElectLeader(1); err != nil {
		t.Fatal(err)
	}
	c.Partition(1, 2, true)
	c.Partition(1, 3, true)
	if _, err := c.Propose(raft.Command{Key: "unsafe", Value: "no"}); err != raft.ErrNoQuorum {
		t.Fatalf("partition result = %v", err)
	}
}

func TestLeaderKillRecoveryRepeated(t *testing.T) {
	for run := 0; run < 100; run++ {
		cluster := raft.NewCluster(1, 2, 3)
		if err := cluster.ElectLeader(1); err != nil {
			t.Fatalf("run %d elect initial leader: %v", run, err)
		}
		if _, err := cluster.Propose(raft.Command{Key: "before", Value: "ok"}); err != nil {
			t.Fatalf("run %d initial proposal: %v", run, err)
		}
		if err := cluster.Kill(1); err != nil {
			t.Fatalf("run %d kill leader: %v", run, err)
		}
		if err := cluster.ElectLeader(2); err != nil {
			t.Fatalf("run %d elect replacement: %v", run, err)
		}
		if _, err := cluster.Propose(raft.Command{Key: "after", Value: "ok"}); err != nil {
			t.Fatalf("run %d replacement proposal: %v", run, err)
		}
		for _, id := range []int{2, 3} {
			if value, ok, _ := cluster.Read(id, "after"); !ok || value != "ok" {
				t.Fatalf("run %d node %d lost committed value", run, id)
			}
		}
	}
}
