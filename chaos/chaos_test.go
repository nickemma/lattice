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
