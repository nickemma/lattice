package bench

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/nickemma/lattice/pkg/raft"
)

func TestElectionDistribution(t *testing.T) {
	type observation struct {
		run, ticks, leader int
	}
	observations := make([]observation, 0, 50)
	for run := 0; run < 50; run++ {
		cluster := raft.NewCluster(1, 2, 3)
		if err := cluster.ElectLeader(1); err != nil {
			t.Fatalf("run %d initial election: %v", run, err)
		}
		if _, err := cluster.Propose(raft.Command{Key: "before", Value: "ok"}); err != nil {
			t.Fatalf("run %d proposal: %v", run, err)
		}
		if err := cluster.Kill(1); err != nil {
			t.Fatalf("run %d kill: %v", run, err)
		}
		ticks := 0
		for ; ticks < 20; ticks++ {
			cluster.Tick()
			if leader, ok := cluster.Leader(); ok && leader != 1 {
				observations = append(observations, observation{run: run, ticks: ticks + 1, leader: leader})
				break
			}
		}
		if len(observations) != run+1 {
			t.Fatalf("run %d did not elect a replacement within 20 ticks", run)
		}
	}

	distribution := make(map[int]int)
	for _, item := range observations {
		distribution[item.ticks]++
	}
	ticks := make([]int, 0, len(distribution))
	for tick := range distribution {
		ticks = append(ticks, tick)
	}
	sort.Ints(ticks)
	var summary strings.Builder
	for _, tick := range ticks {
		fmt.Fprintf(&summary, "%d:%d ", tick, distribution[tick])
	}
	t.Logf("replacement election ticks across 50 leader kills: %s", strings.TrimSpace(summary.String()))

	if path := os.Getenv("LATTICE_ELECTION_OUT"); path != "" {
		var csv strings.Builder
		csv.WriteString("run,election_ticks,new_leader\n")
		for _, item := range observations {
			fmt.Fprintf(&csv, "%d,%d,%d\n", item.run, item.ticks, item.leader)
		}
		if err := os.WriteFile(path, []byte(csv.String()), 0o644); err != nil {
			t.Fatalf("write election distribution: %v", err)
		}
	}
}
