package replication

import "testing"

func TestPartitionCanCreateDivergentReplicas(t *testing.T) {
	c := NewCluster(1, 2, 3)
	if err := c.Write(1, "account", "alice-paid"); err != nil {
		t.Fatal(err)
	}
	if err := c.Write(2, "account", "bob-paid"); err != nil {
		t.Fatal(err)
	}
	a, _, _ := c.Read(1, "account")
	b, _, _ := c.Read(2, "account")
	if a == b {
		t.Fatalf("unsafe replica unexpectedly agreed: %q", a)
	}
}

func TestSyncRepairsStaleReplica(t *testing.T) {
	c := NewCluster(1, 2)
	_ = c.Write(1, "k", "v")
	if err := c.Sync(1, 2); err != nil {
		t.Fatal(err)
	}
	v, ok, err := c.Read(2, "k")
	if err != nil || !ok || v != "v" {
		t.Fatalf("synced value = %q, %v, %v", v, ok, err)
	}
}
