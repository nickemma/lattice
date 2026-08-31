package lsm

import "testing"

func TestMVCCSnapshotRead(t *testing.T) {
	m := NewMVCC()
	first := m.Begin()
	first.Put("k", []byte("old"))
	first.Commit()
	reader := m.Begin()
	writer := m.Begin()
	writer.Put("k", []byte("new"))
	writer.Commit()
	if value, ok := reader.Get("k"); !ok || string(value) != "old" {
		t.Fatalf("snapshot read = %q, %v", value, ok)
	}
	latest := m.Begin()
	if value, ok := latest.Get("k"); !ok || string(value) != "new" {
		t.Fatalf("latest read = %q, %v", value, ok)
	}
}
