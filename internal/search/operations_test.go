package search

import "testing"

func TestSnapshotRestoreAndReindex(t *testing.T) {
	dir := t.TempDir()
	index, err := OpenPersistentIndex(dir, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := index.Upsert(Document{ID: "one", Title: "One", Body: "first"}); err != nil {
		t.Fatal(err)
	}
	path := dir + "/snapshot.json"
	if err := index.Snapshot(path); err != nil {
		t.Fatal(err)
	}
	if err := index.Delete("one"); err != nil {
		t.Fatal(err)
	}
	if count, err := index.Restore(path); err != nil || count != 1 || index.Count() != 1 {
		t.Fatalf("restore count=%d err=%v current=%d", count, err, index.Count())
	}
	if err := index.Reindex(); err != nil {
		t.Fatal(err)
	}
	_ = index.Close()
}
