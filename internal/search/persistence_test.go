package search

import "testing"

func TestPersistentIndexRecoversDocuments(t *testing.T) {
	dir := t.TempDir()
	i, err := OpenPersistentIndex(dir, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := i.Upsert(Document{ID: "persisted", Title: "durable", Body: "wal recovery"}); err != nil {
		t.Fatal(err)
	}
	if err := i.Close(); err != nil {
		t.Fatal(err)
	}
	i, err = OpenPersistentIndex(dir, 2)
	if err != nil {
		t.Fatal(err)
	}
	if i.Count() != 1 {
		t.Fatalf("recovered count = %d", i.Count())
	}
	if err := i.Close(); err != nil {
		t.Fatal(err)
	}
}
