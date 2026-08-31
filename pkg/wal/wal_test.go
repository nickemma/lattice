package wal

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReplayAndRecovery(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data", "wal.log")
	log, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := log.Append(Record{Op: "put", Key: "a", Value: []byte("one")}); err != nil {
		t.Fatal(err)
	}
	if err := log.Append(Record{Op: "delete", Key: "b"}); err != nil {
		t.Fatal(err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	log, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	records, err := log.Replay()
	if err != nil || len(records) != 2 {
		t.Fatalf("replay: records=%d err=%v", len(records), err)
	}
	if string(records[0].Value) != "one" {
		t.Fatalf("value = %q", records[0].Value)
	}
	_ = log.Close()
}

func TestIgnoresInterruptedFinalRecord(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	log, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := log.Append(Record{Op: "put", Key: "a", Value: []byte("one")}); err != nil {
		t.Fatal(err)
	}
	_ = log.Close()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.Write([]byte{0x4c, 0x41, 0x54})
	_ = f.Close()
	log, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	records, err := log.Replay()
	if err != nil || len(records) != 1 {
		t.Fatalf("replay interrupted record: records=%d err=%v", len(records), err)
	}
	_ = log.Close()
}
