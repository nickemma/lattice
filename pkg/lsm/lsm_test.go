package lsm

import (
	"testing"
)

func TestPersistsFlushesAndRecovers(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Put("user:1", []byte("alice")); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("user:2", []byte("bob")); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete("user:2"); err != nil {
		t.Fatal(err)
	}
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	v, err := s.Get("user:1")
	if err != nil || string(v) != "alice" {
		t.Fatalf("get user:1 = %q, %v", v, err)
	}
	if _, err := s.Get("user:2"); err != ErrNotFound {
		t.Fatalf("deleted key error = %v", err)
	}
	_ = s.Close()
}

func TestCompaction(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	_ = s.Put("a", []byte("1"))
	_ = s.Flush()
	_ = s.Put("a", []byte("2"))
	_ = s.Put("b", []byte("3"))
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	v, err := s.Get("a")
	if err != nil || string(v) != "2" {
		t.Fatalf("compacted value = %q, %v", v, err)
	}
	_ = s.Close()
}
