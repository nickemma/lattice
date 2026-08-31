package redis

import "testing"

func TestNewRejectsInvalidURL(t *testing.T) {
	if _, err := New("not-a-redis-url", 0); err == nil {
		t.Fatal("expected invalid redis URL error")
	}
}
