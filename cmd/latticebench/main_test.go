package main

import (
	"math"
	"testing"
)

func TestPercentileInterpolates(t *testing.T) {
	values := []float64{10, 20, 30, 40}
	if got := percentile(values, 0.50); got != 25 {
		t.Fatalf("p50 = %v, want 25", got)
	}
	if got := percentile(values, 0.99); math.Abs(got-39.7) > 1e-9 {
		t.Fatalf("p99 = %v, want 39.7", got)
	}
}
