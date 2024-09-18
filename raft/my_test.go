package raft

import (
	"fmt"
	"testing"
)

func TestCut(t *testing.T) {
	samples := []struct {
		array    []uint64
		pivot    int
		newindex int
	}{
		{[]uint64{9, 77, 70, 1013, 1}, 4, 0},
		{[]uint64{9, 77, 70, 1013, 1}, 3, 4},
		{[]uint64{9, 77, 70, 1013, 1}, 2, 2},
		{[]uint64{9999, 77, 70, 1013, 1}, 0, 4},
		{[]uint64{}, 0, 0},
		{[]uint64{1}, 0, 0},
		{[]uint64{1, 2}, 0, 0},
	}
	for id, sample := range samples {
		res := cut(sample.array, 0, len(sample.array), sample.pivot)
		if res != sample.newindex {
			t.Errorf("#%d, want ans = %d, got = %d", id, sample.newindex, res)
		}
	}
}

func TestFindKthNum(t *testing.T) {
	samples := []struct {
		array []uint64
		k     int
		ans   uint64
	}{
		{[]uint64{9, 77, 70, 1013, 1}, 5, 1013},
		{[]uint64{9, 77, 70, 1013, 1}, 4, 77},
		{[]uint64{9, 77, 70, 1013, 1}, 3, 70},
		{[]uint64{9999, 77, 70, 1013, 1}, 5, 9999},
		{[]uint64{}, 0, 0},
		{[]uint64{1}, 1, 1},
		{[]uint64{1, 2}, 1, 1},
	}
	for id, sample := range samples {
		res := findKthNum(sample.array, 0, len(sample.array), sample.k)
		if res != sample.ans {
			t.Errorf("#%d, want ans = %d, got = %d", id, sample.ans, res)
		}
		fmt.Println("")
	}
}
