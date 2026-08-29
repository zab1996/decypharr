package buffer

import (
	"reflect"
	"testing"
)

func TestBlockAddDirty(t *testing.T) {
	tests := []struct {
		name string
		adds [][2]int // [lo, hi) pairs applied in order
		want []dirtyExtent
	}{
		{
			name: "single write",
			adds: [][2]int{{10, 20}},
			want: []dirtyExtent{{10, 20}},
		},
		{
			name: "disjoint stays sorted regardless of insertion order",
			adds: [][2]int{{100, 200}, {0, 10}},
			want: []dirtyExtent{{0, 10}, {100, 200}},
		},
		{
			name: "adjacent ranges merge (hi == lo)",
			adds: [][2]int{{0, 10}, {10, 20}},
			want: []dirtyExtent{{0, 20}},
		},
		{
			name: "overlapping ranges merge",
			adds: [][2]int{{0, 15}, {10, 20}},
			want: []dirtyExtent{{0, 20}},
		},
		{
			name: "a write bridging two extents swallows both",
			adds: [][2]int{{0, 5}, {20, 25}, {3, 22}},
			want: []dirtyExtent{{0, 25}},
		},
		{
			name: "zero-length write is a no-op",
			adds: [][2]int{{10, 20}, {5, 5}},
			want: []dirtyExtent{{10, 20}},
		},
		{
			name: "inverted range is a no-op",
			adds: [][2]int{{10, 20}, {15, 12}},
			want: []dirtyExtent{{10, 20}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			blk := &block{}
			blk.initDirty()
			for _, a := range tt.adds {
				blk.addDirty(a[0], a[1])
			}
			if !reflect.DeepEqual(blk.dirty, tt.want) {
				t.Fatalf("dirty = %v, want %v", blk.dirty, tt.want)
			}
		})
	}
}

func TestBlockRemoveDirty(t *testing.T) {
	tests := []struct {
		name     string
		initial  []dirtyExtent
		removeLo int
		removeHi int
		want     []dirtyExtent
	}{
		{
			name:     "exact removal clears the extent",
			initial:  []dirtyExtent{{10, 20}},
			removeLo: 10, removeHi: 20,
			want: []dirtyExtent{},
		},
		{
			name:     "removal covering more than the extent clears it",
			initial:  []dirtyExtent{{10, 20}},
			removeLo: 0, removeHi: 30,
			want: []dirtyExtent{},
		},
		{
			name:     "trim from the front",
			initial:  []dirtyExtent{{10, 20}},
			removeLo: 0, removeHi: 15,
			want: []dirtyExtent{{15, 20}},
		},
		{
			name:     "trim from the back",
			initial:  []dirtyExtent{{10, 20}},
			removeLo: 15, removeHi: 30,
			want: []dirtyExtent{{10, 15}},
		},
		{
			name:     "removal in the middle splits the extent",
			initial:  []dirtyExtent{{10, 20}},
			removeLo: 13, removeHi: 17,
			want: []dirtyExtent{{10, 13}, {17, 20}},
		},
		{
			name:     "removal spanning multiple extents clears the covered ones",
			initial:  []dirtyExtent{{0, 5}, {10, 15}, {20, 25}},
			removeLo: 5, removeHi: 20,
			want: []dirtyExtent{{0, 5}, {20, 25}},
		},
		{
			name:     "removal outside any extent is a no-op",
			initial:  []dirtyExtent{{10, 20}},
			removeLo: 30, removeHi: 40,
			want: []dirtyExtent{{10, 20}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			blk := &block{}
			blk.initDirty()
			blk.dirty = append(blk.dirty, tt.initial...)
			blk.removeDirty(tt.removeLo, tt.removeHi)
			if !reflect.DeepEqual(blk.dirty, tt.want) {
				t.Fatalf("dirty = %v, want %v", blk.dirty, tt.want)
			}
		})
	}
}

func TestBlockIsCleanAndClearDirty(t *testing.T) {
	blk := &block{}
	blk.initDirty()
	if !blk.isClean() {
		t.Fatal("a freshly initialized block should be clean")
	}
	blk.addDirty(0, 10)
	if blk.isClean() {
		t.Fatal("a block with a dirty extent should not be clean")
	}
	blk.clearDirty()
	if !blk.isClean() {
		t.Fatal("clearDirty should leave the block clean")
	}
	// clearDirty must retain the underlying storage so a fresh addDirty
	// after clearing doesn't need to grow the inline array.
	blk.addDirty(0, 5)
	if len(blk.dirty) != 1 || blk.dirty[0] != (dirtyExtent{0, 5}) {
		t.Fatalf("dirty after clear+add = %v, want [{0 5}]", blk.dirty)
	}
}
