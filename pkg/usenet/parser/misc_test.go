package parser

import (
	"testing"

	nzbparser "github.com/Tensai75/nzbparser"
)

func testFileGroup(segCount int) *FileGroup {
	segs := make(nzbparser.NzbSegments, segCount)
	for i := range segs {
		segs[i] = nzbparser.NzbSegment{Number: i + 1, Id: "id", Bytes: 1000}
	}
	file := nzbparser.NzbFile{Segments: segs}
	return &FileGroup{
		BaseName: "test",
		Files:    []nzbparser.NzbFile{file},
	}
}

func TestGetNZBSegmentsContiguousRange(t *testing.T) {
	group := testFileGroup(5)

	_, segs := getNZBSegments(0, group.Files[0], group)
	if segs == nil {
		t.Fatal("expected segments for a contiguous 1..5 range")
	}
	if len(segs) != 5 {
		t.Fatalf("len(segs) = %d, want 5", len(segs))
	}
	for i, s := range segs {
		if s.MessageID == "" {
			t.Errorf("segment %d has empty MessageID", i)
		}
	}
}

func TestGetNZBSegmentsRejectsHole(t *testing.T) {
	// Numbers 1,2,4,5 — a hole at 3. Count (4) matches neither the true
	// range width (5) so the range check must reject it outright, instead
	// of the old behavior of zero-filling slot 3 with an empty MessageID.
	file := nzbparser.NzbFile{
		Segments: nzbparser.NzbSegments{
			{Number: 1, Id: "a", Bytes: 1000},
			{Number: 2, Id: "b", Bytes: 1000},
			{Number: 4, Id: "c", Bytes: 1000},
			{Number: 5, Id: "d", Bytes: 1000},
		},
	}
	group := &FileGroup{BaseName: "test", Files: []nzbparser.NzbFile{file}}

	_, segs := getNZBSegments(0, file, group)
	if segs != nil {
		t.Fatalf("expected nil segments for a range with a hole, got %d segments", len(segs))
	}
}

func TestGetNZBSegmentsRejectsDuplicateNumber(t *testing.T) {
	// Numbers 1,2,2,4 — count matches the apparent range width (1..4 = 4)
	// so the cheap range check alone can't catch this; the per-slot
	// occupancy check must.
	file := nzbparser.NzbFile{
		Segments: nzbparser.NzbSegments{
			{Number: 1, Id: "a", Bytes: 1000},
			{Number: 2, Id: "b", Bytes: 1000},
			{Number: 2, Id: "c", Bytes: 1000},
			{Number: 4, Id: "d", Bytes: 1000},
		},
	}
	group := &FileGroup{BaseName: "test", Files: []nzbparser.NzbFile{file}}

	_, segs := getNZBSegments(0, file, group)
	if segs != nil {
		t.Fatalf("expected nil segments for a duplicate segment number, got %d segments", len(segs))
	}
}

func TestGetNZBSegmentsRejectsEmptyMessageID(t *testing.T) {
	file := nzbparser.NzbFile{
		Segments: nzbparser.NzbSegments{
			{Number: 1, Id: "a", Bytes: 1000},
			{Number: 2, Id: "", Bytes: 1000},
		},
	}
	group := &FileGroup{BaseName: "test", Files: []nzbparser.NzbFile{file}}

	_, segs := getNZBSegments(0, file, group)
	if segs != nil {
		t.Fatalf("expected nil segments when a segment has no message id, got %d segments", len(segs))
	}
}

func TestGetNZBSegmentsAcceptsZeroIndexedRange(t *testing.T) {
	// Some posters number segments from 0 instead of 1.
	file := nzbparser.NzbFile{
		Segments: nzbparser.NzbSegments{
			{Number: 0, Id: "a", Bytes: 1000},
			{Number: 1, Id: "b", Bytes: 1000},
			{Number: 2, Id: "c", Bytes: 1000},
		},
	}
	group := &FileGroup{BaseName: "test", Files: []nzbparser.NzbFile{file}}

	_, segs := getNZBSegments(0, file, group)
	if len(segs) != 3 {
		t.Fatalf("expected 3 segments for a 0-indexed contiguous range, got %d", len(segs))
	}
}
