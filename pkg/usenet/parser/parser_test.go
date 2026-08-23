package parser

import (
	"testing"

	nzbparser "github.com/Tensai75/nzbparser"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

func TestGroupProcessedFiles_PreservesFilenameWhenActualFilenameEmpty(t *testing.T) {
	p := &NZBParser{}

	items := []contentResult{
		{
			file: nzbparser.NzbFile{
				Filename:     "subject-derived-name.mkv",
				Basefilename: "subject-derived-name",
			},
			fileType:       storage.NZBFileTypeMedia,
			actualFilename: "", // yEnc header carried no name
		},
	}

	groups := p.groupProcessedFiles(items)

	found := false
	for _, group := range groups {
		for _, f := range group.Files {
			if f.Filename == "subject-derived-name.mkv" {
				found = true
			}
			if f.Filename == "" {
				t.Fatalf("filename was blanked out despite an empty actualFilename: %+v", f)
			}
		}
	}
	if !found {
		t.Fatal("expected the subject-derived filename to survive grouping")
	}
}

func TestGroupProcessedFiles_OverwritesFilenameWhenActualFilenamePresent(t *testing.T) {
	p := &NZBParser{}

	items := []contentResult{
		{
			file: nzbparser.NzbFile{
				Filename:     "subject-derived-name.mkv",
				Basefilename: "subject-derived-name",
			},
			fileType:       storage.NZBFileTypeMedia,
			actualFilename: "actual-content-name.mkv",
		},
	}

	groups := p.groupProcessedFiles(items)

	found := false
	for _, group := range groups {
		for _, f := range group.Files {
			if f.Filename == "actual-content-name.mkv" {
				found = true
			}
		}
	}
	if !found {
		t.Fatal("expected content-detected filename to overwrite the subject-derived one")
	}
}

// detectFileTypeFromContent's TS sync-byte check indexes data[188], which
// needs at least 189 bytes. An exactly-188-byte buffer with the first sync
// byte set must not panic, and correctly reports Unknown rather than Media
// since there aren't enough bytes to confirm the second sync byte.
func TestDetectFileTypeFromContent_ExactBoundaryDoesNotPanic(t *testing.T) {
	p := &NZBParser{}
	data := make([]byte, 188)
	data[0] = 0x47

	got := p.detectFileTypeFromContent(data)
	if got != storage.NZBFileTypeUnknown {
		t.Fatalf("detectFileTypeFromContent(188 bytes) = %v, want Unknown", got)
	}
}

// getNZBSegments rejects a file with a holed or duplicate segment range by
// returning (0, nil) rather than the pre-existing zero-filled-slot behavior.
// processMediaFile must in turn reject the whole file instead of splicing
// that hole into the merged output stream.
func TestProcessMediaFile_RejectsFileWithInvalidSegmentRange(t *testing.T) {
	p := &NZBParser{}

	group := &FileGroup{
		BaseName: "movie",
		Type:     storage.NZBFileTypeMedia,
		Groups:   map[string]struct{}{},
		Files: []nzbparser.NzbFile{
			{
				Filename: "movie.mkv",
				Number:   1,
				Segments: nzbparser.NzbSegments{
					{Number: 1, Id: "a", Bytes: 1000},
					{Number: 2, Id: "b", Bytes: 1000},
					{Number: 2, Id: "c", Bytes: 1000}, // duplicate
					{Number: 4, Id: "d", Bytes: 1000},
				},
			},
		},
	}

	got := p.processMediaFile(group, "")
	if got != nil {
		t.Fatalf("expected processMediaFile to reject a file with an invalid segment range, got %+v", got)
	}
}

func TestDetectFileTypeFromContent_TrueTSPacketDetected(t *testing.T) {
	p := &NZBParser{}
	data := make([]byte, 189)
	data[0] = 0x47
	data[188] = 0x47

	got := p.detectFileTypeFromContent(data)
	if got != storage.NZBFileTypeMedia {
		t.Fatalf("detectFileTypeFromContent(189 bytes, second sync byte present) = %v, want Media", got)
	}
}
