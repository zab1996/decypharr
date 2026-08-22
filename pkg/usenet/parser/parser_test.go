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
