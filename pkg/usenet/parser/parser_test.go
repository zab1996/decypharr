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
