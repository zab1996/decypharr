package parser

import (
	"testing"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// buildExtractedArchiveFiles must classify each extracted file by its own
// content (media vs. not), not by the archive format it came from. Before this
// fix every file extracted from a RAR/7z/ZIP archive kept the archive's own
// FileType (Rar/SevenZip/Zip) permanently, which made renameMediaFiles's
// FileType==Media filter always skip it — the raw scene filename inside a
// correctly-named cli_mount folder never got renamed. This reproduces the
// production symptom directly at the unit responsible for it.
func TestBuildExtractedArchiveFilesClassifiesByContent(t *testing.T) {
	group := &FileGroup{BaseName: "release"}
	baseSegments := []storage.NZBSegment{{Number: 1, Bytes: 100}}

	cases := []struct {
		name     string
		fileName string
		want     storage.NZBFileType
	}{
		{"media file inside RAR", "Show.S01E01.1080p.WEB.h264-GROUP.mkv", storage.NZBFileTypeMedia},
		{"media file inside ZIP", "movie.mp4", storage.NZBFileTypeMedia},
		{"non-media file keeps archive type", "release.nfo", storage.NZBFileTypeZip},
		{"sfv file keeps archive type", "checksums.sfv", storage.NZBFileTypeZip},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			infos := []*storage.ExtractedFileInfo{
				{FileName: tc.fileName, FileSize: 1000, IsStored: true},
			}
			files := buildExtractedArchiveFiles(group, "", storage.NZBFileTypeZip, baseSegments, nil, infos)
			if len(files) != 1 {
				t.Fatalf("expected 1 file, got %d", len(files))
			}
			if files[0].FileType != tc.want {
				t.Fatalf("FileType = %v, want %v", files[0].FileType, tc.want)
			}
		})
	}
}

// End-to-end: a media file that was only reachable via an archive extraction
// (FileType now Media, per the fix above) must actually be visible to
// renameMediaFiles's single-file rename path, matching the production case
// where the NZB entry name (set by cli_debrid) should replace the raw scene
// filename.
func TestRenameMediaFilesRenamesArchiveExtractedSingleFile(t *testing.T) {
	files := []storage.NZBFile{
		{Name: "Batman.Caped.Crusader.S02E07.1080p.HEVC.x265-MeGusta.mkv", FileType: storage.NZBFileTypeMedia},
	}
	nzbName := "Batman Caped Crusader (2024) - S02E07 - TBD - {imdb-tt14681596} - Default - (1080p.HEVC.x265-MeGusta)"

	renameMediaFiles(files, config.DeobfuscateModeIndex, nzbName, zerolog.Nop())

	want := nzbName + ".mkv"
	if files[0].Name != want {
		t.Fatalf("Name = %q, want %q (archive-extracted single media file must be renamed to the NZB entry name)", files[0].Name, want)
	}
}

// Regression guard for the old behavior: before the fix, a single archive-
// extracted media file was tagged NZBFileTypeRar/Zip/SevenZip, so
// renameMediaFiles's mediaFiles filter never selected it and it kept its raw
// scene name. Confirm that a non-Media FileType is indeed excluded from
// renaming — this documents why the classification fix (not a change to
// renameMediaFiles itself) was the correct place to fix the bug.
func TestRenameMediaFilesSkipsNonMediaFileType(t *testing.T) {
	rawName := "Batman.Caped.Crusader.S02E07.1080p.HEVC.x265-MeGusta.mkv"
	files := []storage.NZBFile{
		{Name: rawName, FileType: storage.NZBFileTypeRar},
	}
	nzbName := "Batman Caped Crusader (2024) - S02E07 - TBD - {imdb-tt14681596} - Default - (1080p.HEVC.x265-MeGusta)"

	renameMediaFiles(files, config.DeobfuscateModeIndex, nzbName, zerolog.Nop())

	if files[0].Name != rawName {
		t.Fatalf("Name changed to %q; expected it to stay %q since FileType != Media is never a rename candidate", files[0].Name, rawName)
	}
}
