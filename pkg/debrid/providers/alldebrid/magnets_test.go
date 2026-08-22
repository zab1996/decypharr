package alldebrid

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	json "github.com/bytedance/sonic"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/customerror"
	"github.com/sirrobot01/decypharr/internal/request"
)

// config.Get() is reached through flattenFiles (called when a torrent has
// downloaded status) and calls os.Exit(1) when it cannot write its file, so
// point it at a scratch directory before any test runs. os.Exit skips
// deferred calls, hence the explicit cleanup around m.Run().
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "decypharr-alldebrid-test")
	if err != nil {
		panic(err)
	}
	config.SetConfigPath(dir)
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

// AllDebrid's /magnet/status can shape the "magnets" field as an array, a
// map keyed by index, or (for a single-torrent status call) a bare object.
// Magnets.UnmarshalJSON must accept all three.
func TestMagnetsUnmarshalJSON(t *testing.T) {
	tests := map[string]struct {
		input string
		want  []int
	}{
		"array": {
			input: `[{"id":1},{"id":2}]`,
			want:  []int{1, 2},
		},
		"map": {
			input: `{"first":{"id":1}}`,
			want:  []int{1},
		},
		"single object": {
			input: `{"id":1,"filename":"Release.mkv"}`,
			want:  []int{1},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			var magnets Magnets
			if err := json.Unmarshal([]byte(tt.input), &magnets); err != nil {
				t.Fatalf("Unmarshal() error = %v", err)
			}
			if len(magnets) != len(tt.want) {
				t.Fatalf("Unmarshal() returned %d magnets, want %d", len(magnets), len(tt.want))
			}
			for i, want := range tt.want {
				if magnets[i].Id != want {
					t.Errorf("magnets[%d].Id = %d, want %d", i, magnets[i].Id, want)
				}
			}
		})
	}
}

// A single-torrent status call can come back with more than one magnet in
// the response (e.g. an account-wide array); GetTorrent must pick the one
// that was actually requested rather than assuming it's alone or first.
func TestGetTorrentSelectsRequestedMagnetFromArray(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("id"); got != "2" {
			t.Errorf("id = %q, want 2", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"status":"success","data":{"magnets":[{"id":1,"filename":"Wrong.mkv","statusCode":1},{"id":2,"filename":"Release.mkv","statusCode":1,"hash":"ABC"}]}}`)
	}))
	t.Cleanup(server.Close)

	ad := &AllDebrid{
		Host:   server.URL,
		client: request.New(request.WithMaxRetries(0)),
	}
	torrent, err := ad.GetTorrent("2")
	if err != nil {
		t.Fatalf("GetTorrent() error = %v", err)
	}
	if torrent.Id != "2" || torrent.Name != "Release.mkv" || torrent.InfoHash != "ABC" {
		t.Fatalf("GetTorrent() = %#v, want requested magnet 2", torrent)
	}
}

func TestFindMagnetReturnsNotFound(t *testing.T) {
	_, err := findMagnet(Magnets{{Id: 1}}, "2")
	if !errors.Is(err, customerror.TorrentNotFoundError) {
		t.Fatalf("findMagnet() error = %v, want TorrentNotFoundError", err)
	}
}
