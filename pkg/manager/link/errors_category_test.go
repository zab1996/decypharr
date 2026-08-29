package link

import "testing"

// Transient provider codes (429/503/500/502/504/unrecognised) must not be
// categorized Permanent. In this fork, fetchAndValidate's cache-skip guard
// (isRetryableValidationError, in service.go) and downloader.go's rate-limit
// backoff loop both act on Retryable via ShouldRetry() — a Permanent error is
// acted on by neither, so it gets memoised in s.validated and the file stays
// unreadable until the process restarts.
//
// The second half guards against over-correcting: genuinely permanent codes
// must stay permanent, otherwise the caller would retry forever on a dead file.
func TestTransientCodesAreNotPermanent(t *testing.T) {
	transient := []string{"429", "500", "502", "503", "504", "some_unrecognised_code"}
	for _, code := range transient {
		e := ErrorCodeToLinkError(code)
		if e.IsPermanent() {
			t.Errorf("code %q classified as permanent: the file would stay unreadable until restart", code)
		}
		if !e.ShouldRetry() {
			t.Errorf("code %q: ShouldRetry() is false, so downloader.go's backoff loop and the link cache-skip guard never act on it", code)
		}
	}

	permanent := []string{"401", "unauthorized", "404", "link_not_found", "file_not_available"}
	for _, code := range permanent {
		e := ErrorCodeToLinkError(code)
		if !e.IsPermanent() {
			t.Errorf("code %q should remain permanent", code)
		}
		if e.ShouldRetry() {
			t.Errorf("code %q should not trigger a retry", code)
		}
	}
}
