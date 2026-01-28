package gmail

import "testing"

func TestIsRateLimitError_RateLimitExceeded(t *testing.T) {
	body := []byte(`{
		"error": {
			"code": 403,
			"message": "Quota exceeded for quota metric 'Queries'",
			"errors": [{"reason": "rateLimitExceeded"}]
		}
	}`)

	if !isRateLimitError(body) {
		t.Error("expected rateLimitExceeded to be detected as rate limit error")
	}
}

func TestIsRateLimitError_RateLimitExceededUpperCase(t *testing.T) {
	body := []byte(`{
		"error": {
			"code": 403,
			"details": [{"reason": "RATE_LIMIT_EXCEEDED"}]
		}
	}`)

	if !isRateLimitError(body) {
		t.Error("expected RATE_LIMIT_EXCEEDED to be detected as rate limit error")
	}
}

func TestIsRateLimitError_QuotaExceeded(t *testing.T) {
	body := []byte(`{
		"error": {
			"code": 403,
			"message": "Quota exceeded for quota metric 'Queries'"
		}
	}`)

	if !isRateLimitError(body) {
		t.Error("expected 'Quota exceeded' to be detected as rate limit error")
	}
}

func TestIsRateLimitError_UserRateLimitExceeded(t *testing.T) {
	body := []byte(`{
		"error": {
			"code": 403,
			"errors": [{"reason": "userRateLimitExceeded"}]
		}
	}`)

	if !isRateLimitError(body) {
		t.Error("expected userRateLimitExceeded to be detected as rate limit error")
	}
}

func TestIsRateLimitError_PermissionDenied(t *testing.T) {
	// A real permission error should NOT be detected as rate limit
	body := []byte(`{
		"error": {
			"code": 403,
			"message": "The caller does not have permission",
			"errors": [{"reason": "forbidden"}]
		}
	}`)

	if isRateLimitError(body) {
		t.Error("expected permission denied (forbidden) to NOT be detected as rate limit error")
	}
}

func TestIsRateLimitError_EmptyBody(t *testing.T) {
	if isRateLimitError([]byte{}) {
		t.Error("expected empty body to NOT be detected as rate limit error")
	}
}

func TestIsRateLimitError_InvalidJSON(t *testing.T) {
	// The function does byte matching, not JSON parsing, so invalid JSON is fine
	body := []byte("not valid json but contains rateLimitExceeded")

	if !isRateLimitError(body) {
		t.Error("expected rateLimitExceeded substring to be detected even in invalid JSON")
	}
}
