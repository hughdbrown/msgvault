package gmail

import "testing"

func TestIsRateLimitError(t *testing.T) {
	tests := []struct {
		name string
		body []byte
		want bool
	}{
		{
			name: "RateLimitExceeded",
			body: []byte(`{
				"error": {
					"code": 403,
					"message": "Quota exceeded for quota metric 'Queries'",
					"errors": [{"reason": "rateLimitExceeded"}]
				}
			}`),
			want: true,
		},
		{
			name: "RateLimitExceededUpperCase",
			body: []byte(`{
				"error": {
					"code": 403,
					"details": [{"reason": "RATE_LIMIT_EXCEEDED"}]
				}
			}`),
			want: true,
		},
		{
			name: "QuotaExceeded",
			body: []byte(`{
				"error": {
					"code": 403,
					"message": "Quota exceeded for quota metric 'Queries'"
				}
			}`),
			want: true,
		},
		{
			name: "UserRateLimitExceeded",
			body: []byte(`{
				"error": {
					"code": 403,
					"errors": [{"reason": "userRateLimitExceeded"}]
				}
			}`),
			want: true,
		},
		{
			name: "PermissionDenied",
			body: []byte(`{
				"error": {
					"code": 403,
					"message": "The caller does not have permission",
					"errors": [{"reason": "forbidden"}]
				}
			}`),
			want: false,
		},
		{
			name: "EmptyBody",
			body: []byte{},
			want: false,
		},
		{
			name: "InvalidJSON",
			body: []byte("not valid json but contains rateLimitExceeded"),
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isRateLimitError(tt.body); got != tt.want {
				t.Errorf("isRateLimitError() = %v, want %v", got, tt.want)
			}
		})
	}
}
