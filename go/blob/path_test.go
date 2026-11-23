package blob

import "testing"

func TestValidateBucketPath(t *testing.T) {
	for _, tt := range []struct {
		name      string
		prefix    string
		wantError bool
	}{
		{"empty is allowed", "", false},
		{"single segment", "staging", false},
		{"multi segment prefix", "estuary/tenant/staging", false},
		{"trailing slash is allowed", "estuary/staging/", false},
		{"leading slash rejected", "/estuary/staging", true},
		{"full s3 uri rejected", "s3://my-bucket/estuary/staging", true},
		{"full gcs uri rejected", "gs://my-bucket/estuary/staging", true},
		{"bare scheme rejected", "s3://", true},
		{"https uri rejected", "https://example.com/prefix", true},
		{"embedded scheme rejected", "estuary/s3://nested", true},
		{"current directory segment is allowed", "./estuary/staging", false},
		{"parent segment within the bucket is allowed", "estuary/../staging", false},
		{"resolving to the bucket root is allowed", "estuary/..", false},
		{"name starting with dots is allowed", "..estuary/staging", false},
		{"parent of the bucket root rejected", "..", true},
		{"prefix above the bucket root rejected", "../estuary", true},
		{"prefix resolving above the bucket root rejected", "estuary/../../staging", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateBucketPath(tt.prefix)
			if tt.wantError && err == nil {
				t.Fatalf("expected an error for %q, got nil", tt.prefix)
			}
			if !tt.wantError && err != nil {
				t.Fatalf("expected no error for %q, got %v", tt.prefix, err)
			}
		})
	}
}
