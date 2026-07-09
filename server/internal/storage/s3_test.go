package storage

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestS3StorageKeyFromURL_CustomEndpointPreservesNestedKey(t *testing.T) {
	s := &S3Storage{
		bucket:      "test-bucket",
		endpointURL: "http://localhost:9000",
	}

	rawURL := "http://localhost:9000/test-bucket/uploads/abc/file.png"

	if got := s.KeyFromURL(rawURL); got != "uploads/abc/file.png" {
		t.Fatalf("KeyFromURL(%q) = %q, want %q", rawURL, got, "uploads/abc/file.png")
	}
}

func TestS3StoragePresignGet(t *testing.T) {
	store := &S3Storage{
		client: s3.New(s3.Options{
			Region:      "us-east-1",
			Credentials: aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider("AKID", "SECRET", "")),
		}),
		bucket: "test-bucket",
	}

	got, err := store.PresignGet(context.Background(), "uploads/abc/file.txt", 5*time.Minute)
	if err != nil {
		t.Fatalf("PresignGet: %v", err)
	}
	for _, want := range []string{
		"https://test-bucket.s3.us-east-1.amazonaws.com/uploads/abc/file.txt",
		"X-Amz-Signature=",
		"X-Amz-Expires=300",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("presigned URL %q does not contain %q", got, want)
		}
	}
}

func TestS3StoragePresignGetWithContentDisposition(t *testing.T) {
	store := &S3Storage{
		client: s3.New(s3.Options{
			Region:      "us-east-1",
			Credentials: aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider("AKID", "SECRET", "")),
		}),
		bucket: "test-bucket",
	}

	got, err := store.PresignGetWithContentDisposition(
		context.Background(),
		"uploads/abc/file.txt",
		5*time.Minute,
		`attachment; filename="report.txt"`,
	)
	if err != nil {
		t.Fatalf("PresignGetWithContentDisposition: %v", err)
	}
	u, err := url.Parse(got)
	if err != nil {
		t.Fatalf("parse presigned URL: %v", err)
	}
	if got := u.Query().Get("response-content-disposition"); got != `attachment; filename="report.txt"` {
		t.Fatalf("response-content-disposition = %q", got)
	}
	if sig := u.Query().Get("X-Amz-Signature"); sig == "" {
		t.Fatalf("missing X-Amz-Signature in %q", got)
	}
}

func TestS3StorageKeyFromURL_CustomEndpointWithTrailingSlash(t *testing.T) {
	s := &S3Storage{
		bucket:      "test-bucket",
		endpointURL: "http://localhost:9000/",
	}

	rawURL := "http://localhost:9000/test-bucket/uploads/abc/file.png"

	if got := s.KeyFromURL(rawURL); got != "uploads/abc/file.png" {
		t.Fatalf("KeyFromURL(%q) = %q, want %q", rawURL, got, "uploads/abc/file.png")
	}
}

func TestS3StorageKeyFromURL_CustomEndpointVirtualHostedStyle(t *testing.T) {
	// Tencent COS (and some other S3-compatible backends) reject path-style
	// requests with PathStyleDomainForbidden and require the bucket in the
	// hostname instead.
	s := &S3Storage{
		bucket:             "test-bucket",
		endpointURL:        "https://cos.ap-guangzhou.myqcloud.com",
		virtualHostedStyle: true,
	}

	rawURL := "https://test-bucket.cos.ap-guangzhou.myqcloud.com/uploads/abc/file.png"

	if got := s.KeyFromURL(rawURL); got != "uploads/abc/file.png" {
		t.Fatalf("KeyFromURL(%q) = %q, want %q", rawURL, got, "uploads/abc/file.png")
	}
}

func TestS3StorageKeyFromURL_CustomEndpointVirtualHostedStyle_StillParsesLegacyPathStyleURLs(t *testing.T) {
	// If an operator flips S3_FORCE_PATH_STYLE after objects were already
	// uploaded in path-style, old stored URLs must keep resolving.
	s := &S3Storage{
		bucket:             "test-bucket",
		endpointURL:        "https://cos.ap-guangzhou.myqcloud.com",
		virtualHostedStyle: true,
	}

	rawURL := "https://cos.ap-guangzhou.myqcloud.com/test-bucket/uploads/abc/file.png"

	if got := s.KeyFromURL(rawURL); got != "uploads/abc/file.png" {
		t.Fatalf("KeyFromURL(%q) = %q, want %q", rawURL, got, "uploads/abc/file.png")
	}
}

func TestS3StorageKeyFromURL_VirtualHostedStylePreservesNestedKey(t *testing.T) {
	s := &S3Storage{
		bucket: "test-bucket",
		region: "us-east-1",
	}

	rawURL := "https://test-bucket.s3.us-east-1.amazonaws.com/uploads/abc/file.png"

	if got := s.KeyFromURL(rawURL); got != "uploads/abc/file.png" {
		t.Fatalf("KeyFromURL(%q) = %q, want %q", rawURL, got, "uploads/abc/file.png")
	}
}

func TestS3StorageKeyFromURL_PathStylePreservesNestedKey(t *testing.T) {
	s := &S3Storage{
		bucket: "bucket.with.dots",
		region: "us-east-1",
	}

	rawURL := "https://s3.us-east-1.amazonaws.com/bucket.with.dots/uploads/abc/file.png"

	if got := s.KeyFromURL(rawURL); got != "uploads/abc/file.png" {
		t.Fatalf("KeyFromURL(%q) = %q, want %q", rawURL, got, "uploads/abc/file.png")
	}
}

func TestS3StorageKeyFromURL_LegacyBucketOnlyHostStillRoundTrips(t *testing.T) {
	// Old records written before the suffix bug was fixed look like
	// "https://<bucket>/<key>". They were broken at fetch time but were still
	// stored, so KeyFromURL must continue to recognise that prefix when we
	// migrate or delete those records.
	s := &S3Storage{
		bucket: "test-bucket",
		region: "us-east-1",
	}

	rawURL := "https://test-bucket/uploads/abc/file.png"

	if got := s.KeyFromURL(rawURL); got != "uploads/abc/file.png" {
		t.Fatalf("KeyFromURL(%q) = %q, want %q", rawURL, got, "uploads/abc/file.png")
	}
}

func TestS3StorageObjectKey(t *testing.T) {
	cases := []struct {
		name      string
		keyPrefix string
		key       string
		want      string
	}{
		{
			name: "no prefix configured leaves key unchanged",
			key:  "users/u1/file.png",
			want: "users/u1/file.png",
		},
		{
			name:      "prefix is prepended with a separating slash",
			keyPrefix: "multica-prod",
			key:       "users/u1/file.png",
			want:      "multica-prod/users/u1/file.png",
		},
		{
			name:      "workspace key gets prefixed the same way",
			keyPrefix: "multica-prod",
			key:       "workspaces/w1/file.png",
			want:      "multica-prod/workspaces/w1/file.png",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := &S3Storage{keyPrefix: tc.keyPrefix}
			if got := s.objectKey(tc.key); got != tc.want {
				t.Fatalf("objectKey(%q) = %q, want %q", tc.key, got, tc.want)
			}
		})
	}
}

func TestS3StorageKeyFromURL_StripsConfiguredPrefix(t *testing.T) {
	s := &S3Storage{
		bucket:    "test-bucket",
		region:    "us-east-1",
		keyPrefix: "multica-prod",
	}

	rawURL := "https://test-bucket.s3.us-east-1.amazonaws.com/multica-prod/uploads/abc/file.png"

	if got := s.KeyFromURL(rawURL); got != "uploads/abc/file.png" {
		t.Fatalf("KeyFromURL(%q) = %q, want %q", rawURL, got, "uploads/abc/file.png")
	}
}

func TestS3StorageKeyFromURL_NoPrefixConfiguredLeavesKeyIntact(t *testing.T) {
	// Guards against accidentally stripping a path segment that merely looks
	// like a prefix when S3_KEY_PREFIX is not configured.
	s := &S3Storage{
		bucket: "test-bucket",
		region: "us-east-1",
	}

	rawURL := "https://test-bucket.s3.us-east-1.amazonaws.com/multica-prod/uploads/abc/file.png"

	if got := s.KeyFromURL(rawURL); got != "multica-prod/uploads/abc/file.png" {
		t.Fatalf("KeyFromURL(%q) = %q, want %q", rawURL, got, "multica-prod/uploads/abc/file.png")
	}
}

func TestS3StorageKeyFromURL_PrefixConfiguredButURLKeyDoesNotMatch(t *testing.T) {
	// A URL for an object that was never written under the configured prefix
	// (e.g. legacy data uploaded before S3_KEY_PREFIX was set, or a foreign
	// key) must come back unchanged rather than being mangled.
	s := &S3Storage{
		bucket:    "test-bucket",
		region:    "us-east-1",
		keyPrefix: "multica-prod",
	}

	rawURL := "https://test-bucket.s3.us-east-1.amazonaws.com/uploads/abc/file.png"

	if got := s.KeyFromURL(rawURL); got != "uploads/abc/file.png" {
		t.Fatalf("KeyFromURL(%q) = %q, want %q", rawURL, got, "uploads/abc/file.png")
	}
}

func TestS3StorageUploadedURL_WithPrefix(t *testing.T) {
	// Locks the exact composition Upload performs: uploadedURL(objectKey(key)).
	const key = "uploads/abc/file.png"

	cases := []struct {
		name        string
		bucket      string
		region      string
		cdnDomain   string
		endpointURL string
		keyPrefix   string
		want        string
	}{
		{
			name:      "cdn domain includes prefix",
			bucket:    "test-bucket",
			region:    "us-east-1",
			cdnDomain: "cdn.example.com",
			keyPrefix: "multica-prod",
			want:      "https://cdn.example.com/multica-prod/uploads/abc/file.png",
		},
		{
			name:        "custom endpoint includes prefix",
			bucket:      "test-bucket",
			region:      "us-east-1",
			endpointURL: "http://localhost:9000",
			keyPrefix:   "multica-prod",
			want:        "http://localhost:9000/test-bucket/multica-prod/uploads/abc/file.png",
		},
		{
			name:      "default aws virtual hosted style includes prefix",
			bucket:    "test-bucket",
			region:    "us-east-1",
			keyPrefix: "multica-prod",
			want:      "https://test-bucket.s3.us-east-1.amazonaws.com/multica-prod/uploads/abc/file.png",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := &S3Storage{
				bucket:      tc.bucket,
				region:      tc.region,
				cdnDomain:   tc.cdnDomain,
				endpointURL: tc.endpointURL,
				keyPrefix:   tc.keyPrefix,
			}
			if got := s.uploadedURL(s.objectKey(key)); got != tc.want {
				t.Fatalf("uploadedURL(objectKey(%q)) = %q, want %q", key, got, tc.want)
			}
		})
	}
}

func TestParseForcePathStyle(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want bool
	}{
		{name: "unset defaults to true (path-style, matches pre-existing behavior)", raw: "", want: true},
		{name: "true", raw: "true", want: true},
		{name: "false opts into virtual-hosted style", raw: "false", want: false},
		{name: "1 parses as true", raw: "1", want: true},
		{name: "0 parses as false", raw: "0", want: false},
		{name: "invalid value defaults to true", raw: "not-a-bool", want: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := parseForcePathStyle(tc.raw); got != tc.want {
				t.Fatalf("parseForcePathStyle(%q) = %v, want %v", tc.raw, got, tc.want)
			}
		})
	}
}

func TestNormalizeKeyPrefix(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want string
	}{
		{name: "empty stays empty", raw: "", want: ""},
		{name: "plain value is unchanged", raw: "multica-prod", want: "multica-prod"},
		{name: "leading and trailing slashes are trimmed", raw: "/multica-prod/", want: "multica-prod"},
		{name: "slash-only value normalizes to empty", raw: "/", want: ""},
		{name: "surrounding whitespace is trimmed", raw: "  multica-prod  ", want: "multica-prod"},
		{name: "whitespace and slashes both trimmed", raw: " /multica-prod/ ", want: "multica-prod"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := normalizeKeyPrefix(tc.raw); got != tc.want {
				t.Fatalf("normalizeKeyPrefix(%q) = %q, want %q", tc.raw, got, tc.want)
			}
		})
	}
}

func TestKeyPrefixCollidesWithLogicalRoot(t *testing.T) {
	cases := []struct {
		prefix string
		want   bool
	}{
		{prefix: "", want: false},
		{prefix: "multica-prod", want: false},
		{prefix: "users", want: true},
		{prefix: "workspaces", want: true},
	}
	for _, tc := range cases {
		t.Run(tc.prefix, func(t *testing.T) {
			if got := keyPrefixCollidesWithLogicalRoot(tc.prefix); got != tc.want {
				t.Fatalf("keyPrefixCollidesWithLogicalRoot(%q) = %v, want %v", tc.prefix, got, tc.want)
			}
		})
	}
}

func TestKeyPrefixHasUnsafeCharacters(t *testing.T) {
	cases := []struct {
		prefix string
		want   bool
	}{
		{prefix: "", want: false},
		{prefix: "multica-prod", want: false},
		{prefix: "multica_prod.v2", want: false},
		{prefix: "org/team/multica-prod", want: false},
		{prefix: "multica prod", want: true},
		{prefix: "multica-prod?x=1", want: true},
		{prefix: "multica-prod#frag", want: true},
		{prefix: "multica\tprod", want: true},
	}
	for _, tc := range cases {
		t.Run(tc.prefix, func(t *testing.T) {
			if got := keyPrefixHasUnsafeCharacters(tc.prefix); got != tc.want {
				t.Fatalf("keyPrefixHasUnsafeCharacters(%q) = %v, want %v", tc.prefix, got, tc.want)
			}
		})
	}
}

func TestS3StoragePresignGet_AppliesConfiguredPrefix(t *testing.T) {
	store := &S3Storage{
		client: s3.New(s3.Options{
			Region:      "us-east-1",
			Credentials: aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider("AKID", "SECRET", "")),
		}),
		bucket:    "test-bucket",
		keyPrefix: "multica-prod",
	}

	got, err := store.PresignGet(context.Background(), "uploads/abc/file.txt", 5*time.Minute)
	if err != nil {
		t.Fatalf("PresignGet: %v", err)
	}
	if !strings.Contains(got, "https://test-bucket.s3.us-east-1.amazonaws.com/multica-prod/uploads/abc/file.txt") {
		t.Fatalf("presigned URL %q does not contain prefixed key", got)
	}
}

func TestS3StorageDelete_AppliesConfiguredPrefix(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	s := &S3Storage{
		client: s3.New(s3.Options{
			Region:       "us-east-1",
			Credentials:  aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider("AKID", "SECRET", "")),
			BaseEndpoint: aws.String(srv.URL),
			UsePathStyle: true,
		}),
		bucket:    "test-bucket",
		keyPrefix: "multica-prod",
	}

	s.Delete(context.Background(), "uploads/abc/file.png")

	want := "/test-bucket/multica-prod/uploads/abc/file.png"
	if gotPath != want {
		t.Fatalf("DeleteObject request path = %q, want %q", gotPath, want)
	}
}

func TestS3StorageGetReader_AppliesConfiguredPrefix(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("body"))
	}))
	defer srv.Close()

	s := &S3Storage{
		client: s3.New(s3.Options{
			Region:       "us-east-1",
			Credentials:  aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider("AKID", "SECRET", "")),
			BaseEndpoint: aws.String(srv.URL),
			UsePathStyle: true,
		}),
		bucket:    "test-bucket",
		keyPrefix: "multica-prod",
	}

	reader, err := s.GetReader(context.Background(), "uploads/abc/file.png")
	if err != nil {
		t.Fatalf("GetReader: %v", err)
	}
	reader.Close()

	want := "/test-bucket/multica-prod/uploads/abc/file.png"
	if gotPath != want {
		t.Fatalf("GetObject request path = %q, want %q", gotPath, want)
	}
}

func TestLooksLikeS3Hostname(t *testing.T) {
	cases := []struct {
		bucket string
		want   bool
	}{
		{"my-bucket", false},
		{"bucket.with.dots", false},
		{"my-bucket.s3.us-east-1.amazonaws.com", true},
		{"my-bucket.s3.amazonaws.com", true},
		{"s3.us-east-1.amazonaws.com", true},
	}
	for _, tc := range cases {
		t.Run(tc.bucket, func(t *testing.T) {
			if got := looksLikeS3Hostname(tc.bucket); got != tc.want {
				t.Fatalf("looksLikeS3Hostname(%q) = %v, want %v", tc.bucket, got, tc.want)
			}
		})
	}
}

func TestS3StorageUploadedURL(t *testing.T) {
	const key = "uploads/abc/file.png"

	cases := []struct {
		name               string
		bucket             string
		region             string
		cdnDomain          string
		endpointURL        string
		virtualHostedStyle bool
		want               string
	}{
		{
			name:   "default aws virtual hosted style",
			bucket: "test-bucket",
			region: "us-east-1",
			want:   "https://test-bucket.s3.us-east-1.amazonaws.com/uploads/abc/file.png",
		},
		{
			name:   "default aws path style when bucket contains dots",
			bucket: "bucket.with.dots",
			region: "us-east-1",
			want:   "https://s3.us-east-1.amazonaws.com/bucket.with.dots/uploads/abc/file.png",
		},
		{
			name:      "cdn only",
			bucket:    "test-bucket",
			region:    "us-east-1",
			cdnDomain: "cdn.example.com",
			want:      "https://cdn.example.com/uploads/abc/file.png",
		},
		{
			name:        "endpoint only",
			bucket:      "test-bucket",
			region:      "us-east-1",
			endpointURL: "http://localhost:9000",
			want:        "http://localhost:9000/test-bucket/uploads/abc/file.png",
		},
		{
			name:        "endpoint with trailing slash",
			bucket:      "test-bucket",
			region:      "us-east-1",
			endpointURL: "http://localhost:9000/",
			want:        "http://localhost:9000/test-bucket/uploads/abc/file.png",
		},
		{
			name:        "endpoint and cdn both set prefers cdn",
			bucket:      "test-bucket",
			region:      "us-east-1",
			cdnDomain:   "cdn.example.com",
			endpointURL: "http://localhost:9000",
			want:        "https://cdn.example.com/uploads/abc/file.png",
		},
		{
			name:               "custom endpoint virtual-hosted style (e.g. Tencent COS)",
			bucket:             "test-bucket",
			endpointURL:        "https://cos.ap-guangzhou.myqcloud.com",
			virtualHostedStyle: true,
			want:               "https://test-bucket.cos.ap-guangzhou.myqcloud.com/uploads/abc/file.png",
		},
		{
			name:               "custom endpoint virtual-hosted style with trailing slash",
			bucket:             "test-bucket",
			endpointURL:        "https://cos.ap-guangzhou.myqcloud.com/",
			virtualHostedStyle: true,
			want:               "https://test-bucket.cos.ap-guangzhou.myqcloud.com/uploads/abc/file.png",
		},
		{
			name:               "custom endpoint virtual-hosted style preserves http scheme",
			bucket:             "test-bucket",
			endpointURL:        "http://localhost:9000",
			virtualHostedStyle: true,
			want:               "http://test-bucket.localhost:9000/uploads/abc/file.png",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := &S3Storage{
				bucket:             tc.bucket,
				region:             tc.region,
				cdnDomain:          tc.cdnDomain,
				endpointURL:        tc.endpointURL,
				virtualHostedStyle: tc.virtualHostedStyle,
			}
			if got := s.uploadedURL(key); got != tc.want {
				t.Fatalf("uploadedURL() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestSplitEndpointURL(t *testing.T) {
	cases := []struct {
		endpoint   string
		wantScheme string
		wantHost   string
		wantOK     bool
	}{
		{endpoint: "https://cos.ap-guangzhou.myqcloud.com", wantScheme: "https", wantHost: "cos.ap-guangzhou.myqcloud.com", wantOK: true},
		{endpoint: "https://cos.ap-guangzhou.myqcloud.com/", wantScheme: "https", wantHost: "cos.ap-guangzhou.myqcloud.com", wantOK: true},
		{endpoint: "http://localhost:9000", wantScheme: "http", wantHost: "localhost:9000", wantOK: true},
		{endpoint: "", wantOK: false},
		{endpoint: "cos.ap-guangzhou.myqcloud.com", wantOK: false},
	}
	for _, tc := range cases {
		t.Run(tc.endpoint, func(t *testing.T) {
			scheme, host, ok := splitEndpointURL(tc.endpoint)
			if ok != tc.wantOK || scheme != tc.wantScheme || host != tc.wantHost {
				t.Fatalf("splitEndpointURL(%q) = (%q, %q, %v), want (%q, %q, %v)", tc.endpoint, scheme, host, ok, tc.wantScheme, tc.wantHost, tc.wantOK)
			}
		})
	}
}
