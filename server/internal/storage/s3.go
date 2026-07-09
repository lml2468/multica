package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type S3Storage struct {
	client             *s3.Client
	bucket             string
	region             string // used to construct virtual-hosted-style public URLs when no CDN/endpoint is set
	cdnDomain          string // if set, returned URLs use this instead of bucket name
	endpointURL        string // if set, use a custom endpoint (e.g. MinIO, Tencent COS) instead of AWS S3
	virtualHostedStyle bool   // if true, use <bucket>.<endpoint-host> instead of path-style with endpointURL; some backends (e.g. Tencent COS) reject path-style with PathStyleDomainForbidden
	keyPrefix          string // if set, prepended to every object key so a bucket/CDN domain can be shared with other apps
}

// NewS3StorageFromEnv creates an S3Storage from environment variables.
// Returns nil if S3_BUCKET is not set.
//
// Environment variables:
//   - S3_BUCKET (required)
//   - S3_REGION (default: us-west-2)
//   - AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY (optional; falls back to default credential chain)
//   - S3_FORCE_PATH_STYLE (optional boolean, default true when AWS_ENDPOINT_URL
//     is set; matches the pre-existing always-path-style behavior needed by
//     MinIO. Set to false for backends that require virtual-hosted-style
//     requests instead, e.g. Tencent COS, which rejects path-style with
//     PathStyleDomainForbidden)
//   - S3_KEY_PREFIX (optional; surrounding whitespace and leading/trailing
//     slashes are trimmed. Set it before the first upload — changing it
//     later does not migrate already-uploaded objects: downloads/presigns
//     against them start 404ing, and Delete silently no-ops on them since
//     S3 DeleteObject does not error on a missing key, orphaning the real
//     object. Only manually copying them under the new prefix restores
//     access)
func NewS3StorageFromEnv() *S3Storage {
	bucket := os.Getenv("S3_BUCKET")
	if bucket == "" {
		slog.Info("S3_BUCKET not set, cloud upload disabled")
		return nil
	}
	if looksLikeS3Hostname(bucket) {
		slog.Warn(
			"S3_BUCKET looks like a hostname rather than a bucket name — uploads and public URLs will likely both fail. Use only the bucket name (e.g. \"my-bucket\"), not \"<bucket>.s3.<region>.amazonaws.com\".",
			"value", bucket,
		)
	}

	region := os.Getenv("S3_REGION")
	if region == "" {
		region = "us-west-2"
	}

	opts := []func(*config.LoadOptions) error{
		config.WithRegion(region),
	}

	accessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	if accessKey != "" && secretKey != "" {
		opts = append(opts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
		))
	}

	cfg, err := config.LoadDefaultConfig(context.Background(), opts...)
	if err != nil {
		slog.Error("failed to load AWS config", "error", err)
		return nil
	}

	cdnDomain := os.Getenv("CLOUDFRONT_DOMAIN")

	forcePathStyle := parseForcePathStyle(os.Getenv("S3_FORCE_PATH_STYLE"))

	endpointURL := os.Getenv("AWS_ENDPOINT_URL")
	s3Opts := []func(*s3.Options){}
	if endpointURL != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(endpointURL)
			o.UsePathStyle = forcePathStyle
		})
	}
	virtualHostedStyle := endpointURL != "" && !forcePathStyle

	keyPrefix := normalizeKeyPrefix(os.Getenv("S3_KEY_PREFIX"))
	if keyPrefixCollidesWithLogicalRoot(keyPrefix) {
		slog.Warn(
			"S3_KEY_PREFIX matches an existing top-level object key segment (\"users\" or \"workspaces\") — this corrupts the intermediate logical key KeyFromURL returns for objects uploaded before the prefix was set, even though S3 reads/writes still work because objectKey re-applies the same segment. Choose a different prefix.",
			"value", keyPrefix,
		)
	}
	if keyPrefixHasUnsafeCharacters(keyPrefix) {
		slog.Warn(
			"S3_KEY_PREFIX contains characters outside [A-Za-z0-9._/-] — these are concatenated raw into public URLs (no escaping), so spaces, query/fragment characters, or control characters can produce malformed URLs even though S3 itself would accept the object key.",
			"value", keyPrefix,
		)
	}

	slog.Info("S3 storage initialized", "bucket", bucket, "region", region, "cdn_domain", cdnDomain, "endpoint_url", endpointURL, "virtual_hosted_style", virtualHostedStyle, "key_prefix", keyPrefix)
	return &S3Storage{
		client:             s3.NewFromConfig(cfg, s3Opts...),
		bucket:             bucket,
		region:             region,
		cdnDomain:          cdnDomain,
		endpointURL:        endpointURL,
		virtualHostedStyle: virtualHostedStyle,
		keyPrefix:          keyPrefix,
	}
}

// parseForcePathStyle parses S3_FORCE_PATH_STYLE, defaulting to true (the
// pre-existing always-path-style behavior) when unset or unparseable so
// existing MinIO-style deployments are unaffected.
func parseForcePathStyle(raw string) bool {
	if raw == "" {
		return true
	}
	parsed, err := strconv.ParseBool(raw)
	if err != nil {
		slog.Warn("S3_FORCE_PATH_STYLE is not a valid boolean, defaulting to true (path-style)", "value", raw)
		return true
	}
	return parsed
}

func (s *S3Storage) CdnDomain() string {
	return s.cdnDomain
}

// looksLikeS3Hostname returns true when the configured S3_BUCKET value looks
// like an S3 endpoint hostname rather than a bucket name. Real bucket names
// can never legitimately contain "amazonaws.com", so this is an unambiguous
// misconfiguration signal — the most common form being users pasting
// "<bucket>.s3.<region>.amazonaws.com" into S3_BUCKET.
func looksLikeS3Hostname(bucket string) bool {
	return strings.Contains(bucket, "amazonaws.com")
}

// normalizeKeyPrefix trims surrounding whitespace and slashes from a raw
// S3_KEY_PREFIX value, e.g. " /multica-prod/ " -> "multica-prod". A
// slash-only value normalizes to "" (no prefix).
func normalizeKeyPrefix(raw string) string {
	return strings.Trim(strings.TrimSpace(raw), "/")
}

// keyPrefixCollidesWithLogicalRoot reports whether prefix matches one of the
// fixed top-level segments the uploader already writes objects under (see
// file.go). Such a prefix corrupts the intermediate logical key that
// KeyFromURL returns for objects uploaded before the prefix was configured —
// S3 reads/writes still work because objectKey re-applies the identical
// segment, but the logical key briefly round-trips through a wrong value.
func keyPrefixCollidesWithLogicalRoot(prefix string) bool {
	return prefix == "users" || prefix == "workspaces"
}

// keyPrefixHasUnsafeCharacters reports whether prefix contains characters
// outside the conservative charset object keys and raw string-concatenated
// URLs tolerate without escaping (see uploadedURL). S3 itself accepts a much
// wider key charset, but a prefix with spaces, query/fragment characters, or
// control characters corrupts the public URLs this package builds.
func keyPrefixHasUnsafeCharacters(prefix string) bool {
	for _, r := range prefix {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			continue
		case r == '-' || r == '_' || r == '.' || r == '/':
			continue
		default:
			return true
		}
	}
	return false
}

// splitEndpointURL splits a configured AWS_ENDPOINT_URL into scheme and
// host, e.g. "https://cos.ap-guangzhou.myqcloud.com/" ->
// ("https", "cos.ap-guangzhou.myqcloud.com"). ok is false when the endpoint
// has no recognized scheme.
func splitEndpointURL(endpoint string) (scheme, host string, ok bool) {
	trimmed := strings.TrimRight(endpoint, "/")
	if rest, found := strings.CutPrefix(trimmed, "https://"); found {
		return "https", rest, true
	}
	if rest, found := strings.CutPrefix(trimmed, "http://"); found {
		return "http", rest, true
	}
	return "", "", false
}

// objectKey applies the configured key prefix (if any) to a logical key,
// producing the actual key used against the S3 API. Callers throughout the
// rest of the codebase only ever deal in logical keys (e.g. "users/u1/f.png");
// this is the single point where the prefix is introduced.
func (s *S3Storage) objectKey(key string) string {
	if s.keyPrefix == "" {
		return key
	}
	return s.keyPrefix + "/" + key
}

// stripKeyPrefix removes the configured key prefix from an S3 object key,
// recovering the logical key. It is the inverse of objectKey.
func (s *S3Storage) stripKeyPrefix(key string) string {
	if s.keyPrefix == "" {
		return key
	}
	return strings.TrimPrefix(key, s.keyPrefix+"/")
}

// storageClass returns the appropriate S3 storage class.
// Custom endpoints (e.g. MinIO) only support STANDARD; real AWS defaults to INTELLIGENT_TIERING.
func (s *S3Storage) storageClass() types.StorageClass {
	if s.endpointURL != "" {
		return types.StorageClassStandard
	}
	return types.StorageClassIntelligentTiering
}

// KeyFromURL extracts the S3 object key from a CDN or bucket URL.
// e.g. "https://multica-static.copilothub.ai/abc123.png" → "abc123.png"
//
//	"https://my-bucket.s3.us-east-1.amazonaws.com/uploads/x/y.png" → "uploads/x/y.png"
func (s *S3Storage) KeyFromURL(rawURL string) string {
	return s.stripKeyPrefix(s.rawKeyFromURL(rawURL))
}

// rawKeyFromURL parses the object key out of a CDN or bucket URL as it is
// physically stored in S3, i.e. including the configured key prefix (if any).
func (s *S3Storage) rawKeyFromURL(rawURL string) string {
	if s.endpointURL != "" {
		// Recognize both URL shapes regardless of the current
		// S3_FORCE_PATH_STYLE setting, so a stored URL keeps resolving even
		// after an operator flips that setting post-deployment.
		if scheme, host, ok := splitEndpointURL(s.endpointURL); ok {
			vhPrefix := scheme + "://" + s.bucket + "." + host + "/"
			if strings.HasPrefix(rawURL, vhPrefix) {
				return strings.TrimPrefix(rawURL, vhPrefix)
			}
		}
		psPrefix := strings.TrimRight(s.endpointURL, "/") + "/" + s.bucket + "/"
		if strings.HasPrefix(rawURL, psPrefix) {
			return strings.TrimPrefix(rawURL, psPrefix)
		}
	}

	// Strip known "https://host/" prefixes. Order matters: the more specific
	// region-qualified hosts come first so they win over the legacy bucket-only
	// prefix that we used to write before the suffix bug was fixed.
	prefixes := make([]string, 0, 5)
	if s.cdnDomain != "" {
		prefixes = append(prefixes, "https://"+s.cdnDomain+"/")
	}
	if s.region != "" {
		// virtual-hosted-style: https://<bucket>.s3.<region>.amazonaws.com/<key>
		prefixes = append(prefixes,
			"https://"+s.bucket+".s3."+s.region+".amazonaws.com/",
			// path-style: https://s3.<region>.amazonaws.com/<bucket>/<key>
			"https://s3."+s.region+".amazonaws.com/"+s.bucket+"/",
		)
	}
	// Legacy / fallback: the buggy "https://<bucket>/<key>" form that older
	// records may still hold, plus a generic bucket-host prefix.
	prefixes = append(prefixes, "https://"+s.bucket+"/")

	for _, prefix := range prefixes {
		if strings.HasPrefix(rawURL, prefix) {
			return strings.TrimPrefix(rawURL, prefix)
		}
	}
	// Fallback: take everything after the last "/".
	if i := strings.LastIndex(rawURL, "/"); i >= 0 {
		return rawURL[i+1:]
	}
	return rawURL
}

// GetReader streams the object body back to the caller. The returned
// ReadCloser must be closed; closing it terminates the underlying HTTP
// connection to S3. A missing key surfaces as an *types.NoSuchKey error
// wrapped in the SDK's smithy wrapper — callers can use errors.As to
// distinguish "not found" from a transport failure.
func (s *S3Storage) GetReader(ctx context.Context, key string) (io.ReadCloser, error) {
	if key == "" {
		return nil, fmt.Errorf("s3 GetReader: empty key")
	}
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.objectKey(key)),
	})
	if err != nil {
		return nil, fmt.Errorf("s3 GetObject: %w", err)
	}
	return out.Body, nil
}

func (s *S3Storage) PresignGet(ctx context.Context, key string, ttl time.Duration) (string, error) {
	return s.PresignGetWithContentDisposition(ctx, key, ttl, "")
}

func (s *S3Storage) PresignGetWithContentDisposition(ctx context.Context, key string, ttl time.Duration, contentDisposition string) (string, error) {
	if key == "" {
		return "", fmt.Errorf("s3 PresignGet: empty key")
	}
	if ttl <= 0 {
		ttl = 30 * time.Minute
	}
	input := &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.objectKey(key)),
	}
	if contentDisposition != "" {
		input.ResponseContentDisposition = aws.String(contentDisposition)
	}
	out, err := s3.NewPresignClient(s.client).PresignGetObject(ctx, input, func(opts *s3.PresignOptions) {
		opts.Expires = ttl
	})
	if err != nil {
		return "", fmt.Errorf("s3 PresignGetObject: %w", err)
	}
	return out.URL, nil
}

// Delete removes an object from S3. Errors are logged but not fatal.
func (s *S3Storage) Delete(ctx context.Context, key string) {
	if key == "" {
		return
	}
	objKey := s.objectKey(key)
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(objKey),
	})
	if err != nil {
		slog.Error("s3 DeleteObject failed", "key", objKey, "error", err)
	}
}

// DeleteKeys removes multiple objects from S3. Best-effort, errors are logged.
func (s *S3Storage) DeleteKeys(ctx context.Context, keys []string) {
	for _, key := range keys {
		s.Delete(ctx, key)
	}
}

func (s *S3Storage) Upload(ctx context.Context, key string, data []byte, contentType string, filename string) (string, error) {
	objKey := s.objectKey(key)
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:             aws.String(s.bucket),
		Key:                aws.String(objKey),
		Body:               bytes.NewReader(data),
		ContentType:        aws.String(contentType),
		ContentDisposition: aws.String(ContentDisposition(contentType, filename)),
		CacheControl:       aws.String("max-age=432000,public"),
		StorageClass:       s.storageClass(),
	})
	if err != nil {
		return "", fmt.Errorf("s3 PutObject: %w", err)
	}
	return s.uploadedURL(objKey), nil
}

// uploadedURL returns the URL stored for client consumption after an upload.
// Priority: CDN domain > custom endpoint > AWS S3 region-qualified host. The CDN
// domain wins even when a custom endpoint is set so S3-compatible backends
// (MinIO, R2, B2, Wasabi, etc.) can be paired with a separate public-read
// domain — writes still go through the SDK with the custom endpoint; only the
// reader-facing URL changes.
//
// For the default AWS S3 case, virtual-hosted-style is preferred:
// https://<bucket>.s3.<region>.amazonaws.com/<key>. When the bucket name
// contains dots, the AWS-issued wildcard TLS certificate (`*.s3.amazonaws.com`)
// fails to validate the host, so we fall back to path-style:
// https://s3.<region>.amazonaws.com/<bucket>/<key>.
func (s *S3Storage) uploadedURL(key string) string {
	if s.cdnDomain != "" {
		return fmt.Sprintf("https://%s/%s", s.cdnDomain, key)
	}
	if s.endpointURL != "" {
		if s.virtualHostedStyle {
			if scheme, host, ok := splitEndpointURL(s.endpointURL); ok {
				return fmt.Sprintf("%s://%s.%s/%s", scheme, s.bucket, host, key)
			}
		}
		return fmt.Sprintf("%s/%s/%s", strings.TrimRight(s.endpointURL, "/"), s.bucket, key)
	}
	if strings.Contains(s.bucket, ".") {
		return fmt.Sprintf("https://s3.%s.amazonaws.com/%s/%s", s.region, s.bucket, key)
	}
	return fmt.Sprintf("https://%s.s3.%s.amazonaws.com/%s", s.bucket, s.region, key)
}
