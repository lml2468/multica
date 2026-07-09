package storage

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
)

// newS3TestStorage connects to a real S3-compatible endpoint (e.g. local
// MinIO, or a real Tencent COS bucket) via S3_TEST_ENDPOINT_URL, and skips
// when unset so `go test ./...` keeps working without any external
// dependency — mirrors newRedisTestClient in internal/auth/pat_cache_test.go.
//
// Local dev against MinIO (path-style, the default): `docker run -p 9000:9000
// -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin
// minio/minio server /data`, then `S3_TEST_ENDPOINT_URL=http://localhost:9000
// go test ./internal/storage/...`. When S3_TEST_BUCKET is unset a throwaway
// bucket is created and torn down automatically, which MinIO allows.
//
// Against a real backend that requires virtual-hosted-style requests (e.g.
// Tencent COS, which rejects path-style with PathStyleDomainForbidden — see
// S3_FORCE_PATH_STYLE), set S3_TEST_FORCE_PATH_STYLE=false and
// S3_TEST_BUCKET to an existing bucket you own (bucket create/delete is
// skipped when S3_TEST_BUCKET is set, since COS bucket naming/permissions
// don't suit ephemeral throwaway buckets the way MinIO's do):
//
//	S3_TEST_ENDPOINT_URL=https://cos.ap-guangzhou.myqcloud.com \
//	S3_TEST_FORCE_PATH_STYLE=false \
//	S3_TEST_BUCKET=my-bucket-1250000000 \
//	S3_TEST_ACCESS_KEY_ID=... S3_TEST_SECRET_ACCESS_KEY=... \
//	go test ./internal/storage/... -run Integration -v
func newS3TestStorage(t *testing.T, keyPrefix string) *S3Storage {
	t.Helper()
	endpoint := os.Getenv("S3_TEST_ENDPOINT_URL")
	if endpoint == "" {
		t.Skip("S3_TEST_ENDPOINT_URL not set")
	}
	accessKey := os.Getenv("S3_TEST_ACCESS_KEY_ID")
	if accessKey == "" {
		accessKey = "minioadmin"
	}
	secretKey := os.Getenv("S3_TEST_SECRET_ACCESS_KEY")
	if secretKey == "" {
		secretKey = "minioadmin"
	}
	forcePathStyle := parseForcePathStyle(os.Getenv("S3_TEST_FORCE_PATH_STYLE"))

	client := s3.New(s3.Options{
		Region:       "us-east-1",
		Credentials:  aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
		BaseEndpoint: aws.String(endpoint),
		UsePathStyle: forcePathStyle,
	})

	ctx := context.Background()
	bucket := os.Getenv("S3_TEST_BUCKET")
	ownsBucket := bucket == ""
	if ownsBucket {
		bucket = "multica-test-" + uuid.NewString()
		if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
			t.Skipf("S3_TEST_ENDPOINT_URL unreachable or bucket creation failed: %v", err)
		}
	}
	t.Cleanup(func() {
		cleanupCtx := context.Background()
		out, err := client.ListObjectsV2(cleanupCtx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
		if err == nil {
			for _, obj := range out.Contents {
				client.DeleteObject(cleanupCtx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: obj.Key})
			}
		}
		if ownsBucket {
			client.DeleteBucket(cleanupCtx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
		}
	})

	return &S3Storage{
		client:             client,
		bucket:             bucket,
		region:             "us-east-1",
		endpointURL:        endpoint,
		virtualHostedStyle: !forcePathStyle,
		keyPrefix:          keyPrefix,
	}
}

func TestS3StorageIntegration_UploadGetDeleteRoundTrip(t *testing.T) {
	s := newS3TestStorage(t, "")
	ctx := context.Background()

	key := "users/u1/" + uuid.NewString() + ".txt"
	body := []byte("hello multica")

	url, err := s.Upload(ctx, key, body, "text/plain", "file.txt")
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}
	if url == "" {
		t.Fatal("Upload returned empty URL")
	}

	reader, err := s.GetReader(ctx, key)
	if err != nil {
		t.Fatalf("GetReader: %v", err)
	}
	got, err := io.ReadAll(reader)
	reader.Close()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("round-tripped body = %q, want %q", got, body)
	}

	s.Delete(ctx, key)

	if _, err := s.GetReader(ctx, key); err == nil {
		t.Fatal("expected GetReader to fail after Delete")
	}
}

func TestS3StorageIntegration_UploadWithPrefix_RoundTripsViaKeyFromURL(t *testing.T) {
	const keyPrefix = "multica-prod"
	s := newS3TestStorage(t, keyPrefix)
	ctx := context.Background()

	key := "workspaces/w1/" + uuid.NewString() + ".txt"
	body := []byte("prefixed object")

	url, err := s.Upload(ctx, key, body, "text/plain", "file.txt")
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}
	if !strings.Contains(url, keyPrefix+"/"+key) {
		t.Fatalf("uploaded URL %q does not contain prefixed key %q", url, keyPrefix+"/"+key)
	}

	logicalKey := s.KeyFromURL(url)
	if logicalKey != key {
		t.Fatalf("KeyFromURL(%q) = %q, want logical key %q", url, logicalKey, key)
	}

	reader, err := s.GetReader(ctx, logicalKey)
	if err != nil {
		t.Fatalf("GetReader with logical key recovered from KeyFromURL: %v", err)
	}
	got, err := io.ReadAll(reader)
	reader.Close()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("round-tripped body = %q, want %q", got, body)
	}

	s.Delete(ctx, logicalKey)
	if _, err := s.GetReader(ctx, logicalKey); err == nil {
		t.Fatal("expected GetReader to fail after Delete")
	}
}

func TestS3StorageIntegration_PresignGetReturnsDownloadableURL(t *testing.T) {
	s := newS3TestStorage(t, "multica-prod")
	ctx := context.Background()

	key := "users/u1/" + uuid.NewString() + ".txt"
	body := []byte("presigned content")
	if _, err := s.Upload(ctx, key, body, "text/plain", "file.txt"); err != nil {
		t.Fatalf("Upload: %v", err)
	}

	presignedURL, err := s.PresignGet(ctx, key, 5*time.Minute)
	if err != nil {
		t.Fatalf("PresignGet: %v", err)
	}

	resp, err := http.Get(presignedURL)
	if err != nil {
		t.Fatalf("GET presigned URL: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET presigned URL status = %d, want 200", resp.StatusCode)
	}
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("presigned download body = %q, want %q", got, body)
	}
}
