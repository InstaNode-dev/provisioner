// Package storage provides a MinIO backend for querying object storage usage.
// It is used only for StorageBytes queries — provisioning/deprovisioning of
// storage resources is handled directly by the API (not the provisioner).
//
// Usage is computed with the S3-compatible API using the same root/admin-style
// credentials as MinIO IAM management in the API. For each tenant we walk all
// object versions and incomplete multipart uploads under that tenant's prefix
// in the shared bucket (see api/internal/providers/storage/local.go).
package storage

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// MinIOBackend queries object storage usage by listing objects under a token's
// prefix in the shared MinIO bucket.
type MinIOBackend struct {
	client     *minio.Client
	bucketName string
}

// New creates a MinIOBackend.
// endpoint is "host:port" (no scheme), e.g. "minio.instant-data.svc.cluster.local:9000".
// accessKey / secretKey are the root MinIO credentials (read-only listing is sufficient).
func New(endpoint, accessKey, secretKey, bucketName string) (*MinIOBackend, error) {
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: false,
	})
	if err != nil {
		return nil, fmt.Errorf("storage.MinIOBackend: new client for %s: %w", endpoint, err)
	}
	if bucketName == "" {
		bucketName = "instant-shared"
	}
	return &MinIOBackend{client: client, bucketName: bucketName}, nil
}

// objectPrefix returns the S3 key prefix for a storage resource, matching
// api/internal/providers/storage/local.go (Provision).
func objectPrefix(token, providerResourceID string) string {
	p := strings.TrimSpace(providerResourceID)
	if p != "" {
		if !strings.HasSuffix(p, "/") {
			p += "/"
		}
		return p
	}
	if token == "" {
		return ""
	}
	pfx := token
	if len(pfx) > 8 {
		pfx = pfx[:8]
	}
	return pfx + "/"
}

// StorageBytes returns the total size in bytes under the tenant prefix: committed
// objects (all versions when versioning is enabled, excluding delete markers and
// zero-byte directory placeholders) plus incomplete multipart uploads.
func (b *MinIOBackend) StorageBytes(ctx context.Context, token, providerResourceID string) (int64, error) {
	prefix := objectPrefix(token, providerResourceID)
	if prefix == "" {
		return 0, fmt.Errorf("storage.MinIOBackend.StorageBytes: empty token and provider_resource_id")
	}

	exists, err := b.client.BucketExists(ctx, b.bucketName)
	if err != nil {
		return 0, fmt.Errorf("storage.MinIOBackend.StorageBytes: bucket exists %q: %w", b.bucketName, err)
	}
	if !exists {
		return 0, fmt.Errorf("storage.MinIOBackend.StorageBytes: bucket %q does not exist", b.bucketName)
	}

	var total int64
	for obj := range b.client.ListObjects(ctx, b.bucketName, minio.ListObjectsOptions{
		Prefix:       prefix,
		Recursive:    true,
		WithVersions: true,
	}) {
		if obj.Err != nil {
			return 0, fmt.Errorf("storage.MinIOBackend.StorageBytes: list objects under %q: %w", prefix, obj.Err)
		}
		if obj.IsDeleteMarker {
			continue
		}
		if strings.HasSuffix(obj.Key, "/") && obj.Size == 0 {
			continue
		}
		total += obj.Size
	}

	for part := range b.client.ListIncompleteUploads(ctx, b.bucketName, prefix, true) {
		if part.Err != nil {
			return 0, fmt.Errorf("storage.MinIOBackend.StorageBytes: list multipart under %q: %w", prefix, part.Err)
		}
		total += part.Size
	}

	slog.Debug("storage.MinIOBackend.StorageBytes",
		"token", token,
		"prefix", prefix,
		"bytes", total,
	)
	return total, nil
}
