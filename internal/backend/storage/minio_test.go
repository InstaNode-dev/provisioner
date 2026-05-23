package storage

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestObjectPrefix(t *testing.T) {
	t.Parallel()
	cases := []struct {
		token, provider, want string
	}{
		{"abcdefgh-0000-0000-0000-000000000001", "", "abcdefgh/"},
		{"ab", "", "ab/"},
		{"", "", ""},
		{"", "custom/", "custom/"},
		{"ignored", "my/prefix", "my/prefix/"},
		{"ignored", "my/prefix/", "my/prefix/"},
		// Long token gets truncated to first 8 chars
		{"abcdefghij", "", "abcdefgh/"},
		// Provider with trailing slash kept as-is
		{"tok", "prov/", "prov/"},
	}
	for _, tc := range cases {
		if got := objectPrefix(tc.token, tc.provider); got != tc.want {
			t.Errorf("objectPrefix(%q,%q) = %q, want %q", tc.token, tc.provider, got, tc.want)
		}
	}
}

// --- New() error/happy paths ---

func TestNew_DefaultBucket(t *testing.T) {
	t.Parallel()
	b, err := New("127.0.0.1:9000", "ak", "sk", "")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if b.bucketName != "instant-shared" {
		t.Errorf("bucketName = %q, want instant-shared (default)", b.bucketName)
	}
	if b.client == nil {
		t.Error("client is nil")
	}
}

func TestNew_CustomBucket(t *testing.T) {
	t.Parallel()
	b, err := New("127.0.0.1:9000", "ak", "sk", "my-bucket")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if b.bucketName != "my-bucket" {
		t.Errorf("bucketName = %q, want my-bucket", b.bucketName)
	}
}

func TestNew_InvalidEndpoint(t *testing.T) {
	t.Parallel()
	// minio.New rejects endpoints containing schemes/paths/invalid hosts.
	_, err := New("http://bad endpoint with spaces", "ak", "sk", "b")
	if err == nil {
		t.Fatal("expected error for invalid endpoint")
	}
	if !strings.Contains(err.Error(), "storage.MinIOBackend: new client") {
		t.Errorf("error message missing context: %v", err)
	}
}

// --- StorageBytes() ---

// handleLocation responds to ?location= queries (GetBucketLocation) with
// us-east-1. minio-go calls this on most operations to discover region.
// Returns true if the request was handled.
func handleLocation(w http.ResponseWriter, q url.Values) bool {
	if _, ok := q["location"]; !ok {
		return false
	}
	w.Header().Set("Content-Type", "application/xml")
	_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">us-east-1</LocationConstraint>`))
	return true
}

// fakeS3Server emulates the subset of S3 calls minio-go makes for
// BucketExists, ListObjects (v2 with versions), and ListIncompleteUploads.
type fakeS3Config struct {
	bucketExistsStatus  int    // 200 = exists, 404 = not exist, 500 = error
	listObjectsBody     string // XML body for ListObjectVersions
	listObjectsStatus   int    // default 200
	listMultipartBody   string // XML body for ListMultipartUploads
	listMultipartStatus int    // default 200
}

func newFakeS3(t *testing.T, cfg fakeS3Config) (*httptest.Server, *MinIOBackend) {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		// Path is /<bucket>[/object] or just /<bucket> for bucket-level ops.
		// minio-go uses virtual-host style by default but with a raw host:port endpoint
		// it falls back to path-style. We'll accept either.
		switch r.Method {
		case http.MethodHead:
			// BucketExists -> HEAD /<bucket>
			st := cfg.bucketExistsStatus
			if st == 0 {
				st = http.StatusOK
			}
			// Minio relies on the x-minio-error-code header for non-200 HEADs
			// since HEAD has no body.
			if st == http.StatusNotFound {
				w.Header().Set("x-minio-error-code", "NoSuchBucket")
				w.Header().Set("x-minio-error-desc", "The specified bucket does not exist.")
			}
			w.WriteHeader(st)
			return
		case http.MethodGet:
			// GetBucketLocation: ?location — minio-go calls this before most ops
			// to discover region. Return us-east-1 (default).
			if _, ok := q["location"]; ok {
				w.Header().Set("Content-Type", "application/xml")
				_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">us-east-1</LocationConstraint>`))
				return
			}
			// ListObjectVersions: ?versions
			// ListMultipartUploads: ?uploads
			// minio-go ListObjects with WithVersions=true uses ?versions
			if _, ok := q["versions"]; ok {
				st := cfg.listObjectsStatus
				if st == 0 {
					st = http.StatusOK
				}
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(st)
				_, _ = w.Write([]byte(cfg.listObjectsBody))
				return
			}
			if _, ok := q["uploads"]; ok {
				st := cfg.listMultipartStatus
				if st == 0 {
					st = http.StatusOK
				}
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(st)
				_, _ = w.Write([]byte(cfg.listMultipartBody))
				return
			}
			// Fallback: treat as ListObjectsV2
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(cfg.listObjectsBody))
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("parse srv url: %v", err)
	}
	b, err := New(u.Host, "ak", "sk", "test-bucket")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return srv, b
}

// versionsBody returns a valid ListVersionsResult XML body.
func versionsBody(t *testing.T, objs []versionObj) string {
	t.Helper()
	type vXML struct {
		XMLName        xml.Name `xml:"Version"`
		Key            string   `xml:"Key"`
		Size           int64    `xml:"Size"`
		IsLatest       bool     `xml:"IsLatest"`
		VersionID      string   `xml:"VersionId"`
		LastModified   string   `xml:"LastModified"`
	}
	type dmXML struct {
		XMLName      xml.Name `xml:"DeleteMarker"`
		Key          string   `xml:"Key"`
		IsLatest     bool     `xml:"IsLatest"`
		VersionID    string   `xml:"VersionId"`
		LastModified string   `xml:"LastModified"`
	}

	var sb strings.Builder
	sb.WriteString(`<?xml version="1.0" encoding="UTF-8"?>`)
	sb.WriteString(`<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
	sb.WriteString(`<Name>test-bucket</Name>`)
	sb.WriteString(`<Prefix></Prefix>`)
	sb.WriteString(`<KeyMarker></KeyMarker>`)
	sb.WriteString(`<VersionIdMarker></VersionIdMarker>`)
	sb.WriteString(`<MaxKeys>1000</MaxKeys>`)
	sb.WriteString(`<IsTruncated>false</IsTruncated>`)
	for _, o := range objs {
		if o.deleteMarker {
			b, _ := xml.Marshal(dmXML{Key: o.key, IsLatest: true, VersionID: "null", LastModified: "2026-01-01T00:00:00.000Z"})
			sb.Write(b)
		} else {
			b, _ := xml.Marshal(vXML{Key: o.key, Size: o.size, IsLatest: true, VersionID: "null", LastModified: "2026-01-01T00:00:00.000Z"})
			sb.Write(b)
		}
	}
	sb.WriteString(`</ListVersionsResult>`)
	return sb.String()
}

type versionObj struct {
	key          string
	size         int64
	deleteMarker bool
}

func multipartBody(t *testing.T, uploads []multipartUp) string {
	t.Helper()
	var sb strings.Builder
	sb.WriteString(`<?xml version="1.0" encoding="UTF-8"?>`)
	sb.WriteString(`<ListMultipartUploadsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
	sb.WriteString(`<Bucket>test-bucket</Bucket>`)
	sb.WriteString(`<KeyMarker></KeyMarker>`)
	sb.WriteString(`<UploadIdMarker></UploadIdMarker>`)
	sb.WriteString(`<MaxUploads>1000</MaxUploads>`)
	sb.WriteString(`<IsTruncated>false</IsTruncated>`)
	for _, u := range uploads {
		sb.WriteString(`<Upload>`)
		sb.WriteString(`<Key>` + u.key + `</Key>`)
		sb.WriteString(`<UploadId>` + u.uploadID + `</UploadId>`)
		sb.WriteString(`<Size>` + fmt.Sprintf("%d", u.size) + `</Size>`)
		sb.WriteString(`<Initiated>2026-01-01T00:00:00.000Z</Initiated>`)
		sb.WriteString(`</Upload>`)
	}
	sb.WriteString(`</ListMultipartUploadsResult>`)
	return sb.String()
}

type multipartUp struct {
	key      string
	uploadID string
	size     int64
}

func TestStorageBytes_EmptyPrefix(t *testing.T) {
	t.Parallel()
	b, err := New("127.0.0.1:9000", "ak", "sk", "")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	got, err := b.StorageBytes(context.Background(), "", "")
	if err == nil {
		t.Fatal("expected error for empty token and provider_resource_id")
	}
	if got != 0 {
		t.Errorf("bytes = %d, want 0", got)
	}
	if !strings.Contains(err.Error(), "empty token") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestStorageBytes_BucketExistsError(t *testing.T) {
	t.Parallel()
	_, b := newFakeS3(t, fakeS3Config{
		bucketExistsStatus: http.StatusInternalServerError,
	})
	_, err := b.StorageBytes(context.Background(), "abcdefgh", "")
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "bucket exists") {
		t.Errorf("error missing 'bucket exists' context: %v", err)
	}
}

func TestStorageBytes_BucketNotExist(t *testing.T) {
	t.Parallel()
	_, b := newFakeS3(t, fakeS3Config{
		bucketExistsStatus: http.StatusNotFound,
	})
	_, err := b.StorageBytes(context.Background(), "abcdefgh", "")
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "does not exist") {
		t.Errorf("error missing 'does not exist': %v", err)
	}
}

func TestStorageBytes_HappyPathSumsObjectVersionsAndMultipart(t *testing.T) {
	t.Parallel()
	objs := []versionObj{
		{key: "abcdefgh/file1.txt", size: 100},
		{key: "abcdefgh/file2.txt", size: 250},
		{key: "abcdefgh/dir/", size: 0},          // dir placeholder — skipped
		{key: "abcdefgh/file3.txt", deleteMarker: true}, // delete marker — skipped
		{key: "abcdefgh/big.bin", size: 1024},
	}
	uploads := []multipartUp{
		{key: "abcdefgh/incomplete1", uploadID: "u1", size: 50},
		{key: "abcdefgh/incomplete2", uploadID: "u2", size: 75},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		switch r.Method {
		case http.MethodHead:
			w.WriteHeader(http.StatusOK)
			return
		case http.MethodGet:
			if handleLocation(w, q) {
				return
			}
			if _, ok := q["versions"]; ok {
				w.Header().Set("Content-Type", "application/xml")
				_, _ = w.Write([]byte(versionsBody(t, objs)))
				return
			}
			if _, ok := q["uploads"]; ok {
				w.Header().Set("Content-Type", "application/xml")
				_, _ = w.Write([]byte(multipartBody(t, uploads)))
				return
			}
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(versionsBody(t, objs)))
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	u, _ := url.Parse(srv.URL)
	b, err := New(u.Host, "ak", "sk", "test-bucket")
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	got, err := b.StorageBytes(context.Background(), "abcdefgh-token-uuid", "")
	if err != nil {
		t.Fatalf("StorageBytes: %v", err)
	}
	// Objects: 100 + 250 + 1024 = 1374. Multipart u1=50, u2=75 = 125. Total 1499.
	const want = int64(1374 + 50 + 75)
	if got != want {
		t.Errorf("bytes = %d, want %d", got, want)
	}
}

func TestStorageBytes_ListObjectsError(t *testing.T) {
	t.Parallel()
	// Bucket exists, but ListObjectVersions returns 500.
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
			return
		}
		q := r.URL.Query()
		if handleLocation(w, q) {
			return
		}
		if _, ok := q["versions"]; ok {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`<?xml version="1.0"?><Error><Code>InternalError</Code><Message>boom</Message></Error>`))
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	u, _ := url.Parse(srv.URL)
	b, _ := New(u.Host, "ak", "sk", "test-bucket")

	_, err := b.StorageBytes(context.Background(), "abcdefgh", "")
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "list objects") {
		t.Errorf("error missing 'list objects': %v", err)
	}
}

func TestStorageBytes_ListMultipartError(t *testing.T) {
	t.Parallel()
	objs := []versionObj{{key: "abcdefgh/f", size: 10}}
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
			return
		}
		q := r.URL.Query()
		if handleLocation(w, q) {
			return
		}
		if _, ok := q["versions"]; ok {
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(versionsBody(t, objs)))
			return
		}
		if _, ok := q["uploads"]; ok {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`<?xml version="1.0"?><Error><Code>InternalError</Code><Message>boom</Message></Error>`))
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	u, _ := url.Parse(srv.URL)
	b, _ := New(u.Host, "ak", "sk", "test-bucket")

	_, err := b.StorageBytes(context.Background(), "abcdefgh", "")
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "list multipart") {
		t.Errorf("error missing 'list multipart': %v", err)
	}
}

func TestStorageBytes_ProviderResourceIDPath(t *testing.T) {
	t.Parallel()
	objs := []versionObj{{key: "tenant-prefix/x", size: 42}}
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
			return
		}
		q := r.URL.Query()
		if handleLocation(w, q) {
			return
		}
		// Verify that the prefix the client passes matches what we expect.
		if _, ok := q["versions"]; ok {
			if pfx := q.Get("prefix"); pfx != "tenant-prefix/" {
				t.Errorf("prefix = %q, want tenant-prefix/", pfx)
			}
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(versionsBody(t, objs)))
			return
		}
		if _, ok := q["uploads"]; ok {
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(multipartBody(t, nil)))
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	u, _ := url.Parse(srv.URL)
	b, _ := New(u.Host, "ak", "sk", "test-bucket")

	got, err := b.StorageBytes(context.Background(), "ignored", "tenant-prefix")
	if err != nil {
		t.Fatalf("StorageBytes: %v", err)
	}
	if got != 42 {
		t.Errorf("bytes = %d, want 42", got)
	}
}

func TestStorageBytes_EmptyListing(t *testing.T) {
	t.Parallel()
	_, b := newFakeS3(t, fakeS3Config{
		listObjectsBody:   versionsBody(t, nil),
		listMultipartBody: multipartBody(t, nil),
	})
	got, err := b.StorageBytes(context.Background(), "abcdefgh", "")
	if err != nil {
		t.Fatalf("StorageBytes: %v", err)
	}
	if got != 0 {
		t.Errorf("bytes = %d, want 0", got)
	}
}
