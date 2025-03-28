// Copyright (C) 2024 Storj Labs, Inc.
// See LICENSE for copying information.

package metasearch

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zeebo/assert"
	"go.uber.org/zap"

	"storj.io/common/uuid"
)

// Mock repository

type mockRepo struct {
	objects map[string]ObjectInfo
}

func newMockRepo() *mockRepo {
	return &mockRepo{
		objects: make(map[string]ObjectInfo),
	}
}

func (r *mockRepo) GetMetadata(ctx context.Context, loc ObjectLocation) (ObjectInfo, error) {
	path := fmt.Sprintf("sj://%s/%s", loc.BucketName, loc.ObjectKey)
	obj, ok := r.objects[path]
	if !ok {
		return ObjectInfo{}, ErrNotFound
	}

	return obj, nil
}

func (r *mockRepo) UpdateMetadata(ctx context.Context, loc ObjectLocation, meta ObjectMetadata) error {
	path := fmt.Sprintf("sj://%s/%s", loc.BucketName, loc.ObjectKey)
	r.objects[path] = ObjectInfo{
		ObjectLocation: loc,
		Metadata:       meta,
	}
	return nil
}

func (r *mockRepo) DeleteMetadata(ctx context.Context, loc ObjectLocation) error {
	path := fmt.Sprintf("sj://%s/%s", loc.BucketName, loc.ObjectKey)
	delete(r.objects, path)
	return nil
}

func (r *mockRepo) QueryMetadata(ctx context.Context, loc ObjectLocation, containsQuery map[string]interface{}, startAfter ObjectLocation, batchSize int) (QueryMetadataResult, error) {
	results := QueryMetadataResult{}
	path := fmt.Sprintf("sj://%s/%s", loc.BucketName, loc.ObjectKey)

	// return all objects whose path starts with the `loc`
	for k, obj := range r.objects {
		if !strings.HasPrefix(k, path) {
			continue
		}

		results.Objects = append(results.Objects, obj)

	}
	return results, nil
}

func (r *mockRepo) MigrateMetadata(ctx context.Context, obj ObjectInfo) (err error) {
	path := fmt.Sprintf("sj://%s/%s", obj.BucketName, obj.ObjectKey)

	existing, ok := r.objects[path]
	if !ok || existing.MetaSearchQueuedAt == nil {
		return ErrNotFound
	}

	obj.MetaSearchQueuedAt = nil
	r.objects[path] = obj
	return nil
}

func (r *mockRepo) GetObjectsForMigration(ctx context.Context, projectID uuid.UUID, startTime *time.Time, migrate ObjectMigrationFunc) error {
	for _, obj := range r.objects {
		if obj.MetaSearchQueuedAt != nil && !migrate(ctx, obj) {
			break
		}
	}
	return nil
}

func (r *mockRepo) updateFromUplink(bucket string, key string, encryptedMetadata string) error {
	path := fmt.Sprintf("sj://%s/enc:%s", bucket, key)

	obj, ok := r.objects[path]
	if !ok {
		return ErrNotFound
	}

	obj.Metadata.EncryptedMetadata = []byte(encryptedMetadata)
	now := time.Now()
	obj.MetaSearchQueuedAt = &now
	r.objects[path] = obj
	return nil
}

func (r *mockRepo) queuedForMigration(bucket string, key string) bool {
	path := fmt.Sprintf("sj://%s/enc:%s", bucket, key)
	return r.objects[path].MetaSearchQueuedAt != nil
}

// Mock authentication

type mockAuthenticator struct{}

func (a *mockAuthenticator) Authenticate(ctx context.Context, r *http.Request) (uuid.UUID, Encryptor, Authorizer, error) {
	return uuid.UUID{}, &mockEncryptor{}, &mockAuthorizer{}, nil
}

type mockAuthorizer struct{}

func (a *mockAuthorizer) Authorize(ctx context.Context, encryptedLocation ObjectLocation, action Action) error {
	return nil
}

type mockEncryptor struct {
	restrictPrefix   string
	comparisonResult map[*mockEncryptor]EncryptorComparisonResult
}

func (e *mockEncryptor) EncryptPath(_ string, p string) (string, error) {
	return "enc:" + p, nil
}

func (e *mockEncryptor) DecryptPath(_ string, p string) (string, error) {
	s := string(p)
	prefix := "enc:" + e.restrictPrefix
	if strings.HasPrefix(s, prefix) {
		return s[4:], nil
	}
	return "", fmt.Errorf("invalid encrypted path: %s", s)
}

func (e *mockEncryptor) EncryptMetadata(bucket string, path string, meta *ObjectMetadata) error {
	buf, err := json.Marshal(meta.ClearMetadata)
	meta.EncryptedMetadataNonce = []byte("nonce")
	meta.EncryptedMetadata = buf
	meta.EncryptedMetadataKey = []byte("key")
	return err
}

func (e *mockEncryptor) DecryptMetadata(bucket string, path string, meta *ObjectMetadata) error {
	var obj map[string]interface{}
	err := json.Unmarshal(meta.EncryptedMetadata, &obj)
	meta.ClearMetadata = obj
	return err
}

func (e *mockEncryptor) Compare(other Encryptor) EncryptorComparisonResult {
	m, ok := other.(*mockEncryptor)
	if !ok {
		return EncryptorComparisonDifferent
	}
	c, ok := e.comparisonResult[m]
	if !ok {
		return EncryptorComparisonDifferent
	}
	return c
}

// Utility functions

const testProjectID = "12345678-1234-5678-9999-1234567890ab"

func testServer() *Server {
	repo := newMockRepo()
	auth := &mockAuthenticator{}
	logger, _ := zap.NewDevelopment()
	server, _ := NewServer(logger, repo, auth, "")
	return server
}

func testRequest(method, path, body string) *http.Request {
	var r *http.Request
	url := "http://localhost" + path
	if body != "" {
		r, _ = http.NewRequest(method, url, strings.NewReader(body))
	} else {
		r, _ = http.NewRequest(method, url, nil)
	}
	r.Header.Set("Authorization", "Bearer testtoken")
	r.Header.Set("X-Project-ID", testProjectID)
	return r
}

func handleRequest(server *Server, method string, path string, body string) *httptest.ResponseRecorder {
	rr := httptest.NewRecorder()
	r := testRequest(method, path, body)
	server.Handler.ServeHTTP(rr, r)
	return rr
}

func assertResponse(t *testing.T, rr *httptest.ResponseRecorder, code int, body string) {
	assert.Equal(t, rr.Code, code)
	if body == "" {
		return
	}

	actualBody, _ := io.ReadAll(rr.Body)
	require.JSONEq(t, body, string(actualBody))
}

// Test utility functions

func TestPageToken(t *testing.T) {
	projectID, _ := uuid.FromString(testProjectID)
	startAfter := ObjectLocation{
		ProjectID:  projectID,
		BucketName: "testbucket",
		ObjectKey:  "foo.txt",
	}
	generatedToken := getPageToken(startAfter)
	parsedToken, err := parsePageToken(generatedToken)
	assert.Nil(t, err)
	assert.Equal(t, parsedToken, startAfter)
}

// Test server

func TestMetaSearchCRUD(t *testing.T) {
	server := testServer()

	// Insert metadata
	rr := handleRequest(server, http.MethodPut, "/metadata/testbucket/foo.txt", `{
		"foo": "456",
		"n": 2,
		"tags": [
			"tag1",
			"tag3"
		]
	}`)
	assert.Equal(t, rr.Code, http.StatusNoContent)

	// Get metadata
	rr = handleRequest(server, http.MethodGet, "/metadata/testbucket/foo.txt", "")
	assertResponse(t, rr, http.StatusOK, `{
		"foo": "456",
		"n": 2,
		"tags": [
			"tag1",
			"tag3"
		]
	}`)

	// Delete metadata
	rr = handleRequest(server, http.MethodDelete, "/metadata/testbucket/foo.txt", "")
	assert.Equal(t, rr.Code, http.StatusNoContent)

	// Get metadata again
	rr = handleRequest(server, http.MethodGet, "/metadata/testbucket/foo.txt", "")
	assertResponse(t, rr, http.StatusNotFound, `{
		"error": "not found"
	}`)
}

func TestMetaSearchQuery(t *testing.T) {
	server := testServer()

	// Insert metadata
	rr := handleRequest(server, http.MethodPut, "/metadata/testbucket/foo.txt", `{
		"foo": "456",
		"n": 1
	}`)
	assert.Equal(t, rr.Code, http.StatusNoContent)

	rr = handleRequest(server, http.MethodPut, "/metadata/testbucket/subdir/bar.txt", `{
		"foo": "456",
		"n": 2
	}`)
	assert.Equal(t, rr.Code, http.StatusNoContent)

	// Query without match => return all results
	rr = handleRequest(server, http.MethodPost, "/metasearch/testbucket", ``)
	assert.Equal(t, rr.Code, http.StatusOK)
	var resp map[string]interface{}
	err := json.NewDecoder(rr.Body).Decode(&resp)
	require.Nil(t, err)
	require.Len(t, resp["results"], 2)

	// Query with key prefix
	rr = handleRequest(server, http.MethodPost, "/metasearch/testbucket", `{
		"keyPrefix": "subdir"
	}`)
	assertResponse(t, rr, http.StatusOK, `{
		"results": [{
			"path": "sj://testbucket/subdir/bar.txt",
			"metadata": {
				"foo": "456",
				"n": 2
			}
		}]
	}`)

	// Query with match only
	rr = handleRequest(server, http.MethodPost, "/metasearch/testbucket", `{
		"match": {
			"foo": "456"
		}
	}`)
	assert.Equal(t, rr.Code, http.StatusOK)
	err = json.NewDecoder(rr.Body).Decode(&resp)
	require.Nil(t, err)
	require.Len(t, resp["results"], 2)

	// Query with match and filter
	rr = handleRequest(server, http.MethodPost, "/metasearch/testbucket", `{
		"match": {
			"foo": "456"
		},
		"filter": "n > `+"`1`"+`"
	}`)
	assertResponse(t, rr, http.StatusOK, `{
		"results": [{
			"path": "sj://testbucket/subdir/bar.txt",
			"metadata": {
				"foo": "456",
				"n": 2
			}
		}]
	}`)

	// Query with match, filter and projection
	rr = handleRequest(server, http.MethodPost, "/metasearch/testbucket", `{
		"match": {
			"foo": "456"
		},
		"filter": "n > `+"`1`"+`",
		"projection": "n"
	}`)
	assertResponse(t, rr, http.StatusOK, `{
		"results": [{
			"path": "sj://testbucket/subdir/bar.txt",
			"metadata": 2
		}]
	}`)
}

func TestMigrationOnGet(t *testing.T) {
	server := testServer()
	repo := server.Repo.(*mockRepo)

	// Insert metadata via HTTP
	rr := handleRequest(server, http.MethodPut, "/metadata/testbucket/foo.txt", `{"foo": 1}`)
	assert.Equal(t, rr.Code, http.StatusNoContent)

	// Update metadata from uplink
	err := repo.updateFromUplink("testbucket", "foo.txt", `{"foo":2}`)
	assert.NoError(t, err)
	assert.True(t, repo.queuedForMigration("testbucket", "foo.txt"))

	// Get metadata => metadata from uplink is returned, object is migrated
	rr = handleRequest(server, http.MethodGet, "/metadata/testbucket/foo.txt", "")
	assertResponse(t, rr, http.StatusOK, `{"foo": 2}`)
	assert.False(t, repo.queuedForMigration("testbucket", "foo.txt"))
}

func TestMigrationOnSearch(t *testing.T) {
	server := testServer()
	repo := server.Repo.(*mockRepo)

	// Insert metadata via HTTP
	rr := handleRequest(server, http.MethodPut, "/metadata/testbucket/foo.txt", `{"foo": 1}`)
	assert.Equal(t, rr.Code, http.StatusNoContent)

	// Update metadata from uplink
	err := repo.updateFromUplink("testbucket", "foo.txt", `{"foo":2}`)
	assert.NoError(t, err)
	assert.True(t, repo.queuedForMigration("testbucket", "foo.txt"))

	// Search metadata => metadata from uplink is returned, object is migrated
	rr = handleRequest(server, http.MethodPost, "/metasearch/testbucket", `{"match":{"foo":"2"}}`)
	assertResponse(t, rr, http.StatusOK, `{
		"results": [{
			"path": "sj://testbucket/foo.txt",
			"metadata": {
				"foo": 2
			}
		}]
	}`)
	assert.False(t, repo.queuedForMigration("testbucket", "foo.txt"))
}
