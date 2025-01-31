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

	"github.com/stretchr/testify/require"
	"github.com/zeebo/assert"
	"go.uber.org/zap"

	"storj.io/common/uuid"
	"storj.io/storj/cmd/uplink/ulloc"
)

// Mock repository

type mockRepo struct {
	metadata map[string]ObjectMetadata
}

func newMockRepo() *mockRepo {
	return &mockRepo{
		metadata: make(map[string]ObjectMetadata),
	}
}

func (r *mockRepo) GetMetadata(ctx context.Context, loc ObjectLocation) (ObjectInfo, error) {
	path := fmt.Sprintf("sj://%s/%s", loc.BucketName, loc.ObjectKey)
	m, ok := r.metadata[path]
	if !ok {
		return ObjectInfo{}, ErrNotFound
	}
	return ObjectInfo{
		Metadata: m,
	}, nil
}

func (r *mockRepo) UpdateMetadata(ctx context.Context, loc ObjectLocation, meta ObjectMetadata) error {
	path := fmt.Sprintf("sj://%s/%s", loc.BucketName, loc.ObjectKey)
	r.metadata[path] = meta
	return nil
}

func (r *mockRepo) DeleteMetadata(ctx context.Context, loc ObjectLocation) error {
	path := fmt.Sprintf("sj://%s/%s", loc.BucketName, loc.ObjectKey)
	delete(r.metadata, path)
	return nil
}

func (r *mockRepo) QueryMetadata(ctx context.Context, loc ObjectLocation, containsQuery map[string]interface{}, startAfter ObjectLocation, batchSize int) (QueryMetadataResult, error) {
	results := QueryMetadataResult{}
	path := fmt.Sprintf("sj://%s/%s", loc.BucketName, loc.ObjectKey)

	// return all objects whose path starts with the `loc`
	for k, m := range r.metadata {
		if !strings.HasPrefix(k, path) {
			continue
		}

		objloc, _ := ulloc.Parse(k)
		bucket, key, _ := objloc.RemoteParts()
		results.Objects = append(results.Objects, ObjectInfo{
			ObjectLocation: ObjectLocation{
				ProjectID:  loc.ProjectID,
				BucketName: bucket,
				ObjectKey:  key,
			},
			Metadata: m,
		})

	}
	return results, nil
}

// Mock authentication

type mockAuth struct{}

func (a *mockAuth) Authenticate(ctx context.Context, r *http.Request) (uuid.UUID, Encryptor, error) {
	return uuid.UUID{}, &mockEncryptor{}, nil
}

type mockEncryptor struct{}

func (e *mockEncryptor) EncryptPath(_ string, p string) (string, error) {
	return "enc:" + p, nil
}

func (e *mockEncryptor) DecryptPath(_ string, p string) (string, error) {
	s := string(p)
	if strings.HasPrefix(s, "enc:") {
		return s[4:], nil
	}
	return "", fmt.Errorf("invalid encrypted path: %s", s)
}

func (e *mockEncryptor) EncryptMetadata(bucket string, path string, meta *ObjectMetadata) error {
	buf, err := json.Marshal(meta)
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

// Utility functions

const testProjectID = "12345678-1234-5678-9999-1234567890ab"

func testServer() *Server {
	repo := newMockRepo()
	auth := &mockAuth{}
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
