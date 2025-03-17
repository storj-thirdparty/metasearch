// Copyright (C) 2024 Storj Labs, Inc.
// See LICENSE for copying information.

package metasearch

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/jmespath/go-jmespath"
	"go.uber.org/zap"

	"storj.io/common/uuid"
)

// Server implements the REST API for metadata search.
type Server struct {
	Logger   *zap.Logger
	Repo     MetaSearchRepo
	Auth     Authenticator
	Endpoint string
	Handler  http.Handler
	Migrator *ObjectMigrator
}

// BaseRequest contains common fields for all requests.
type BaseRequest struct {
	ProjectID  uuid.UUID      `json:"-"`
	Location   ObjectLocation `json:"-"`
	Authorizer Authorizer     `json:"-"`

	Encryptor         Encryptor      `json:"-"`
	EncryptedLocation ObjectLocation `json:"-"`
}

const defaultBatchSize = 100
const maxBatchSize = 1000
const migrationTimeout = 10 * time.Second

// GetRequest contains fields for a get request.
type GetRequest struct {
	BaseRequest
}

// SearchRequest contains fields for a view or search request.
type SearchRequest struct {
	BaseRequest

	KeyPrefix  string                 `json:"keyPrefix,omitempty"`
	Match      map[string]interface{} `json:"match,omitempty"`
	Filter     string                 `json:"filter,omitempty"`
	Projection string                 `json:"projection,omitempty"`

	BatchSize int    `json:"batchSize,omitempty"`
	PageToken string `json:"pageToken,omitempty"`

	startAfter     ObjectLocation
	filterPath     *jmespath.JMESPath
	projectionPath *jmespath.JMESPath
}

// SearchResponse contains fields for a view or search response.
type SearchResponse struct {
	Results   []SearchResult `json:"results"`
	PageToken string         `json:"pageToken,omitempty"`
}

// SearchResult contains fields for a single search result.
type SearchResult struct {
	Path     string      `json:"path"`
	Metadata interface{} `json:"metadata"`
}

// NewServer creates a new metasearch server process.
func NewServer(log *zap.Logger, repo MetaSearchRepo, auth Authenticator, endpoint string) (*Server, error) {
	s := &Server{
		Logger:   log,
		Repo:     repo,
		Auth:     auth,
		Endpoint: endpoint,
		Migrator: NewObjectMigrator(log, repo),
	}

	router := mux.NewRouter()

	// CRUD operations
	router.HandleFunc("/metadata/{bucket}/{key:.*}", s.HandleGet).Methods(http.MethodGet)
	router.HandleFunc("/metadata/{bucket}/{key:.*}", s.HandleUpdate).Methods(http.MethodPut)
	router.HandleFunc("/metadata/{bucket}/{key:.*}", s.HandleDelete).Methods(http.MethodDelete)

	// Search
	router.HandleFunc("/metasearch/{bucket}", s.HandleQuery).Methods(http.MethodPost)

	s.Handler = router

	return s, nil
}

// Run starts the metasearch server.
func (s *Server) Run() error {
	s.Migrator.Start()
	return http.ListenAndServe(s.Endpoint, s.Handler)
}

func (s *Server) validateRequest(ctx context.Context, r *http.Request, baseRequest *BaseRequest, body interface{}) error {
	// Parse authorization header
	projectID, encryptor, authorizer, err := s.Auth.Authenticate(ctx, r)
	if err != nil {
		return err
	}
	baseRequest.Authorizer = authorizer

	s.Migrator.AddProject(ctx, projectID, encryptor)
	if !s.Migrator.WaitForProject(ctx, projectID, migrationTimeout) {
		return fmt.Errorf("%w: metadata is being indexed", ErrMetadataMigrationInProgress)
	}

	// Decode request body
	if body != nil && r.Body != nil {
		if err = json.NewDecoder(r.Body).Decode(body); err != nil {
			return fmt.Errorf("%w: error decoding request body: %w", ErrBadRequest, err)
		}
	}

	// Set location
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	key := vars["key"]
	baseRequest.Encryptor = encryptor
	baseRequest.Location = ObjectLocation{
		ProjectID:  projectID,
		BucketName: bucket,
		ObjectKey:  key,
	}

	// Encrypt location
	encKey, err := encryptor.EncryptPath(bucket, key)
	if err != nil {
		return fmt.Errorf("%w: the access token does not have permission for path '%s'", ErrAuthorizationFailed, key)
	}

	baseRequest.EncryptedLocation = ObjectLocation{
		ProjectID:  projectID,
		BucketName: bucket,
		ObjectKey:  encKey,
	}

	return nil
}

func (s *Server) HandleGet(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var request BaseRequest

	err := s.validateRequest(ctx, r, &request, nil)
	if err != nil {
		s.errorResponse(w, err)
		return
	}

	err = request.Authorizer.Authorize(ctx, request.EncryptedLocation, ActionReadMetadata)
	if err != nil {
		s.errorResponse(w, err)
		return
	}

	obj, err := s.Repo.GetMetadata(ctx, request.EncryptedLocation)
	if err != nil {
		s.errorResponse(w, err)
		return
	}

	if obj.MetaSearchQueuedAt != nil {
		_ = s.Migrator.MigrateObject(ctx, &obj)
	}
	s.jsonResponse(w, http.StatusOK, obj.Metadata.ClearMetadata)
}

// HandleQuery handles a metadata view or search request.
func (s *Server) HandleQuery(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var request SearchRequest
	var result SearchResponse

	err := s.validateSearchRequest(ctx, r, &request)
	if err != nil {
		s.errorResponse(w, err)
		return
	}

	err = request.Authorizer.Authorize(ctx, request.EncryptedLocation, ActionQueryMetadata)
	if err != nil {
		s.errorResponse(w, err)
		return
	}

	result, err = s.searchMetadata(ctx, &request)
	if err != nil {
		s.errorResponse(w, err)
		return
	}

	s.jsonResponse(w, http.StatusOK, result)
}

func (s *Server) validateSearchRequest(ctx context.Context, r *http.Request, request *SearchRequest) error {
	err := s.validateRequest(ctx, r, &request.BaseRequest, request)
	if err != nil {
		return err
	}

	// Validate match query
	if request.Match == nil {
		request.Match = make(map[string]interface{})
	}

	// Validate batch size
	if request.BatchSize <= 0 || request.BatchSize > maxBatchSize {
		request.BatchSize = defaultBatchSize
	}

	// Validate pageToken
	if request.PageToken != "" {
		request.startAfter, err = parsePageToken(request.PageToken)
		if err != nil {
			return err
		}
	}

	// Override key by KeyPrefix parameter
	keyPrefix := normalizeKeyPrefix(request.KeyPrefix)
	if keyPrefix != "" {
		encPrefix, err := request.Encryptor.EncryptPath(request.Location.BucketName, keyPrefix)
		if err != nil {
			return fmt.Errorf("%w: the access token does not have permission for path '%s'", ErrAuthorizationFailed, keyPrefix)
		}
		request.Location.ObjectKey = keyPrefix + "/"
		request.EncryptedLocation.ObjectKey = string(encPrefix) + "/"
	}

	// Validate filter
	if request.Filter != "" {
		request.filterPath, err = jmespath.Compile(request.Filter)
		if err != nil {
			return jmespathError("invalid filter expression", err)
		}
	}

	// Validate projection
	if request.Projection != "" {
		request.projectionPath, err = jmespath.Compile(request.Projection)
		if err != nil {
			return jmespathError("invalid projection expression", err)
		}
	}

	return nil
}

func (s *Server) searchMetadata(ctx context.Context, request *SearchRequest) (response SearchResponse, err error) {
	searchResult, err := s.Repo.QueryMetadata(ctx, request.EncryptedLocation, request.Match, request.startAfter, request.BatchSize)
	if err != nil {
		return
	}

	// Extract keys
	var metadata map[string]interface{}
	var projectedMetadata interface{}

	var shouldInclude bool
	response.Results = make([]SearchResult, 0)
	for _, obj := range searchResult.Objects {
		// Decode path
		decodedPath, encryptorErr := request.Encryptor.DecryptPath(request.Location.BucketName, string(obj.ObjectKey))
		if encryptorErr != nil {
			continue
		}

		// Apply filter
		metadata = obj.Metadata.ClearMetadata
		shouldInclude, err = s.filterMetadata(request, metadata)
		if err != nil {
			return
		}
		if !shouldInclude {
			continue
		}

		// Apply projection
		if request.projectionPath != nil {
			projectedMetadata, err = request.projectionPath.Search(metadata)
		} else {
			projectedMetadata = metadata
		}
		if err != nil {
			return
		}

		response.Results = append(response.Results, SearchResult{
			Path:     fmt.Sprintf("sj://%s/%s", obj.BucketName, decodedPath),
			Metadata: projectedMetadata,
		})
	}

	// Determine page token
	if len(searchResult.Objects) >= request.BatchSize {
		last := searchResult.Objects[len(searchResult.Objects)-1]
		response.PageToken = getPageToken(last.ObjectLocation)
	}

	return
}

func (s *Server) filterMetadata(request *SearchRequest, metadata map[string]interface{}) (bool, error) {
	if request.Filter == "" {
		return true, nil
	}

	// Evaluate JMESPath filter
	result, err := request.filterPath.Search(metadata)
	if err != nil {
		return false, fmt.Errorf("%w: %v", ErrBadRequest, err)
	}

	// Check if result is a boolean
	if b, ok := result.(bool); ok {
		return b, nil
	}

	// Check if result is nil
	if result == nil {
		return false, nil
	}

	// Include metadata if result is not nil or false
	return true, nil
}

// HandleUpdate handles a metadata update request.
func (s *Server) HandleUpdate(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var request BaseRequest
	var metadata map[string]interface{}

	err := s.validateRequest(ctx, r, &request, &metadata)
	if err != nil {
		s.errorResponse(w, err)
		return
	}

	err = request.Authorizer.Authorize(ctx, request.EncryptedLocation, ActionWriteMetadata)
	if err != nil {
		s.errorResponse(w, err)
		return
	}

	meta := ObjectMetadata{
		ClearMetadata: metadata,
	}

	err = request.Encryptor.EncryptMetadata(request.Location.BucketName, request.Location.ObjectKey, &meta)
	if err != nil {
		s.errorResponse(w, fmt.Errorf("%w: cannot encrypt metadata", ErrBadRequest))
		return
	}

	err = s.Repo.UpdateMetadata(ctx, request.EncryptedLocation, meta)
	if err != nil {
		s.errorResponse(w, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// HandleDelete handles a metadata delete request.
func (s *Server) HandleDelete(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var request BaseRequest

	err := s.validateRequest(ctx, r, &request, nil)
	if err != nil {
		s.errorResponse(w, err)
		return
	}

	err = request.Authorizer.Authorize(ctx, request.EncryptedLocation, ActionDeleteMetadata)
	if err != nil {
		s.errorResponse(w, err)
		return
	}

	err = s.Repo.DeleteMetadata(ctx, request.EncryptedLocation)
	if err != nil {
		s.errorResponse(w, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) jsonResponse(w http.ResponseWriter, status int, body interface{}) {
	jsonBytes, err := json.Marshal(body)
	if err != nil {
		s.errorResponse(w, fmt.Errorf("%w: %v", ErrInternalError, err))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	w.Write(jsonBytes)
}

func (s *Server) errorResponse(w http.ResponseWriter, err error) {
	s.Logger.Warn("error during API request", zap.Error(err))

	var e *ErrorResponse
	if !errors.As(err, &e) {
		e = ErrInternalError
	}

	resp, _ := json.Marshal(e)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(e.StatusCode)
	w.Write([]byte(resp))
}

func getPageToken(obj ObjectLocation) string {
	q := url.Values{}
	q.Set("projectID", obj.ProjectID.String())
	q.Set("bucketName", obj.BucketName)
	q.Set("objectKey", obj.ObjectKey)
	q.Set("version", strconv.FormatInt(obj.Version, 10))

	return base64.StdEncoding.EncodeToString([]byte(q.Encode()))
}

func parsePageToken(s string) (ObjectLocation, error) {
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return ObjectLocation{}, fmt.Errorf("invalid page token: %w", ErrBadRequest)
	}

	q, err := url.ParseQuery(string(b))
	if err != nil {
		return ObjectLocation{}, fmt.Errorf("invalid params in page token: %w", ErrBadRequest)
	}

	projectID, err := uuid.FromString(q.Get("projectID"))
	if err != nil {
		return ObjectLocation{}, fmt.Errorf("invalid projectID in page token: %w", ErrBadRequest)
	}

	bucketName := q.Get("bucketName")
	if bucketName == "" {
		return ObjectLocation{}, fmt.Errorf("invalid bucketName in page token: %w", ErrBadRequest)
	}

	objectKey := q.Get("objectKey")
	if objectKey == "" {
		return ObjectLocation{}, fmt.Errorf("invalid objectKey in page token: %w", ErrBadRequest)
	}

	version, err := strconv.ParseInt(q.Get("version"), 10, 64)
	if err != nil {
		return ObjectLocation{}, fmt.Errorf("invalid version in page token: %w", ErrBadRequest)
	}

	return ObjectLocation{
		ProjectID:  projectID,
		BucketName: bucketName,
		ObjectKey:  objectKey,
		Version:    version,
	}, nil
}

func jmespathError(msg string, err error) error {
	var syntaxError jmespath.SyntaxError
	if errors.As(err, &syntaxError) {
		return &ErrorResponse{
			StatusCode: 400,
			Message:    fmt.Sprintf("%s: %s\n\n%s", msg, err.Error(), syntaxError.HighlightLocation()),
		}
	}
	return fmt.Errorf("%w: %s", ErrBadRequest, msg)
}
