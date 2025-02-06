// Copyright (C) 2024 Storj Labs, Inc.
// See LICENSE for copying information.

package metasearch

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"storj.io/common/encryption"
	"storj.io/common/macaroon"
	"storj.io/common/uuid"
	"storj.io/storj/satellite"
	"storj.io/storj/satellite/console"

	"storj.io/uplink"
)

// Authenticator authenticates HTTP requests for metasearch
type Authenticator interface {
	Authenticate(ctx context.Context, r *http.Request) (projectID uuid.UUID, encryptor Encryptor, authorizer Authorizer, err error)
}

// HeaderAuth authenticates metasearch HTTP requests based on the Authorization header
type HeaderAuth struct {
	db satellite.DB
}

func NewHeaderAuth(db satellite.DB) *HeaderAuth {
	return &HeaderAuth{
		db: db,
	}
}

func (a *HeaderAuth) Authenticate(ctx context.Context, r *http.Request) (projectID uuid.UUID, encryptor Encryptor, authorizer Authorizer, err error) {
	// Parse authorization header
	hdr := r.Header.Get("Authorization")
	if hdr == "" {
		err = fmt.Errorf("%w: missing authorization header", ErrAuthorizationFailed)
		return
	}

	// Check for valid authorization
	if !strings.HasPrefix(hdr, "Bearer ") {
		err = fmt.Errorf("%w: invalid authorization header", ErrAuthorizationFailed)
		return
	}

	// Parse API token
	rawAccess := strings.TrimPrefix(hdr, "Bearer ")
	access, err := uplink.ParseAccess(rawAccess)
	if err != nil {
		err = fmt.Errorf("%w: cannot parse access token: %v", ErrAuthorizationFailed, err)
		return
	}

	config := uplink.Config{
		UserAgent: "metasearch",
	}

	project, err := config.OpenProject(ctx, access)
	if err != nil {
		err = fmt.Errorf("%w: cannot open project: %v", ErrAuthorizationFailed, err)
		return
	}
	defer project.Close()

	var keyInfo *console.APIKeyInfo
	apiKey := accessGetAPIKey(access)
	keyInfo, err = a.db.Console().APIKeys().GetByHead(ctx, apiKey.Head())
	if err != nil {
		err = fmt.Errorf("%w: cannot find project by API key", ErrAuthorizationFailed)
		return
	}
	projectID = keyInfo.ProjectID

	encryptor = NewUplinkEncryptor(access)
	authorizer = NewAPIKeyAuthorizer(access, apiKey, keyInfo)

	return
}

// Authorizer authorizes client requests for metasearch
type Authorizer interface {
	Authorize(ctx context.Context, encryptedLocation ObjectLocation, action Action) error
}

// Action describes an action performed in the metainfo database.
type Action macaroon.ActionType

const (
	ActionReadMetadata   = Action(macaroon.ActionRead)
	ActionQueryMetadata  = Action(macaroon.ActionRead)
	ActionWriteMetadata  = Action(macaroon.ActionWrite)
	ActionDeleteMetadata = Action(macaroon.ActionWrite)
)

// APIKeyAuthorizer authorizes requests using storj macaroons.
type APIKeyAuthorizer struct {
	access  *uplink.Access
	store   *encryption.Store
	apiKey  *macaroon.APIKey
	keyInfo *console.APIKeyInfo
}

// NewAPIKeyAuthorizer creates an APIKey based authorizer.
func NewAPIKeyAuthorizer(access *uplink.Access, apiKey *macaroon.APIKey, keyInfo *console.APIKeyInfo) *APIKeyAuthorizer {
	encAccess := accessGetEncAccess(access)

	return &APIKeyAuthorizer{
		access:  access,
		store:   encAccess.Store,
		apiKey:  apiKey,
		keyInfo: keyInfo,
	}
}

func (a *APIKeyAuthorizer) Authorize(ctx context.Context, encryptedLocation ObjectLocation, action Action) error {
	if len(encryptedLocation.ObjectKey) == 0 && a.store.GetDefaultKey() == nil {
		return fmt.Errorf("%w: the access token does not have permission for the whole bucket", ErrAuthorizationFailed)
	}

	// TODO: handle revocations
	err := a.apiKey.Check(ctx, a.keyInfo.Secret, a.keyInfo.Version, macaroon.Action{
		Op:            macaroon.ActionType(action),
		Bucket:        []byte(encryptedLocation.BucketName),
		EncryptedPath: []byte(encryptedLocation.ObjectKey),
		Time:          time.Now(),
	}, nil)

	if err != nil {
		return fmt.Errorf("%w: %v", ErrAuthorizationFailed, err)
	}
	return nil
}
