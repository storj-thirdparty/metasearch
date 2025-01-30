// Copyright (C) 2024 Storj Labs, Inc.
// See LICENSE for copying information.

package metasearch

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"storj.io/common/uuid"
	"storj.io/storj/satellite"

	"storj.io/uplink"
)

// Auth authenticates HTTP requests for metasearch
type Auth interface {
	Authenticate(ctx context.Context, r *http.Request) (projectID uuid.UUID, encryptor Encryptor, err error)
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

func (a *HeaderAuth) Authenticate(ctx context.Context, r *http.Request) (projectID uuid.UUID, encryptor Encryptor, err error) {
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

	projectID, err = a.getProjectID(ctx, access)
	if err != nil {
		return
	}

	encryptor = NewUplinkEncryptor(access)

	return
}

// GetPublicID gets the public project ID for the given access grant.
func (a *HeaderAuth) getProjectID(ctx context.Context, access *uplink.Access) (id uuid.UUID, err error) {
	accessKey := accessGetAPIKey(access)
	info, err := a.db.Console().APIKeys().GetByHead(ctx, accessKey.Head())
	if err != nil {
		return uuid.UUID{}, fmt.Errorf("%w: cannot find project by API key", ErrAuthorizationFailed)
	}

	return info.ProjectID, nil
}
