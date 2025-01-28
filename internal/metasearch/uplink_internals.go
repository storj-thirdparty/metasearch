package metasearch

import (
	_ "unsafe" // for go:linkname

	"storj.io/common/grant"
	"storj.io/common/macaroon"
	"storj.io/uplink"
)

// accessGetAPIKey exposes Access.getAPIKey in uplink.
//
//go:linkname accessGetAPIKey storj.io/uplink.access_getAPIKey
func accessGetAPIKey(*uplink.Access) *macaroon.APIKey

// accessGetEncAccess exposes Access.getEncAccess in uplink.
//
// NB: this is used with linkname in internal/expose.
// It needs to be updated when this is updated.
//
//lint:ignore U1000, used with linkname
//nolint:unused
//go:linkname accessGetEncAccess storj.io/uplink.access_getEncAccess
func accessGetEncAccess(access *uplink.Access) *grant.EncryptionAccess
