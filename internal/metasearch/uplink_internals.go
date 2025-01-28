package metasearch

import (
	_ "unsafe" // for go:linkname

	"storj.io/common/macaroon"
	"storj.io/uplink"
)

// accessGetAPIKey exposes Access.getAPIKey.
//
//go:linkname accessGetAPIKey storj.io/uplink.access_getAPIKey
func accessGetAPIKey(*uplink.Access) *macaroon.APIKey
