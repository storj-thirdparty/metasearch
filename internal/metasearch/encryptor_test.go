// Copyright (C) 2025 Storj Labs, Inc.
// See LICENSE for copying information.

package metasearch

import (
	"testing"

	"github.com/stretchr/testify/require"

	"storj.io/uplink"
)

const testAccess = `1LC8Zg37pGse66WsfUx4jrfFGebdCmWeK9zEnBG6cGmSWU4hczxwCLrmF1Vf4ofsLuhbXrF84ZZrjoDo1z67fYVLossqgwkwLzSfJvJ3mWHW3iwidxC2ooy4iFQVfpuEBRuVzqzBZfJ7Gkn2oBY1t7G9z5a4HqKZY6jvFvMS1hAAePrXj8iAmMV5cd3eT51Y1U3wSit6kuUv1xv8u9Lr26wwLzV6PMW9x6Nw2q5uYrrS27wEcGpGbDZvqbf2rggoWcfpUVk5xYavHBY8LsipFRV81c6ASdiQoB`
const testBucket = "encrypted"
const testPath = "foo.txt"

func TestPathEncryption(t *testing.T) {
	access, err := uplink.ParseAccess(testAccess)
	require.NoError(t, err)
	encryptor := NewUplinkEncryptor(access)

	clearPath := "subdir/foo.txt"

	encPath, err := encryptor.EncryptPath(testBucket, clearPath)
	require.NoError(t, err)

	clearPath2, err := encryptor.DecryptPath(testBucket, encPath)
	require.NoError(t, err)

	require.Equal(t, clearPath, clearPath2)
}

func TestMetadataEncryption(t *testing.T) {
	access, err := uplink.ParseAccess(testAccess)
	require.NoError(t, err)
	encryptor := NewUplinkEncryptor(access)

	clearMeta := map[string]interface{}{"foo": true}

	nonce, encMeta, encKey, err := encryptor.EncryptMetadata(testBucket, testPath, clearMeta)
	require.NoError(t, err)

	clearMeta2, err := encryptor.DecryptMetadata(testBucket, testPath, nonce, encMeta, encKey)
	require.NoError(t, err)

	require.Len(t, clearMeta2, 1)
	require.Equal(t, true, clearMeta2["foo"])
}
