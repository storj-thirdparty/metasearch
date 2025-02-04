// Copyright (C) 2025 Storj Labs, Inc.
// See LICENSE for copying information.

package metasearch

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"storj.io/common/uuid"
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
	meta := ObjectMetadata{
		ClearMetadata: clearMeta,
	}

	err = encryptor.EncryptMetadata(testBucket, testPath, &meta)
	require.NoError(t, err)

	meta.ClearMetadata = nil
	err = encryptor.DecryptMetadata(testBucket, testPath, &meta)
	require.NoError(t, err)

	clearMeta2 := meta.ClearMetadata
	require.Len(t, clearMeta2, 1)
	require.Equal(t, true, clearMeta2["foo"])
}

func TestEncryptorRepository(t *testing.T) {
	e1 := &mockEncryptor{restrictPrefix: "1/"}
	e2 := &mockEncryptor{restrictPrefix: "2/"}

	r := NewEncryptorRepository()
	require.True(t, r.AddEncryptor(e1))
	require.True(t, r.AddEncryptor(e2))

	// Encrypt with e2
	projectID, _ := uuid.New()
	obj := &ObjectInfo{
		ObjectLocation: ObjectLocation{
			ProjectID:  projectID,
			BucketName: "mybucket",
			ObjectKey:  "2/foo.txt",
		},
		Metadata: ObjectMetadata{
			ClearMetadata: map[string]interface{}{"foo": "bar"},
		},
	}
	encryptedPath, err := e2.EncryptPath(obj.BucketName, obj.ObjectKey)
	require.NoError(t, err)
	obj.ObjectKey = encryptedPath

	err = e2.EncryptMetadata(obj.BucketName, obj.ObjectKey, &obj.Metadata)
	require.NoError(t, err)
	obj.Metadata.ClearMetadata = nil

	// Decrypt metadata with repository
	obj.Metadata.ClearMetadata = nil
	clearPath, meta, err := r.DecryptMetadata(obj)

	require.Equal(t, clearPath, "2/foo.txt")
	metaJSON, _ := json.Marshal(meta.ClearMetadata)
	require.JSONEq(t, `{"foo":"bar"}`, string(metaJSON))
}
