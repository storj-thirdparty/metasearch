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

// access with unencrypted paths
const accessUnencrypted = "1LC8Zg37pGse66WsfUx4jrfFGebdCmWeK9zEnBG6cGmSWU4hczxwCLrmF1Vf4ofsLuhbXrF84ZZrjoDo1z67fYVLossqgwkwLzSfJvJ3mWHW3iwidxC2ooy4iFQVfpuEBRuVzqzBZfJ7Gkn2oBY1t7G9z5a4HqKZY6jvFvMS1hAAePrXj8iAmMV5cd3eT51Y1U3wSit6kuUv1xv8u9Lr26wwLzV6PMW9x6Nw2q5uYrrS27wEcGpGbDZvqbf2rggoWcfpUVk5xYavHBY8LsipFRV81c6ANu6rB2"

// access with encrypted paths
const accessEncrypted = "1LC8Zg37pGse66WsfUx4jrfFGebdCmWeK9zEnBG6cGmSWU4hczxwCLrmF1Vf4ofsLuhbXrF84ZZrjoDo1z67fYVLossqgwkwLzSfJvJ3mWHW3iwidxC2ooy4iFQVfpuEBRuVzqzBZfJ7Gkn2oBY1t7G9z5a4HqKZY6jvFvMS1hAAePrXj8iAmMV5cd3eT51Y1U3wSit6kuUv1xv8u9Lr26wwLzV6PMW9x6Nw2q5uYrrS27wEcGpGbDZvqbf2rggoWcfpUVk5xYavHBY8LsipFRV81c6ASdiQoB"

// access with encrypted paths, restricted to /subdir
const accessRestricted = "14BqHcZsNkzHK9nssp1HAzUN9f17BU7GvvbqiPGLRYRhVP28Z9dYuCKcWYtcLxEHbT7au3Yvk1gY6fZpX6ZzdhNNiuKwrcHxmpv1SZ6aVZTc8Gdx6R1QxcNG6a1bP6meoBTrUjT5Zp22XrGycjvyK485Kjyrss1e3rdbhN9jjikTCfUBAL25fEXTsx38C9baA9KnqBtEfXcsMR8hpF5pBx8UnNwR2VQDSwRyDKUywfeYGyJ4fMoTtpa6gVAmDokBD6FkdB4udtPZBSqbWPi6ZpVSBogawKHVAGJTSatENBHhYLfu23WyynXimHkf9ZqccAuELnuc66DEhA5HkrmC2GGhL2XjCiANSzXrCCwQpF3BBbtJ3zcuhT782CxRFJ2qE2ZBugt5f77VjYjhxmZSoFZ4bRXLGMTuXaKCNGHMSr792fbjwhAShCGFBZL8EBTqa5MsjErND2i5jKMCAmFY3QcmG8RBAXjUxcq"

// access with encrypted paths, restricted to /subdir, /subdir2, /subdir3, /subdir4
const accessRestricted2 = "17djgjJ82HAdckLUACZUirCNFPZG8ezw9a2CsfzU8UWGF4TtXpcp17nYbjvKMXUurPaWTnwy2E4vFBbYdGKa5iTyLS9KnHVQz1ESQKLajVQ4ypMSAUfgZZ5P5bSAJUbYyma8GNEER9FyVJEX3Bu6XRRpwsecumuEYsbBrspTn1iPwNDQT3fMiuE6tU45BSFBkFESUYqAZKs8ghcS14C2GLD6F8YSdNw9CQj6pQfnP6qbx43uFwBpW1vfi5cMSCmavY2egEcm4R35eDkiuT4ZvCNpW3YB9nByMFiv59CGLBG2jjUktEXrrNGbG4QmP4WXsnXhHtG8bYZGxscSF8AiXngG3bpNF3KBVCE1po1tL9uUGMT31h7hH4BMY165Z3n6Cdt9BfApiNWbbAEx4SEtwrXHisvZFaBEiDMjdXNkkgKKytoMLDU4FzoQoaj1Lr64aLLujoYvkiF3TeofQykupgWyMfwEZ29odcn9TruCaCnjfcnSuRf3dQFhczBmXEZHWwZ7otBzmxjMFKUmtdDXyNjjnJgddLAtQBFi53tRAVN1n2MoNMnKbCte5DAPNVNWcCBPzirX6brAGV6JkeGWqW4HX7UuU6ttxKnCj7nvo4TxZmqfEjoxw6CkGkttTEWbMQETjg3eepWpbWkmKvGFfij8bze7V6zg43SeHzGvFxKgzxepQkKnyncZM14CTKXCwUyuNWJwRrtAiTAjgNi159L7w4tfrs28ssm7gwTWHGcWViQLyxTn9DTuBQtgFRWwZVLkpeRjKPCyut8rSRKLuEPCR2yG8ERALNMSpMdmeQ3LfUMYcAfTJeBsgsRwtK7yvz25dSNdURxzPiYJmTBrCrxBFKhTJSNvudYLUCC11mLWxmKBij8SWPj5zEca1hTLJgZMVq4Hn1pX8CYbUC3c3nXV4g1N2WFTkFmMdbf3Cnw3r96QQbHp7Eyu8nx931kr75GxQVTWjmT37fxq15anww6iokRrqsKdLxTCWsuJf9KtUe6yHw3dWGZ6jF7ZqfvGp5o5gVDwhLNcNBZzu2uXRdaKkxafSp5V1kTeQs4DAteyPkxdfmz759JTX5aYX5gQk6YW23RG"

const testBucket = "encrypted"
const testPath = "foo.txt"

func TestPathEncryption(t *testing.T) {
	access, err := uplink.ParseAccess(accessEncrypted)
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
	access, err := uplink.ParseAccess(accessEncrypted)
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

func TestUplinkAccessComparison(t *testing.T) {
	accUnencrypted, err := uplink.ParseAccess(accessUnencrypted)
	require.NoError(t, err)
	accUnencrypted2, err := uplink.ParseAccess(accessUnencrypted)
	require.NoError(t, err)
	accEncrypted, err := uplink.ParseAccess(accessEncrypted)
	require.NoError(t, err)
	accRestricted, err := uplink.ParseAccess(accessRestricted)
	require.NoError(t, err)
	accRestricted2, err := uplink.ParseAccess(accessRestricted2)
	require.NoError(t, err)

	unencrypted := NewUplinkEncryptor(accUnencrypted)
	unencrypted2 := NewUplinkEncryptor(accUnencrypted2)
	encrypted := NewUplinkEncryptor(accEncrypted)
	restricted := NewUplinkEncryptor(accRestricted)
	restricted2 := NewUplinkEncryptor(accRestricted2)

	require.Equal(t, EncryptorComparisonIdentical, unencrypted.Compare(unencrypted))
	require.Equal(t, EncryptorComparisonIdentical, unencrypted.Compare(unencrypted2))
	require.Equal(t, EncryptorComparisonDifferent, unencrypted.Compare(encrypted))
	require.Equal(t, EncryptorComparisonDifferent, unencrypted.Compare(restricted))
	require.Equal(t, EncryptorComparisonDifferent, encrypted.Compare(restricted))
	require.Equal(t, EncryptorComparisonSubset, restricted.Compare(restricted2))
	require.Equal(t, EncryptorComparisonSuperset, restricted2.Compare(restricted))
}

func TestEncryptorRepository(t *testing.T) {
	e1 := &mockEncryptor{restrictPrefix: "1/"}
	e2 := &mockEncryptor{restrictPrefix: "2/"}
	e1identical := &mockEncryptor{restrictPrefix: "1/", comparisonResult: map[*mockEncryptor]EncryptorComparisonResult{
		e1: EncryptorComparisonIdentical,
	}}
	e2subset := &mockEncryptor{restrictPrefix: "2/", comparisonResult: map[*mockEncryptor]EncryptorComparisonResult{
		e2: EncryptorComparisonSubset,
	}}
	e2superset := &mockEncryptor{restrictPrefix: "2/", comparisonResult: map[*mockEncryptor]EncryptorComparisonResult{
		e2: EncryptorComparisonSuperset,
	}}

	r := NewEncryptorRepository()

	// Do not add identical encryptors and subsets
	require.True(t, r.AddEncryptor(e1))
	require.True(t, r.AddEncryptor(e2))
	require.False(t, r.AddEncryptor(e1identical))
	require.False(t, r.AddEncryptor(e2subset))

	// Superset replaces an existing item
	require.Len(t, r.encryptors, 2)
	require.True(t, r.AddEncryptor(e2superset))
	require.Len(t, r.encryptors, 2)

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

	// Remove unused encryptors => only the second one remains
	require.True(t, r.CheckEncryptors(1))
	require.Len(t, r.encryptors, 1)
	require.Equal(t, "2/", r.encryptors[0].encryptor.(*mockEncryptor).restrictPrefix)
}
