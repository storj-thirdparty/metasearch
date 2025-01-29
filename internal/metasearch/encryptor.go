// Copyright (C) 2025 Storj Labs, Inc.
// See LICENSE for copying information.

package metasearch

import (
	"encoding/hex"
	"fmt"

	"storj.io/common/encryption"
	"storj.io/common/paths"
	"storj.io/uplink"
)

// PathEncryptor provides a simple interface to encrypt and decrypt paths
type PathEncryptor interface {
	EncryptPath(bucket string, path string) (string, error)
	DecryptPath(bucket string, path string) (string, error)
}

// UplinkPathEncryptor encrypts/decrypts paths using the uplink library.
type UplinkPathEncryptor struct {
	store *encryption.Store
}

// NewUplinkPathEncryptor createa a new NewUplinkPathEncryptor instance
func NewUplinkPathEncryptor(access *uplink.Access) *UplinkPathEncryptor {
	encAccess := accessGetEncAccess(access)
	return &UplinkPathEncryptor{
		store: encAccess.Store,
	}
}

func (e *UplinkPathEncryptor) EncryptPath(bucket string, path string) (string, error) {
	p := paths.NewUnencrypted(path)
	encPath, err := encryption.EncryptPath(bucket, p, e.store.GetDefaultPathCipher(), e.store)
	if err != nil {
		return "", fmt.Errorf("%w: cannot encrypt path: %s", ErrInternalError, path)
	}
	fmt.Printf("%s => %v\n", path, hex.EncodeToString([]byte(encPath.Raw())))
	return encPath.Raw(), nil
}

func (e *UplinkPathEncryptor) DecryptPath(bucket string, path string) (string, error) {
	p, err := encryption.DecryptPath(bucket, paths.NewEncrypted(path), e.store.GetDefaultPathCipher(), e.store)
	if err != nil {
		return "", fmt.Errorf("%w: cannot decrypt path", ErrInternalError)
	}
	return p.String(), err
}
