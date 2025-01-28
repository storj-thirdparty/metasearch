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
	EncryptPath(bucket string, path string) ([]byte, error)
	DecryptPath(bucket string, path []byte) (string, error)
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

func (e *UplinkPathEncryptor) EncryptPath(bucket string, path string) ([]byte, error) {
	p := paths.NewUnencrypted(path)
	encPath, err := encryption.EncryptPath(bucket, p, e.store.GetDefaultPathCipher(), e.store)
	if err != nil {
		return nil, fmt.Errorf("%w: cannot encrypt path: %s", ErrInternalError, path)
	}
	fmt.Printf("%s => %v\n", path, hex.EncodeToString([]byte(encPath.Raw())))
	return []byte(encPath.Raw()), nil
}

func (e *UplinkPathEncryptor) DecryptPath(bucket string, path []byte) (string, error) {
	p, err := encryption.DecryptPath(bucket, paths.NewEncrypted(string(path)), e.store.GetDefaultPathCipher(), e.store)
	if err != nil {
		return "", fmt.Errorf("%w: cannot decrypt path", ErrInternalError)
	}
	return string(p.String()), err
}
