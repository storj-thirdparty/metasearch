// Copyright (C) 2025 Storj Labs, Inc.
// See LICENSE for copying information.

package metasearch

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"storj.io/common/encryption"
	"storj.io/common/memory"
	"storj.io/common/paths"
	"storj.io/common/pb"
	"storj.io/common/storj"
	"storj.io/uplink"
)

// Encryptor provides a simple interface to encrypt and decrypt paths and metadata.
type Encryptor interface {
	// EncryptPath encrypts the path of an object.
	EncryptPath(bucket string, path string) (string, error)

	// DecryptPath decrypts the path of an object.
	DecryptPath(bucket string, path string) (string, error)

	// EncryptMetadata encrypts the user metadata of an object.
	// The path parameter must be unencrypted.
	EncryptMetadata(bucket string, path string, meta map[string]interface{}) (nonce []byte, encmeta []byte, key []byte, err error)

	// DecryptMetadata decrypts the user metadata of an object.
	// The path parameter must be unencrypted.
	DecryptMetadata(bucket string, path string, nonce []byte, encmeta []byte, key []byte) (map[string]interface{}, error)
}

// NOTE: this hardcoded value comes from uplink.OpenProject(). It may be moved
// to EncryptionAccess in the future.
var defaultEncryptionParameters = storj.EncryptionParameters{
	CipherSuite: storj.EncAESGCM,
	BlockSize:   29 * 256 * memory.B.Int32(),
}

// UplinkEncryptor encrypts/decrypts paths using the uplink library.
type UplinkEncryptor struct {
	store *encryption.Store
}

// NewUplinkEncryptor createa a new UplinkEncryptor instance.
func NewUplinkEncryptor(access *uplink.Access) *UplinkEncryptor {
	encAccess := accessGetEncAccess(access)
	return &UplinkEncryptor{
		store: encAccess.Store,
	}
}

func (e *UplinkEncryptor) EncryptPath(bucket string, path string) (string, error) {
	p := paths.NewUnencrypted(path)
	encPath, err := encryption.EncryptPath(bucket, p, e.store.GetDefaultPathCipher(), e.store)
	if err != nil {
		return "", fmt.Errorf("%w: cannot encrypt path: %s", ErrInternalError, path)
	}
	fmt.Printf("%s => %v\n", path, hex.EncodeToString([]byte(encPath.Raw())))
	return encPath.Raw(), nil
}

func (e *UplinkEncryptor) DecryptPath(bucket string, path string) (string, error) {
	p, err := encryption.DecryptPath(bucket, paths.NewEncrypted(path), e.store.GetDefaultPathCipher(), e.store)
	if err != nil {
		return "", fmt.Errorf("%w: cannot decrypt path", ErrInternalError)
	}
	return p.String(), err
}

func (e *UplinkEncryptor) EncryptMetadata(bucket string, path string, meta map[string]interface{}) (nonce []byte, encmeta []byte, key []byte, err error) {
	// Create streamInfo structure
	shallowMeta, err := toShallowMetadata(meta)
	if err != nil {
		return nil, nil, nil, err
	}

	metadataBytes, err := pb.Marshal(&pb.SerializableMeta{
		UserDefined: shallowMeta,
	})
	if err != nil {
		return nil, nil, nil, err
	}

	streamInfo, err := pb.Marshal(&pb.StreamInfo{
		Metadata: metadataBytes,
	})
	if err != nil {
		return nil, nil, nil, err
	}

	// Encrypt streamInfo
	derivedKey, err := encryption.DeriveContentKey(bucket, paths.NewUnencrypted(path), e.store)
	if err != nil {
		return nil, nil, nil, err
	}

	var metadataKey storj.Key
	// generate random key for encrypting the segment's content
	_, err = rand.Read(metadataKey[:])
	if err != nil {
		return nil, nil, nil, err
	}

	var encryptedKeyNonce storj.Nonce
	// generate random nonce for encrypting the metadata key
	_, err = rand.Read(encryptedKeyNonce[:])
	if err != nil {
		return nil, nil, nil, err
	}

	encryptedKey, err := encryption.EncryptKey(&metadataKey, defaultEncryptionParameters.CipherSuite, derivedKey, &encryptedKeyNonce)
	if err != nil {
		return nil, nil, nil, err
	}
	key = []byte(encryptedKey)

	encryptedStreamInfo, err := encryption.Encrypt(streamInfo, defaultEncryptionParameters.CipherSuite, &metadataKey, &storj.Nonce{})
	if err != nil {
		return nil, nil, nil, err
	}

	streamMetaBytes, err := pb.Marshal(&pb.StreamMeta{
		EncryptedStreamInfo: encryptedStreamInfo,
	})
	if err != nil {
		return nil, nil, nil, err
	}
	encmeta = streamMetaBytes

	return encryptedKeyNonce[:], streamMetaBytes, []byte(encryptedKey), nil
}

func (e *UplinkEncryptor) DecryptMetadata(bucket string, path string, nonce []byte, encmeta []byte, key []byte) (map[string]interface{}, error) {
	streamMeta := pb.StreamMeta{}
	err := pb.Unmarshal(encmeta, &streamMeta)
	if err != nil {
		return nil, err
	}

	// Decrypt encryptedMetadata payload into pb.StreamInfo
	derivedKey, err := encryption.DeriveContentKey(bucket, paths.NewUnencrypted(path), e.store)
	if err != nil {
		return nil, err
	}

	cipher := storj.CipherSuite(streamMeta.EncryptionType)
	if cipher == storj.EncUnspecified {
		cipher = defaultEncryptionParameters.CipherSuite
	}

	enckey := storj.EncryptedPrivateKey(key)
	sjnonce := storj.Nonce(nonce)
	contentKey, err := encryption.DecryptKey(enckey, cipher, derivedKey, &sjnonce)
	if err != nil {
		return nil, err
	}

	streamInfo, err := encryption.Decrypt(streamMeta.EncryptedStreamInfo, cipher, contentKey, &storj.Nonce{})
	if err != nil {
		return nil, err
	}

	var stream pb.StreamInfo
	if err := pb.Unmarshal(streamInfo, &stream); err != nil {
		return nil, err
	}

	// Deserialize metadata
	serializableMeta := pb.SerializableMeta{}
	err = pb.Unmarshal(stream.Metadata, &serializableMeta)
	if err != nil {
		return nil, err
	}

	if serializableMeta.UserDefined == nil {
		return nil, nil
	}

	return toDeepMetadata(serializableMeta.UserDefined)
}
