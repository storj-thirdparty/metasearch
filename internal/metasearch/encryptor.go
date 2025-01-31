// Copyright (C) 2025 Storj Labs, Inc.
// See LICENSE for copying information.

package metasearch

import (
	"crypto/rand"
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
	EncryptMetadata(bucket string, path string, meta *ObjectMetadata) error

	// DecryptMetadata decrypts the user metadata of an object.
	// The path parameter must be unencrypted.
	DecryptMetadata(bucket string, path string, meta *ObjectMetadata) error
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
	return encPath.Raw(), nil
}

func (e *UplinkEncryptor) DecryptPath(bucket string, path string) (string, error) {
	p, err := encryption.DecryptPath(bucket, paths.NewEncrypted(path), e.store.GetDefaultPathCipher(), e.store)
	if err != nil {
		return "", fmt.Errorf("%w: cannot decrypt path", ErrInternalError)
	}
	return p.String(), err
}

func (e *UplinkEncryptor) EncryptMetadata(bucket string, path string, meta *ObjectMetadata) error {
	// Create streamInfo structure
	shallowMeta, err := toShallowMetadata(meta.ClearMetadata)
	if err != nil {
		return err
	}

	metadataBytes, err := pb.Marshal(&pb.SerializableMeta{
		UserDefined: shallowMeta,
	})
	if err != nil {
		return err
	}

	streamInfo, err := pb.Marshal(&pb.StreamInfo{
		Metadata: metadataBytes,
	})
	if err != nil {
		return err
	}

	// Encrypt streamInfo
	derivedKey, err := encryption.DeriveContentKey(bucket, paths.NewUnencrypted(path), e.store)
	if err != nil {
		return err
	}

	var metadataKey storj.Key
	// generate random key for encrypting the segment's content
	_, err = rand.Read(metadataKey[:])
	if err != nil {
		return err
	}

	// generate random nonce for encrypting the metadata key
	var encryptedKeyNonce storj.Nonce
	_, err = rand.Read(encryptedKeyNonce[:])
	if err != nil {
		return err
	}
	meta.EncryptedMetadataNonce = encryptedKeyNonce[:]

	encryptedKey, err := encryption.EncryptKey(&metadataKey, defaultEncryptionParameters.CipherSuite, derivedKey, &encryptedKeyNonce)
	if err != nil {
		return err
	}
	meta.EncryptedMetadataKey = []byte(encryptedKey)

	encryptedStreamInfo, err := encryption.Encrypt(streamInfo, defaultEncryptionParameters.CipherSuite, &metadataKey, &storj.Nonce{})
	if err != nil {
		return err
	}

	streamMetaBytes, err := pb.Marshal(&pb.StreamMeta{
		EncryptedStreamInfo: encryptedStreamInfo,
	})
	if err != nil {
		return err
	}
	meta.EncryptedMetadata = streamMetaBytes

	return nil
}

func (e *UplinkEncryptor) DecryptMetadata(bucket string, path string, meta *ObjectMetadata) error {
	streamMeta := pb.StreamMeta{}
	err := pb.Unmarshal(meta.EncryptedMetadata, &streamMeta)
	if err != nil {
		return err
	}

	// Decrypt encryptedMetadata payload into pb.StreamInfo
	derivedKey, err := encryption.DeriveContentKey(bucket, paths.NewUnencrypted(path), e.store)
	if err != nil {
		return err
	}

	cipher := storj.CipherSuite(streamMeta.EncryptionType)
	if cipher == storj.EncUnspecified {
		cipher = defaultEncryptionParameters.CipherSuite
	}

	enckey := storj.EncryptedPrivateKey(meta.EncryptedMetadataKey)
	sjnonce := storj.Nonce(meta.EncryptedMetadataNonce)
	contentKey, err := encryption.DecryptKey(enckey, cipher, derivedKey, &sjnonce)
	if err != nil {
		return err
	}

	streamInfo, err := encryption.Decrypt(streamMeta.EncryptedStreamInfo, cipher, contentKey, &storj.Nonce{})
	if err != nil {
		return err
	}

	var stream pb.StreamInfo
	if err := pb.Unmarshal(streamInfo, &stream); err != nil {
		return err
	}

	// Deserialize metadata
	serializableMeta := pb.SerializableMeta{}
	err = pb.Unmarshal(stream.Metadata, &serializableMeta)
	if err != nil {
		return err
	}

	if serializableMeta.UserDefined == nil {
		meta.ClearMetadata = nil
		return nil
	}

	clearMetadata, err := toDeepMetadata(serializableMeta.UserDefined)
	if err != nil {
		return err
	}

	meta.ClearMetadata = clearMetadata
	return nil
}
