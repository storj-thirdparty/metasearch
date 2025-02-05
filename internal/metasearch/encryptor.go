// Copyright (C) 2025 Storj Labs, Inc.
// See LICENSE for copying information.

package metasearch

import (
	"crypto/rand"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"

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

	// Compare two encryptors
	Compare(other Encryptor) EncryptorComparisonResult
}

// EncryptorComparisonResult is an enum that describes the relationship between two encryptors.
type EncryptorComparisonResult int

const (
	EncryptorComparisonIdentical EncryptorComparisonResult = 0
	EncryptorComparisonSubset    EncryptorComparisonResult = iota
	EncryptorComparisonSuperset  EncryptorComparisonResult = iota
	EncryptorComparisonDifferent EncryptorComparisonResult = iota
)

// NOTE: this hardcoded value comes from uplink.OpenProject(). It may be moved
// to EncryptionAccess in the future.
var defaultEncryptionParameters = storj.EncryptionParameters{
	CipherSuite: storj.EncAESGCM,
	BlockSize:   29 * 256 * memory.B.Int32(),
}

// UplinkEncryptor encrypts/decrypts paths using the uplink library.
type UplinkEncryptor struct {
	store        *encryption.Store
	storeEntries map[UplinkEncryptorStoreEntry]bool
}

// UplinkEncryptorStoreEntry stores an encryption.Store entry for easy comparison
type UplinkEncryptorStoreEntry struct {
	bucket     string
	root       bool
	unencPath  paths.Unencrypted
	encPath    paths.Encrypted
	key        storj.Key
	pathCipher storj.CipherSuite
}

// NewUplinkEncryptor createa a new UplinkEncryptor instance.
func NewUplinkEncryptor(access *uplink.Access) *UplinkEncryptor {
	encAccess := accessGetEncAccess(access)
	storeEntries := make(map[UplinkEncryptorStoreEntry]bool)

	key := encAccess.Store.GetDefaultKey()
	if key != nil && !key.IsZero() {
		storeEntry := UplinkEncryptorStoreEntry{
			root:       true,
			key:        *key,
			pathCipher: encAccess.Store.GetDefaultPathCipher(),
		}
		storeEntries[storeEntry] = true
	}

	encAccess.Store.IterateWithCipher(func(bucket string, unencPath paths.Unencrypted, encPath paths.Encrypted, key storj.Key, pathCipher storj.CipherSuite) error {
		storeEntry := UplinkEncryptorStoreEntry{
			bucket:     bucket,
			unencPath:  unencPath,
			encPath:    encPath,
			key:        key,
			pathCipher: pathCipher,
		}
		storeEntries[storeEntry] = true
		return nil
	})

	return &UplinkEncryptor{
		store:        encAccess.Store,
		storeEntries: storeEntries,
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
	if len(meta.EncryptedMetadataKey) == 0 || len(meta.EncryptedMetadataNonce) == 0 {
		return nil
	}

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

func (e *UplinkEncryptor) Compare(other Encryptor) EncryptorComparisonResult {
	subset := 0
	superset := 0

	oe, ok := other.(*UplinkEncryptor)
	if !ok {
		return EncryptorComparisonDifferent
	}

	for storeEntry := range e.storeEntries {
		if _, ok := oe.storeEntries[storeEntry]; !ok {
			superset++
		}
	}
	for storeEntry := range oe.storeEntries {
		if _, ok := e.storeEntries[storeEntry]; !ok {
			subset++
		}
	}

	switch {
	case subset == 0 && superset == 0:
		return EncryptorComparisonIdentical
	case subset > 0 && superset == 0:
		return EncryptorComparisonSubset
	case subset == 0 && superset > 0:
		return EncryptorComparisonSuperset
	default:
		return EncryptorComparisonDifferent
	}
}

// EncryptorRepository maintains a set of encryptors for a project, and tries
// the find the best encryptor for an object.
type EncryptorRepository struct {
	encryptors []EncryptorRepositoryEntry
	mutex      *sync.Mutex
}

// EncryptorRepositoryEntry stores a single encryptor with its success rate.
type EncryptorRepositoryEntry struct {
	encryptor Encryptor
	success   int64
	total     int64
}

// NewEncryptorRepository creates an empty encryptor repository.
func NewEncryptorRepository() *EncryptorRepository {
	return &EncryptorRepository{
		mutex: &sync.Mutex{},
	}
}

// AddEncryptor adds the encryptor to the repository, trying to keep a minimal
// set of encryptors. Returns true if the encryptor has not been in the
// repository yet, or if it replaced an existing item because it is a superset.
func (r *EncryptorRepository) AddEncryptor(newEncryptor Encryptor) bool {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	newEntry := EncryptorRepositoryEntry{
		encryptor: newEncryptor,
	}

	for i := range len(r.encryptors) {
		entry := r.encryptors[i]
		cmp := newEncryptor.Compare(entry.encryptor)
		if cmp == EncryptorComparisonIdentical || cmp == EncryptorComparisonSubset {
			return false
		}
		if cmp == EncryptorComparisonSuperset {
			r.encryptors[i] = newEntry
			return true
		}
	}

	r.encryptors = append(r.encryptors, newEntry)
	return true
}

// DecryptObjectDetails tries to decode object key and metadata by all
// available encryptors. Returns the object key (decrypted on success,
// encrypted on error) and an error if none of the encryptors succeeded.
func (r *EncryptorRepository) DecryptMetadata(obj *ObjectInfo) (clearObjectKey string, meta ObjectMetadata, err error) {
	meta = obj.Metadata

	for i := range len(r.encryptors) {
		e := &r.encryptors[i]
		atomic.AddInt64(&e.total, 1)

		// Try to decrypt path
		clearObjectKey, err = e.encryptor.DecryptPath(obj.BucketName, obj.ObjectKey)
		if err != nil {
			continue
		}

		// Try to decrypt metadata
		err = e.encryptor.DecryptMetadata(obj.BucketName, clearObjectKey, &meta)
		if err != nil {
			continue
		}

		// If both path and metadata can be encrypted, return with success
		atomic.AddInt64(&e.success, 1)
		return
	}

	return obj.ObjectKey, meta, errors.New("cannot find decryption key for object")
}

// CheckEncryptors removes unused encryptors if needed.
// Returns true if an encryptor was removed.
func (r *EncryptorRepository) CheckEncryptors(maxEncryptors int) bool {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if len(r.encryptors) <= maxEncryptors {
		return false
	}

	slices.SortFunc(r.encryptors, func(e1 EncryptorRepositoryEntry, e2 EncryptorRepositoryEntry) int {
		// We compare the encryptors based on the successful encryptions for the total set of queries.
		// The list is sorted in reverse order.
		cmp := e2.success - e1.success
		switch {
		case cmp > 0:
			return 1
		case cmp < 0:
			return -1
		default:
			return 0
		}
	})

	r.encryptors = r.encryptors[:maxEncryptors]
	return true
}
