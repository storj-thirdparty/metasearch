// Copyright (C) 2024 Storj Labs, Inc.
// See LICENSE for copying information.

package metasearch

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"

	"storj.io/common/uuid"
	"storj.io/storj/shared/tagsql"
)

const (
	statusPending     = "1"
	statusesCommitted = "(3,4)"

	deleteMarkerUnversioned = 5
	deleteMarkerVersioned   = 6

	MaxFindObjectsByClearMetadataQuerySize = 10
)

// MetaSearchRepo performs operations on object metadata.
type MetaSearchRepo interface {
	// Get metadata for an object.
	GetMetadata(ctx context.Context, loc ObjectLocation) (obj ObjectInfo, err error)

	// Query metadata in a bucket, optionally in a subdirectory.
	// To search in a subdirectory, pass it in loc.ObjectKey, with a trailing /.
	QueryMetadata(ctx context.Context, loc ObjectLocation, containsQuery map[string]interface{}, startAfter ObjectLocation, batchSize int) (QueryMetadataResult, error)

	// Set metadata for an object.
	UpdateMetadata(ctx context.Context, loc ObjectLocation, meta ObjectMetadata) (err error)

	// Delete metadata for an object.
	DeleteMetadata(ctx context.Context, loc ObjectLocation) (err error)

	// MigrateMetadata updates encrypted metadata for an object and removes it from the migration queue.
	MigrateMetadata(ctx context.Context, obj ObjectInfo) (err error)

	// GetObjectsForMigration fetches all objects to migrate and calls the callback function until it returns false.
	GetObjectsForMigration(ctx context.Context, projectID uuid.UUID, startTime *time.Time, migrate ObjectMigrationFunc) error
}

// ObjectLocation specifies the location of an object.
type ObjectLocation struct {
	ProjectID  uuid.UUID
	BucketName string
	ObjectKey  string
	Version    int64 // optional
}

// ObjectInfo contains a subset of object fields that are used by metasearch.
type ObjectInfo struct {
	ObjectLocation

	Status   byte
	Metadata ObjectMetadata

	MetaSearchQueuedAt *time.Time
}

// ObjectMetadata stores both clear and encrypted metadata for an object.
type ObjectMetadata struct {
	EncryptedMetadataNonce []byte
	EncryptedMetadata      []byte
	EncryptedMetadataKey   []byte

	ClearMetadata map[string]interface{}
}

// QueryMetadataResult is the response of the QueryMetadata operation.
type QueryMetadataResult struct {
	Objects []ObjectInfo
}

// ObjectMigrationFunc is called by GetObjectsForMigration. If the function returns false, the migration stops.
type ObjectMigrationFunc func(ctx context.Context, obj ObjectInfo) bool

// MetabaseSearchRepository implements MetaSearchRepo using the metabase database.
type MetabaseSearchRepository struct {
	db  tagsql.DB
	log *zap.Logger
}

// NewMetabaseSearchRepository creates a new MetabaseSearchRepository.
func NewMetabaseSearchRepository(db tagsql.DB, log *zap.Logger) *MetabaseSearchRepository {
	return &MetabaseSearchRepository{
		db:  db,
		log: log,
	}
}

func (r *MetabaseSearchRepository) GetMetadata(ctx context.Context, loc ObjectLocation) (obj ObjectInfo, err error) {
	var clearMetadata *string
	var status byte

	err = r.db.QueryRowContext(ctx, `
		SELECT
			project_id, bucket_name, object_key, version, status,
			encrypted_metadata_nonce, encrypted_metadata, encrypted_metadata_encrypted_key,
			clear_metadata,
			metasearch_queued_at
		FROM objects
		WHERE
			(project_id, bucket_name, object_key) = ($1, $2, $3) AND
			status <> `+statusPending+`
		ORDER BY version DESC
		LIMIT 1`,
		loc.ProjectID, []byte(loc.BucketName), []byte(loc.ObjectKey),
	).Scan(
		&obj.ProjectID, &obj.BucketName, &obj.ObjectKey, &obj.Version, &obj.Status,
		&obj.Metadata.EncryptedMetadataNonce, &obj.Metadata.EncryptedMetadata, &obj.Metadata.EncryptedMetadataKey,
		&clearMetadata,
		&obj.MetaSearchQueuedAt,
	)

	if errors.Is(err, sql.ErrNoRows) || status == deleteMarkerUnversioned || status == deleteMarkerVersioned {
		return ObjectInfo{}, fmt.Errorf("%w: object not found", ErrNotFound)
	} else if err != nil {
		return ObjectInfo{}, fmt.Errorf("%w: %v", ErrInternalError, err)
	}

	obj.Metadata.ClearMetadata, err = parseJSON(clearMetadata)
	if err != nil {
		return ObjectInfo{}, fmt.Errorf("%w: %v", ErrInternalError, err)
	}

	return obj, nil
}

func (r *MetabaseSearchRepository) UpdateMetadata(ctx context.Context, loc ObjectLocation, meta ObjectMetadata) (err error) {
	// Marshal JSON metadata
	var clearMetadata *string
	if meta.ClearMetadata != nil {
		data, err := json.Marshal(meta.ClearMetadata)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrBadRequest, err)
		}
		s := string(data)
		clearMetadata = &s
	}

	// Execute query
	result, err := r.db.ExecContext(ctx, `
		UPDATE objects
		SET
			encrypted_metadata_nonce=$4, encrypted_metadata=$5, encrypted_metadata_encrypted_key=$6,
			clear_metadata = $7,
			metasearch_queued_at=NULL
		WHERE
			(project_id, bucket_name, object_key) = ($1, $2, $3) AND
			status IN `+statusesCommitted+` AND
			version IN (
				SELECT version
				FROM objects
				WHERE
					(project_id, bucket_name, object_key) = ($1, $2, $3) AND
					status <> `+statusPending+` AND
					(expires_at IS NULL OR expires_at > now())
				ORDER BY version DESC
				LIMIT 1
			)
		`,
		loc.ProjectID, []byte(loc.BucketName), []byte(loc.ObjectKey),
		meta.EncryptedMetadataNonce, meta.EncryptedMetadata, meta.EncryptedMetadataKey,
		clearMetadata,
	)

	if err != nil {
		return fmt.Errorf("%w: unable update to object metadata: %v", ErrInternalError, err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("%w: unable to get rows affected: %v", ErrInternalError, err)
	}

	if affected == 0 {
		return fmt.Errorf("%w: object not found", ErrNotFound)
	}

	return nil
}

func (r *MetabaseSearchRepository) DeleteMetadata(ctx context.Context, loc ObjectLocation) (err error) {
	return r.UpdateMetadata(ctx, loc, ObjectMetadata{})
}

func (r *MetabaseSearchRepository) QueryMetadata(ctx context.Context, loc ObjectLocation, containsQuery map[string]interface{}, startAfter ObjectLocation, batchSize int) (QueryMetadataResult, error) {
	cq, err := json.Marshal(containsQuery)
	if err != nil {
		return QueryMetadataResult{}, fmt.Errorf("%w: %v", ErrInternalError, err)
	}
	jsonContainsQuery := string(cq)

	// Create query
	query := `
		SELECT
			project_id, bucket_name, object_key, version, status,
			encrypted_metadata_nonce, encrypted_metadata, encrypted_metadata_encrypted_key,
			clear_metadata,
			metasearch_queued_at
		FROM objects@objects_pkey
		WHERE
	`

	// We make a subquery for each clear_metadata part. This is optimized for
	// CockroachDB whose optimizer is very unpredictable when querying with
	// multiple JSONB values, and would often scan the full table instead of
	// using the GIN index.
	args := make([]interface{}, 0)
	containsQueryParts, err := splitToJSONLeaves(jsonContainsQuery)
	if err != nil {
		return QueryMetadataResult{}, fmt.Errorf("%w: %v", ErrInternalError, err)
	}
	if len(containsQueryParts) > MaxFindObjectsByClearMetadataQuerySize {
		return QueryMetadataResult{}, fmt.Errorf("%s: too many values in metadata query", ErrBadRequest)
	}

	if len(containsQueryParts) > 0 {
		query += `(project_id, bucket_name, object_key, version) IN (`
		for i, part := range containsQueryParts {
			if i > 0 {
				query += "INTERSECT \n"
			}
			query += fmt.Sprintf("(SELECT project_id, bucket_name, object_key, version FROM objects@objects_clear_metadata_idx WHERE clear_metadata @> $%d)\n", len(args)+1)
			args = append(args, part)
		}
		query += `)`
	}
	if len(args) > 0 {
		query += ` AND `
	}

	query += fmt.Sprintf("project_id = $%d AND bucket_name = $%d AND status <> $%d AND (expires_at IS NULL OR expires_at > now())", len(args)+1, len(args)+2, len(args)+3)
	args = append(args, loc.ProjectID, []byte(loc.BucketName), statusPending)

	// Determine first and last object conditions
	if startAfter.ProjectID.IsZero() {
		// first page => use key prefix
		query += fmt.Sprintf("\nAND (project_id, bucket_name, object_key, version) >= ($%d, $%d, $%d, $%d)", len(args)+1, len(args)+2, len(args)+3, len(args)+4)
		args = append(args, loc.ProjectID, []byte(loc.BucketName), []byte(loc.ObjectKey), 0)
	} else {
		// subsequent pages => use startAfter
		query += fmt.Sprintf("\nAND (project_id, bucket_name, object_key, version) > ($%d, $%d, $%d, $%d)", len(args)+1, len(args)+2, len(args)+3, len(args)+4)
		args = append(args, loc.ProjectID, []byte(loc.BucketName), []byte(startAfter.ObjectKey), startAfter.Version)
	}

	if loc.ObjectKey != "" {
		prefixLimit := prefixLimit(loc.ObjectKey)
		query += fmt.Sprintf("\nAND (project_id, bucket_name, object_key, version) < ($%d, $%d, $%d, $%d)", len(args)+1, len(args)+2, len(args)+3, len(args)+4)
		args = append(args, loc.ProjectID, []byte(loc.BucketName), []byte(prefixLimit), 0)
	}

	query += fmt.Sprintf("\nORDER BY project_id, bucket_name, object_key, version LIMIT $%d", len(args)+1)
	args = append(args, batchSize)

	// Execute query
	r.log.Debug("Querying objects by clear metadata",
		zap.Stringer("Project", loc.ProjectID),
		zap.String("Bucket", loc.BucketName),
		zap.String("KeyPrefix", string(loc.ObjectKey)),
		zap.String("ContainsQuery", jsonContainsQuery),
		zap.Int("BatchSize", batchSize),
		zap.String("StartAfterKey", string(startAfter.ObjectKey)),
	)

	var result QueryMetadataResult
	result.Objects = make([]ObjectInfo, 0, batchSize)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return QueryMetadataResult{}, fmt.Errorf("%w: %v", ErrInternalError, err)
	}
	defer rows.Close()

	for rows.Next() {
		var last ObjectInfo
		var clearMetadata *string
		err = rows.Scan(
			&last.ProjectID, &last.BucketName, &last.ObjectKey, &last.Version, &last.Status,
			&last.Metadata.EncryptedMetadataNonce, &last.Metadata.EncryptedMetadata, &last.Metadata.EncryptedMetadataKey,
			&clearMetadata,
			&last.MetaSearchQueuedAt,
		)
		if err != nil {
			return QueryMetadataResult{}, fmt.Errorf("%w: %v", ErrInternalError, err)
		}

		last.Metadata.ClearMetadata, err = parseJSON(clearMetadata)
		if err != nil {
			return QueryMetadataResult{}, fmt.Errorf("%w: %v", ErrInternalError, err)
		}

		result.Objects = append(result.Objects, last)
	}

	return result, nil
}

func (r *MetabaseSearchRepository) MigrateMetadata(ctx context.Context, obj ObjectInfo) (err error) {
	// Marshal JSON metadata
	var clearMetadata *string
	if obj.Metadata.ClearMetadata != nil {
		data, err := json.Marshal(obj.Metadata.ClearMetadata)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrBadRequest, err)
		}
		s := string(data)
		clearMetadata = &s
	}

	// Execute query
	result, err := r.db.ExecContext(ctx, `
		UPDATE objects
		SET
			encrypted_metadata_nonce=$6, encrypted_metadata=$7, encrypted_metadata_encrypted_key=$8,
			clear_metadata = $9,
			metasearch_queued_at=NULL
		WHERE
			(project_id, bucket_name, object_key, version) = ($1, $2, $3, $4) AND
			metasearch_queued_at=$5
		`,
		obj.ProjectID, []byte(obj.BucketName), []byte(obj.ObjectKey), obj.Version, obj.MetaSearchQueuedAt,
		obj.Metadata.EncryptedMetadataNonce, obj.Metadata.EncryptedMetadata, obj.Metadata.EncryptedMetadataKey,
		clearMetadata,
	)

	if err != nil {
		return fmt.Errorf("%w: unable update to object metadata: %v", ErrInternalError, err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("%w: unable to get rows affected: %v", ErrInternalError, err)
	}

	if affected == 0 {
		return fmt.Errorf("%w: object not found or has been already migrated", ErrNotFound)
	}

	return nil
}

func (r *MetabaseSearchRepository) GetObjectsForMigration(ctx context.Context, projectID uuid.UUID, startTime *time.Time, migrate ObjectMigrationFunc) error {
	query := `
		SELECT
			project_id, bucket_name, object_key, version, status,
			encrypted_metadata_nonce, encrypted_metadata, encrypted_metadata_encrypted_key,
			clear_metadata,
			metasearch_queued_at
		FROM objects@objects_metasearch_queued_at_idx
		WHERE
			project_id=$1 AND
			metasearch_queued_at IS NOT NULL
	`
	args := []interface{}{projectID}

	if startTime != nil {
		query += " AND metasearch_queued_at >= $2 "
		args = append(args, *startTime)
	}

	query += " ORDER BY metasearch_queued_at "
	rows, err := r.db.QueryContext(ctx, query, args...)

	if err != nil {
		return fmt.Errorf("%w: %v", ErrInternalError, err)
	}
	defer rows.Close()

	for rows.Next() {
		var obj ObjectInfo
		var clearMetadata *string
		err = rows.Scan(
			&obj.ProjectID, &obj.BucketName, &obj.ObjectKey, &obj.Version, &obj.Status,
			&obj.Metadata.EncryptedMetadataNonce, &obj.Metadata.EncryptedMetadata, &obj.Metadata.EncryptedMetadataKey,
			&clearMetadata,
			&obj.MetaSearchQueuedAt,
		)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrInternalError, err)
		}

		obj.Metadata.ClearMetadata, err = parseJSON(clearMetadata)
		if err != nil {
			// Log and ignore error: the migrator will only use the encrypted metadata
			r.log.Warn("cannot decode clear metadata",
				zap.Stringer("Project", obj.ProjectID),
				zap.String("Bucket", obj.BucketName),
				zap.String("ObjectKey", obj.ObjectKey),
				zap.Error(err),
			)
		}

		if !migrate(ctx, obj) {
			break
		}
	}
	return nil
}
