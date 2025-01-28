// Copyright (C) 2024 Storj Labs, Inc.
// See LICENSE for copying information.

package metasearch

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"go.uber.org/zap"
	"storj.io/common/uuid"

	"storj.io/storj/satellite/metabase"
	"storj.io/storj/shared/tagsql"
)

const (
	statusPending     = "1"
	statusesCommitted = "(3,4)"

	MaxFindObjectsByClearMetadataQuerySize = 10
)

// MetaSearchRepo performs operations on object metadata.
type MetaSearchRepo interface {
	GetMetadata(ctx context.Context, loc metabase.ObjectLocation) (meta map[string]interface{}, err error)
	QueryMetadata(ctx context.Context, loc metabase.ObjectLocation, containsQuery map[string]interface{}, startAfter metabase.ObjectStream, batchSize int) (FindObjectsByClearMetadataResult, error)
	UpdateMetadata(ctx context.Context, loc metabase.ObjectLocation, meta map[string]interface{}) (err error)
	DeleteMetadata(ctx context.Context, loc metabase.ObjectLocation) (err error)
}

type MetabaseSearchRepository struct {
	db  tagsql.DB
	log *zap.Logger
}

type FindObjectsByClearMetadata struct {
	ProjectID     uuid.UUID
	BucketName    metabase.BucketName
	KeyPrefix     string
	ContainsQuery string
}

type FindObjectsByClearMetadataResult struct {
	Objects []FindObjectsByClearMetadataResultObject
}

type FindObjectsByClearMetadataResultObject struct {
	metabase.ObjectStream
	ClearMetadata string
}

// NewMetabaseSearchRepository creates a new MetabaseSearchRepository.
func NewMetabaseSearchRepository(db tagsql.DB, log *zap.Logger) *MetabaseSearchRepository {
	return &MetabaseSearchRepository{
		db:  db,
		log: log,
	}
}

func (r *MetabaseSearchRepository) GetMetadata(ctx context.Context, loc metabase.ObjectLocation) (meta map[string]interface{}, err error) {
	var clearMetadata *string
	var status metabase.ObjectStatus

	err = r.db.QueryRowContext(ctx, `
		SELECT clear_metadata, status
		FROM objects
		WHERE
			(project_id, bucket_name, object_key) = ($1, $2, $3) AND
			status <> `+statusPending+`
		ORDER BY version DESC
		LIMIT 1`,
		loc.ProjectID, loc.BucketName, loc.ObjectKey,
	).Scan(&clearMetadata, &status)

	if errors.Is(err, sql.ErrNoRows) || status.IsDeleteMarker() {
		return nil, fmt.Errorf("%w: object not found", ErrNotFound)
	} else if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInternalError, err)
	}

	if clearMetadata != nil {
		return parseJSON(*clearMetadata)
	}
	return nil, nil
}

func (r *MetabaseSearchRepository) UpdateMetadata(ctx context.Context, loc metabase.ObjectLocation, meta map[string]interface{}) (err error) {
	// Parse JSON metadata
	var newMetadata *string
	if meta != nil {
		data, err := json.Marshal(meta)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrBadRequest, err)
		}
		s := string(data)
		newMetadata = &s
	}

	result, err := r.db.ExecContext(ctx, `
		UPDATE objects
		SET clear_metadata = $4
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
		loc.ProjectID, loc.BucketName, loc.ObjectKey, newMetadata,
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

func (r *MetabaseSearchRepository) DeleteMetadata(ctx context.Context, loc metabase.ObjectLocation) (err error) {
	return r.UpdateMetadata(ctx, loc, nil)
}

func (r *MetabaseSearchRepository) QueryMetadata(ctx context.Context, loc metabase.ObjectLocation, containsQuery map[string]interface{}, startAfter metabase.ObjectStream, batchSize int) (FindObjectsByClearMetadataResult, error) {
	cq, err := json.Marshal(containsQuery)
	if err != nil {
		return FindObjectsByClearMetadataResult{}, fmt.Errorf("%w: %v", ErrInternalError, err)
	}

	opts := FindObjectsByClearMetadata{
		ProjectID:     loc.ProjectID,
		BucketName:    loc.BucketName,
		KeyPrefix:     string(loc.ObjectKey),
		ContainsQuery: string(cq),
	}

	// Create query
	query := `
		SELECT
			project_id, bucket_name, object_key, version, stream_id, clear_metadata
		FROM objects@objects_pkey
		WHERE
	`

	// We make a subquery for each clear_metadata part. This is optimized for
	// CockroachDB whose optimizer is very unpredictable when querying with
	// multiple JSONB values, and would often scan the full table instead of
	// using the GIN index.
	args := make([]interface{}, 0)
	containsQueryParts, err := splitToJSONLeaves(opts.ContainsQuery)
	if err != nil {
		return FindObjectsByClearMetadataResult{}, fmt.Errorf("%w: %v", ErrInternalError, err)
	}
	if len(containsQueryParts) > MaxFindObjectsByClearMetadataQuerySize {
		return FindObjectsByClearMetadataResult{}, fmt.Errorf("%s: too many values in metadata query", ErrBadRequest)
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
	args = append(args, opts.ProjectID, opts.BucketName, statusPending)

	// Determine first and last object conditions
	if startAfter.ProjectID.IsZero() {
		// first page => use key prefix
		query += fmt.Sprintf("\nAND (project_id, bucket_name, object_key, version) >= ($%d, $%d, $%d, $%d)", len(args)+1, len(args)+2, len(args)+3, len(args)+4)
		args = append(args, opts.ProjectID, opts.BucketName, metabase.ObjectKey(opts.KeyPrefix), 0)
	} else {
		// subsequent pages => use startAfter
		query += fmt.Sprintf("\nAND (project_id, bucket_name, object_key, version) > ($%d, $%d, $%d, $%d)", len(args)+1, len(args)+2, len(args)+3, len(args)+4)
		args = append(args, opts.ProjectID, opts.BucketName, startAfter.ObjectKey, startAfter.Version)
	}

	if opts.KeyPrefix != "" {
		prefixLimit := metabase.PrefixLimit(metabase.ObjectKey(opts.KeyPrefix))
		query += fmt.Sprintf("\nAND (project_id, bucket_name, object_key, version) < ($%d, $%d, $%d, $%d)", len(args)+1, len(args)+2, len(args)+3, len(args)+4)
		args = append(args, opts.ProjectID, opts.BucketName, prefixLimit, 0)
	}

	query += fmt.Sprintf("\nORDER BY project_id, bucket_name, object_key, version LIMIT $%d", len(args)+1)
	// fmt.Println(query)
	args = append(args, batchSize)

	// Execute query
	r.log.Debug("Querying objects by clear metadata",
		zap.Stringer("Project", opts.ProjectID),
		zap.Stringer("Bucket", opts.BucketName),
		zap.String("KeyPrefix", string(opts.KeyPrefix)),
		zap.String("ContainsQuery", opts.ContainsQuery),
		zap.Int("BatchSize", batchSize),
		zap.String("StartAfterKey", string(startAfter.ObjectKey)),
	)

	var result FindObjectsByClearMetadataResult
	result.Objects = make([]FindObjectsByClearMetadataResultObject, 0, batchSize)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return FindObjectsByClearMetadataResult{}, fmt.Errorf("%w: %v", ErrInternalError, err)
	}

	var last FindObjectsByClearMetadataResultObject
	for rows.Next() {
		err = rows.Scan(
			&last.ProjectID, &last.BucketName, &last.ObjectKey, &last.Version, &last.StreamID, &last.ClearMetadata)
		if err != nil {
			return FindObjectsByClearMetadataResult{}, fmt.Errorf("%w: %v", ErrInternalError, err)
		}

		result.Objects = append(result.Objects, last)
	}

	return result, nil
}
