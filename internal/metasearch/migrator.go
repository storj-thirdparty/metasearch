// Copyright (C) 2025 Storj Labs, Inc.
// See LICENSE for copying information.

package metasearch

import (
	"context"
	"fmt"
	"sync"

	"go.uber.org/zap"

	"storj.io/common/uuid"
)

// ObjectMigrator manages encryptors and migrates the encrypted metadata to
// clear metadata in the background.
type ObjectMigrator struct {
	log     *zap.Logger
	repo    MetaSearchRepo
	workers map[uuid.UUID]*ObjectMigratorWorker
	mutex   *sync.Mutex
}

// NewObjectMigrator creates an ObjectMigrator instance.
func NewObjectMigrator(log *zap.Logger, repo MetaSearchRepo) *ObjectMigrator {
	return &ObjectMigrator{
		log:     log,
		repo:    repo,
		workers: make(map[uuid.UUID]*ObjectMigratorWorker),
		mutex:   &sync.Mutex{},
	}
}

// AddProject starts a worker for the given project if it does not exist, and adds the encryptor to it.
func (m *ObjectMigrator) AddProject(ctx context.Context, projectID uuid.UUID, encryptor Encryptor) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if _, ok := m.workers[projectID]; ok {
		return
	}

	worker := ObjectMigratorWorker{
		log:       m.log,
		repo:      m.repo,
		projectID: projectID,
		encryptor: encryptor,
	}

	m.workers[projectID] = &worker
}

func (m *ObjectMigrator) MigrateProject(ctx context.Context, projectID uuid.UUID) error {
	m.mutex.Lock()
	worker, ok := m.workers[projectID]
	m.mutex.Unlock()

	if !ok {
		return fmt.Errorf("no migration worker for project '%s'", projectID)
	}

	err := m.repo.GetObjectsForMigration(ctx, projectID, func(ctx context.Context, obj ObjectInfo) bool {
		_ = worker.MigrateObject(ctx, &obj)
		return true // TODO: timeout and/or error handling
	})

	if err != nil {
		m.log.Warn("error migrating project",
			zap.Stringer("ProjectID", projectID),
			zap.Error(err),
		)
	}

	return err
}

// MigrateObject migrates a single object using the stored encryptors.
func (m *ObjectMigrator) MigrateObject(ctx context.Context, obj *ObjectInfo) error {
	m.mutex.Lock()
	worker, ok := m.workers[obj.ProjectID]
	m.mutex.Unlock()

	if !ok {
		return fmt.Errorf("no migration worker for project '%s'", obj.ProjectID)
	}

	return worker.MigrateObject(ctx, obj)
}

// ObjectMigratorWorker migrates objects for a single ProjectID.
type ObjectMigratorWorker struct {
	log       *zap.Logger
	repo      MetaSearchRepo
	projectID uuid.UUID
	encryptor Encryptor
}

// MigrateObject migrates a single object in database.
func (w *ObjectMigratorWorker) MigrateObject(ctx context.Context, obj *ObjectInfo) error {
	// Check project ID
	if obj.ProjectID != w.projectID {
		return fmt.Errorf("worker projectID mismatch: %s vs %s", w.projectID, obj.ProjectID)
	}

	// Decrypt path
	clearObjectKey, err := w.encryptor.DecryptPath(obj.BucketName, obj.ObjectKey)
	if err != nil {
		w.log.Warn("cannot decrypt object path",
			zap.Stringer("Project", obj.ProjectID),
			zap.String("Bucket", obj.BucketName),
			zap.String("ObjectKey", obj.ObjectKey),
			zap.Error(err),
		)
		return err
	}

	// Decrypt metadata
	meta := obj.Metadata
	err = w.encryptor.DecryptMetadata(obj.BucketName, clearObjectKey, &meta)
	if err != nil {
		w.log.Warn("cannot decrypt metadata",
			zap.Stringer("Project", obj.ProjectID),
			zap.String("Bucket", obj.BucketName),
			zap.String("ObjectKey", clearObjectKey),
			zap.Error(err),
		)
		return err
	}
	obj.Metadata = meta

	// Migrate metadata
	err = w.repo.MigrateMetadata(ctx, *obj)
	if err != nil {
		w.log.Warn("cannot migrate metadata",
			zap.Stringer("Project", obj.ProjectID),
			zap.String("Bucket", clearObjectKey),
			zap.String("ObjectKey", clearObjectKey),
			zap.Error(err),
		)
		return err
	}

	w.log.Debug("migrated metadata for object",
		zap.Stringer("Project", obj.ProjectID),
		zap.String("Bucket", obj.BucketName),
		zap.String("ObjectKey", clearObjectKey),
	)
	return nil
}
