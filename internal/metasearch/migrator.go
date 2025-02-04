// Copyright (C) 2025 Storj Labs, Inc.
// See LICENSE for copying information.

package metasearch

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"storj.io/common/uuid"
)

const migrationInterval = 1 * time.Second
const maxEncryptorsPerProject = 100

// ObjectMigrator manages encryptors and migrates the encrypted metadata to
// clear metadata in the background.
type ObjectMigrator struct {
	log     *zap.Logger
	repo    MetaSearchRepo
	workers map[uuid.UUID]*ObjectMigratorWorker
	mutex   *sync.Mutex
	running bool
	done    chan bool
}

// NewObjectMigrator creates an ObjectMigrator instance.
func NewObjectMigrator(log *zap.Logger, repo MetaSearchRepo) *ObjectMigrator {
	return &ObjectMigrator{
		log:     log,
		repo:    repo,
		workers: make(map[uuid.UUID]*ObjectMigratorWorker),
		mutex:   &sync.Mutex{},
		done:    make(chan bool, 1),
	}
}

// AddProject starts a worker for the given project if it does not exist, and adds the encryptor to it.
func (m *ObjectMigrator) AddProject(ctx context.Context, projectID uuid.UUID, encryptor Encryptor) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if worker, ok := m.workers[projectID]; ok {
		worker.AddEncryptor(encryptor)
		return
	}

	worker := NewObjectMigratorWorker(m.log, m.repo, projectID)
	worker.AddEncryptor(encryptor)
	m.workers[projectID] = worker
}

// Start object migrator in the background.
func (m *ObjectMigrator) Start() {
	m.running = true
	go func() {
		for m.running {
			time.Sleep(migrationInterval)

			m.mutex.Lock()
			for _, worker := range m.workers {
				worker.Start()
			}
			m.mutex.Unlock()
		}
		m.done <- true
	}()
}

// Stop object migrator, wait until it finishes all pending migrations.
func (m *ObjectMigrator) Stop() {
	if !m.running {
		return
	}

	m.running = false
	<-m.done
}

// WaitForProject triggers the migraion of a project in the background, and
// waits until it finishes with a timeout. It returns true if the migration has
// completed before the timeout.
func (m *ObjectMigrator) WaitForProject(ctx context.Context, projectID uuid.UUID, timeout time.Duration) bool {
	m.mutex.Lock()
	worker, ok := m.workers[projectID]
	m.mutex.Unlock()

	if !ok {
		m.log.Error("no migration worker for project", zap.Stringer("ProjectID", projectID))
		return false
	}

	return worker.WaitForProject(ctx, timeout)
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

	mutex       *sync.Mutex
	running     bool
	encryptors  *EncryptorRepository
	subscribers []chan bool
	startTime   *time.Time
}

// NewObjectMigratorWorker creates a new object migrator worker.
func NewObjectMigratorWorker(log *zap.Logger, repo MetaSearchRepo, projectID uuid.UUID) *ObjectMigratorWorker {
	return &ObjectMigratorWorker{
		log:        log,
		repo:       repo,
		projectID:  projectID,
		mutex:      &sync.Mutex{},
		encryptors: NewEncryptorRepository(),
	}
}

// AddEncryptor adds an encryptor to the worker. If the encryptor has different
// settings from all previous ones, reprocess failed items in the migration queue.
func (w *ObjectMigratorWorker) AddEncryptor(encryptor Encryptor) {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if w.encryptors.AddEncryptor(encryptor) {
		// Restart the migration queue if a new encryptor is added, so that
		// migrations that failed due to decryption errors can be retried.
		w.startTime = nil
	}
}

// WaitForProject triggers the migraion of a project in the background, and
// waits until it finishes with a timeout. It returns true if the migration has
// completed before the timeout.
func (w *ObjectMigratorWorker) WaitForProject(ctx context.Context, timeout time.Duration) bool {
	// Start worker, subscribe to its finish event
	w.mutex.Lock()
	done := make(chan bool, 1)
	w.subscribers = append(w.subscribers, done)
	w.mutex.Unlock()

	w.Start()

	// Create timeout
	timeoutCh := make(chan bool, 1)
	go func() {
		time.Sleep(timeout)
		timeoutCh <- true
	}()

	// Wait for worker/timeout
	select {
	case <-timeoutCh:
		return false
	case <-done:
		return true
	}
}

// Start the migration worker. Must be called while w.mutex is locked.
func (w *ObjectMigratorWorker) Start() {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if w.running {
		return
	}
	w.running = true

	go func() {
		w.MigrateProject(context.Background())

		w.mutex.Lock()
		for _, subscriber := range w.subscribers {
			subscriber <- true
		}
		w.subscribers = nil
		w.running = false
		w.mutex.Unlock()
	}()
}

func (w *ObjectMigratorWorker) MigrateProject(ctx context.Context) error {
	err := w.repo.GetObjectsForMigration(ctx, w.projectID, w.startTime, func(ctx context.Context, obj ObjectInfo) bool {
		_ = w.MigrateObject(ctx, &obj)
		return true
	})

	if err != nil {
		w.log.Warn("error migrating project",
			zap.Stringer("ProjectID", w.projectID),
			zap.Error(err),
		)
	}

	return err
}

// MigrateObject migrates a single object in database.
func (w *ObjectMigratorWorker) MigrateObject(ctx context.Context, obj *ObjectInfo) error {
	// Check project ID
	if obj.ProjectID != w.projectID {
		return fmt.Errorf("worker projectID mismatch: %s vs %s", w.projectID, obj.ProjectID)
	}

	// Decrypt path and metadata
	clearObjectKey, meta, err := w.encryptors.DecryptMetadata(obj)
	if err != nil {
		w.log.Warn(err.Error(),
			zap.Stringer("Project", obj.ProjectID),
			zap.String("Bucket", obj.BucketName),
			zap.String("ObjectKey", obj.ObjectKey),
			zap.Error(err),
		)
		w.updateStartTime(obj) // skip this object until we get a new encryptor
		return err
	}

	// Limit number of encryptors
	w.encryptors.CheckEncryptors(maxEncryptorsPerProject)

	// Migrate metadata
	obj.Metadata = meta
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

	w.updateStartTime(obj)
	return nil
}

func (w *ObjectMigratorWorker) updateStartTime(obj *ObjectInfo) {
	w.mutex.Lock()
	if w.startTime == nil || w.startTime.Before(*obj.MetaSearchQueuedAt) {
		w.startTime = obj.MetaSearchQueuedAt
	}
	w.mutex.Unlock()
}
