// Copyright (C) 2024 Storj Labs, Inc.
// See LICENSE for copying information.

package main

import (
	"fmt"
	"os"
	"path/filepath"

	_ "embed"

	"github.com/spf13/cobra"
	"github.com/zeebo/errs"
	"go.uber.org/zap"

	_ "github.com/jackc/pgx/v5"        // registers pgx as a tagsql driver.
	_ "github.com/jackc/pgx/v5/stdlib" // registers pgx as a tagsql driver.

	"storj.io/metasearch/internal/metasearch"

	"storj.io/storj/satellite/satellitedb"
	"storj.io/storj/shared/tagsql"

	"storj.io/common/cfgstruct"
	"storj.io/common/fpath"
	"storj.io/common/process"
)

var (
	rootCmd = &cobra.Command{
		Use:   "metasearch",
		Short: "Metadata Search server",
	}
	setupCmd = &cobra.Command{
		Use:         "setup",
		Short:       "Create config files",
		RunE:        cmdSetup,
		Annotations: map[string]string{"type": "setup"},
	}
	migrateCmd = &cobra.Command{
		Use:   "migrate",
		Short: "Migrates database to be suitable for metasearch",
		RunE:  cmdMigrate,
	}
	runCmd = &cobra.Command{
		Use:   "run",
		Short: "Run the metasearch server",
		RunE:  cmdRun,
	}
	confDir string

	runCfg   MetaSearchConf
	setupCfg MetaSearchConf
)

type MetaSearchConf struct {
	SatelliteDatabaseURL string `help:"URL to connect to the database" default:""`
	MetabaseURL          string `help:"URL to connect to the metabase" default:""`
	Endpoint             string `help:"Server endpoint (IP + port)" default:"localhost:9998"`
}

func cmdSetup(cmd *cobra.Command, args []string) (err error) {
	setupDir, err := filepath.Abs(confDir)
	if err != nil {
		return err
	}

	valid, _ := fpath.IsValidSetupDir(setupDir)
	if !valid {
		return fmt.Errorf("satellite configuration already exists (%v)", setupDir)
	}

	err = os.MkdirAll(setupDir, 0700)
	if err != nil {
		return err
	}

	if setupCfg.SatelliteDatabaseURL == "" {
		return fmt.Errorf("SatelliteDatabaseURL is required")
	}

	if setupCfg.MetabaseURL == "" {
		return fmt.Errorf("MetabaseURL is required")
	}

	return process.SaveConfig(cmd, filepath.Join(setupDir, "config.yaml"))
}

func cmdRun(cmd *cobra.Command, args []string) (err error) {
	ctx, _ := process.Ctx(cmd)
	log := zap.L()

	db, err := satellitedb.Open(ctx, log.Named("db"), runCfg.SatelliteDatabaseURL, satellitedb.Options{
		ApplicationName: "metadata-api",
	})
	if err != nil {
		return errs.New("Error creating satellite database connection: %+v", err)
	}
	defer func() {
		err = errs.Combine(err, db.Close())
	}()

	metadb, err := tagsql.Open(ctx, "cockroach", runCfg.MetabaseURL)
	if err != nil {
		return errs.New("failed to connect to metabase db: %+v", err)
	}
	defer func() {
		err = errs.Combine(err, metadb.Close())
	}()

	repo := metasearch.NewMetabaseSearchRepository(metadb, log)
	auth := metasearch.NewHeaderAuth(db)
	metadataAPI, err := metasearch.NewServer(log, repo, auth, runCfg.Endpoint)
	if err != nil {
		return errs.New("Error creating metasearch server: %+v", err)
	}

	return metadataAPI.Run()
}

//go:embed migration/001.objects.clear_metadata_metasearch_queued_at.sql
var migrateSql string

func cmdMigrate(cmd *cobra.Command, args []string) (err error) {
	ctx, _ := process.Ctx(cmd)
	log := zap.L()

	metadb, err := tagsql.Open(ctx, "cockroach", runCfg.MetabaseURL)
	if err != nil {
		return errs.New("failed to connect to metabase db: %+v", err)
	}
	defer func() {
		err = errs.Combine(err, metadb.Close())
	}()

	log.Info("running database migrations")
	_, err = metadb.ExecContext(ctx, migrateSql)
	if err != nil {
		log.Error("database migration failed", zap.Error(err))
	}
	return
}

func init() {
	defaultConfDir := fpath.ApplicationDir("storj", "metasearch")
	cfgstruct.SetupFlag(zap.L(), rootCmd, &confDir, "config-dir", defaultConfDir, "main directory for satellite configuration")
	defaults := cfgstruct.DefaultsFlag(rootCmd)
	rootCmd.AddCommand(runCmd)
	rootCmd.AddCommand(setupCmd)
	rootCmd.AddCommand(migrateCmd)
	process.Bind(runCmd, &runCfg, defaults, cfgstruct.ConfDir(confDir))
	process.Bind(migrateCmd, &runCfg, defaults, cfgstruct.ConfDir(confDir))
	process.Bind(setupCmd, &setupCfg, defaults, cfgstruct.ConfDir(confDir), cfgstruct.SetupMode())
}

func main() {
	logger, _, _ := process.NewLogger("metasearch")
	zap.ReplaceGlobals(logger)

	process.Exec(rootCmd)
}
