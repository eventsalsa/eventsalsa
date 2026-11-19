// Command migrate-gen generates SQL migration files for projector infrastructure.
//
// Usage:
//
//	go run eventsalsa.dev/projector/cmd/migrate-gen -output migrations -filename init_projector.sql
//
// Or with go generate:
//
//	//go:generate go run eventsalsa.dev/projector/cmd/migrate-gen -output migrations
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"eventsalsa.dev/projector/migrations"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	config := migrations.DefaultConfig()

	flags := flag.NewFlagSet("migrate-gen", flag.ContinueOnError)
	flags.SetOutput(stderr)

	outputFolder := flags.String("output", config.OutputFolder, "Output folder for migration file")
	outputFilename := flags.String("filename", "", "Output filename (default: timestamp-based)")
	projectorInstancesTable := flags.String("projector-instances-table", config.ProjectorInstancesTable, "Name of projector instances table")
	projectionAssignmentsTable := flags.String("projection-assignments-table", config.ProjectionAssignmentsTable, "Name of projection assignments table")
	projectionCheckpointsTable := flags.String("projection-checkpoints-table", config.ProjectionCheckpointsTable, "Name of projection checkpoints table")
	projectorLeaderLeasesTable := flags.String("projector-leader-leases-table", config.ProjectorLeaderLeasesTable, "Name of leader lease table")

	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}

		return 2
	}

	config.OutputFolder = *outputFolder
	config.ProjectorInstancesTable = *projectorInstancesTable
	config.ProjectionAssignmentsTable = *projectionAssignmentsTable
	config.ProjectionCheckpointsTable = *projectionCheckpointsTable
	config.ProjectorLeaderLeasesTable = *projectorLeaderLeasesTable

	if *outputFilename != "" {
		config.OutputFilename = *outputFilename
	}

	if err := migrations.GeneratePostgres(&config); err != nil {
		if _, writeErr := fmt.Fprintf(stderr, "Error generating migration: %v\n", err); writeErr != nil {
			return 1
		}
		return 1
	}

	if _, err := fmt.Fprintf(stdout, "Generated PostgreSQL migration: %s\n", filepath.Join(config.OutputFolder, config.OutputFilename)); err != nil {
		return 1
	}
	return 0
}
