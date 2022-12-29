package settings

import (
	"go-common/utils/unit"
	"runtime"
)

type DumplingOptions struct {
	ChunkSize unit.FileSize `yaml:"chunk_size"`
	// Number of goroutines to use, default cpu cores
	Threads int `yaml:"threads"`
	// Attempted size of INSERT statement in bytes
	StatementSize int64 `yaml:"statement_size"`
	// Split table into chunks of this many rows, default unlimited
	MaxRows int64 `yaml:"max_rows"`
	// Use backslash to escape quotation marks
	EscapeBackslash bool `yaml:"escape_backslash"`
	// Do not dump views
	NoViews bool `yaml:"no_views"`
	// Only support transactional consistency
	TransactionalConsistency bool `yaml:"transactional_consistency"`

	Where string `yaml:"where"`

	Consistency string `yaml:"consistency" validate:"oneof=none flush lock snapshot"`
	// Snapshot position. Valid only when consistency=snapshot
	SnapshotPosition string `yaml:"snapshot_position" validate:"required_if=Consistency snapshot"`
}

func defaultDumplingOptions() DumplingOptions {
	return DumplingOptions{
		ChunkSize:                10_000,
		Threads:                  runtime.NumCPU(),
		StatementSize:            0,
		MaxRows:                  0,
		Where:                    "",
		Consistency:              "none",
		NoViews:                  true,
		TransactionalConsistency: true,
	}
}
