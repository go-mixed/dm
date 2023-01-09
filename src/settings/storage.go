package settings

import (
	"gopkg.in/go-mixed/go-common.v1/utils/io"
	"path/filepath"
	"time"
)

type StorageOptions struct {
	Dir           string        `yaml:"dir"`
	GCTimer       time.Duration `yaml:"gc_timer"`
	ConfSyncTimer time.Duration `yaml:"conf_sync_timer"`

	MemoryMode bool `yaml:"memory_mode"`
}

func defaultStorageOptions() StorageOptions {
	return StorageOptions{
		Dir:           filepath.Join(io_utils.GetCurrentDir(), "storage"),
		GCTimer:       1 * time.Minute,
		ConfSyncTimer: 30 * time.Second,
		MemoryMode:    false,
	}
}
