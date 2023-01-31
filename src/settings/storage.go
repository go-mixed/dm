package settings

import (
	"gopkg.in/go-mixed/go-common.v1/utils/io"
	"path/filepath"
	"time"
)

type StorageOptions struct {
	// 存储目录
	Dir string `yaml:"dir"`
	// badger引擎gc间隔时间
	GCInterval time.Duration `yaml:"gc_interval"`
	// 位置状态文件落盘间隔时间
	PositionStatusSyncInterval time.Duration `yaml:"position_status_sync_interval"`
	// 清理消费后的数据间隔时间
	FlushInterval time.Duration `yaml:"flush_interval"`

	MemoryMode bool `yaml:"memory_mode"`
}

func defaultStorageOptions() StorageOptions {
	return StorageOptions{
		Dir:                        filepath.Join(io_utils.GetCurrentDir(), "storage"),
		GCInterval:                 1 * time.Minute,
		PositionStatusSyncInterval: 30 * time.Second,
		MemoryMode:                 false,
		FlushInterval:              10 * time.Second,
	}
}

func (o StorageOptions) IsImmediateFlush() bool {
	return o.FlushInterval <= 0
}
