package storage

import (
	"fmt"
	"github.com/fly-studio/dm/src/common"
	"github.com/fly-studio/dm/src/consumer"
	"github.com/fly-studio/dm/src/settings"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/pkg/errors"
	"go-common-storage"
	"go-common/utils"
	"go-common/utils/io"
	"go-common/utils/text"
	"go.etcd.io/bbolt"
	"go.uber.org/zap"
	"path/filepath"
	"sync"
)

type Storage struct {
	settings *settings.Settings
	logger   *utils.Logger

	bolt *storage.Bolt

	tables map[string]*schema.Table

	latestID       uint64
	exportingCount uint64

	mutex sync.Mutex
}

func NewStorage(settings *settings.Settings, logger *utils.Logger) (*Storage, error) {
	storagePath := filepath.Join(settings.Storage, common.BoltFilename)
	bolt, err := storage.NewBolt(storagePath, logger.Sugar())
	if err != nil {
		return nil, err
	}
	bolt.SetEncodeFunc(text_utils.GobEncode).SetDecodeFunc(text_utils.GobDecode)

	return &Storage{
		settings: settings,
		logger:   logger,
		bolt:     bolt,
		tables:   make(map[string]*schema.Table),

		latestID: 0,
		mutex:    sync.Mutex{},
	}, nil
}

func (s *Storage) Initial() error {
	s.ReadTables()

	pos := s.ReadBinLogPosition()
	if pos.IsEmpty() { // delete events if master-info.yaml not exists
		s.ClearEvents()
	}

	_ = s.bolt.Bucket(common.StorageEvents).View(func(bucket *bbolt.Bucket) error {
		s.latestID = bucket.Sequence()
		return nil
	})

	return nil
}

func (s *Storage) Close() error {
	return s.bolt.Close()
}

func (s *Storage) SaveBinLogPosition(binLog common.BinLogPosition) {
	positionPath := filepath.Join(s.settings.Storage, common.PositionFilename)
	if err := utils.WriteSettings(binLog, positionPath); err != nil {
		s.logger.Error(err.Error())
	}
	s.logger.Info("[Storage]binlog position saved", zap.String("file", binLog.File), zap.Uint32("position", binLog.Position), zap.Uint64("latestID", s.latestID))
}

func (s *Storage) ReadBinLogPosition() common.BinLogPosition {
	var savedBinLog common.BinLogPosition
	positionPath := filepath.Join(s.settings.Storage, common.PositionFilename)

	if io_utils.IsFile(positionPath) {
		if err := utils.LoadSettings(&savedBinLog, positionPath); err == nil {
			return savedBinLog
		}
	}

	return common.BinLogPosition{}
}

func (s *Storage) GetLatestBinLogPosition(currentBinLog common.BinLogPosition) common.BinLogPosition {
	savedBinLog := s.ReadBinLogPosition()
	if savedBinLog.GreaterThan(currentBinLog) {
		return savedBinLog
	}

	return currentBinLog
}

func (s *Storage) ReadTables() {
	if err := s.bolt.Bucket(common.StorageTables).ForEach(func(kv utils.KV) error {
		var table schema.Table
		if err := text_utils.GobDecode(kv.Value, &table); err != nil {
			s.logger.Error(fmt.Sprintf("[Storage]read table \"%s\" error", kv.Key), zap.Error(err))
		} else {
			s.tables[kv.Key] = &table
		}
		return nil
	}); err != nil {
		s.logger.Error("[Storage]read tables error", zap.Error(err))
	}
}

func (s *Storage) GetTable(table *schema.Table) (string, *schema.Table) {
	tableName := common.BuildTableName(table.Schema, table.Name, table.Columns)

	if t, ok := s.tables[tableName]; ok {
		return tableName, t
	}

	s.tables[tableName] = table                                            // 存储table的快照结构
	s.tables[common.BuildTableName(table.Schema, table.Name, nil)] = table // 存储Schema.Table的结构

	if err := s.bolt.Bucket(common.StorageTables).Set(tableName, table); err != nil {
		s.logger.Error("[Storage]table write to storage error", zap.Error(err))
	}

	return tableName, table
}

func (s *Storage) SaveEvents(events []consumer.RowEvent) {
	if len(events) <= 0 {
		return
	}
	if err := s.bolt.Bucket(common.StorageEvents).Batch(func(bucket *bbolt.Bucket) error {
		for _, event := range events {
			id, err := bucket.NextSequence()
			if err != nil {
				return errors.WithStack(err)
			}
			key := fmt.Sprintf("%020d/%s/%s", id, common.BuildTableName(event.Schema, event.Table, nil), event.Action)
			event.ID = id

			buf, err := text_utils.GobEncode(event)
			if err != nil {
				s.logger.Error(fmt.Sprintf("[Storage]encode event \"%s\" error", key), zap.Error(err))
				buf = nil
			}
			if err = bucket.Put([]byte(key), buf); err != nil {
				s.logger.Error(fmt.Sprintf("[Storage]write event \"%s\" error", key), zap.Error(err))
			}
		}
		s.latestID = bucket.Sequence()
		return nil
	}); err != nil {
		s.logger.Error(fmt.Sprintf("[Storage]writed %d events of \"%s\" error", len(events), events[0].Action), zap.Error(err))
	}

	s.logger.Info(fmt.Sprintf("[Storage]writed %d events of \"%s\"", len(events), events[0].Action))
}

func (s *Storage) ClearEvents() {
	if err := s.bolt.Bucket(common.StorageEvents).Clear(); err != nil {
		s.logger.Error("[Storage]clear events bucket error", zap.Error(err))
	}
}

func (s *Storage) EventCount() uint64 {
	return uint64(s.bolt.Bucket(common.StorageEvents).Count())
}

func (s *Storage) LatestID() uint64 {
	return s.latestID
}
