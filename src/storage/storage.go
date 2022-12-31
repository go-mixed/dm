package storage

import (
	"fmt"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/pkg/errors"
	"go.etcd.io/bbolt"
	"go.uber.org/zap"
	consumer "gopkg.in/go-mixed/dm-consumer.v1"
	"gopkg.in/go-mixed/dm.v1/src/common"
	"gopkg.in/go-mixed/dm.v1/src/settings"
	"gopkg.in/go-mixed/go-common.v1/conf.v1"
	"gopkg.in/go-mixed/go-common.v1/logger.v1"
	"gopkg.in/go-mixed/go-common.v1/storage.v1"
	"gopkg.in/go-mixed/go-common.v1/utils"
	"gopkg.in/go-mixed/go-common.v1/utils/io"
	"gopkg.in/go-mixed/go-common.v1/utils/text"
	"path/filepath"
)

type Storage struct {
	settings *settings.Settings
	logger   *logger.Logger

	bolt *storage.Bolt

	tables map[string]*schema.Table

	latestID uint64
}

func NewStorage(settings *settings.Settings, logger *logger.Logger) (*Storage, error) {
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
	if err := conf.WriteSettings(binLog, positionPath); err != nil {
		s.logger.Error(err.Error())
	}
	s.logger.Info("[Storage]binlog position saved", zap.String("file", binLog.File), zap.Uint32("position", binLog.Position), zap.Uint64("latestID", s.latestID))
}

func (s *Storage) ReadBinLogPosition() common.BinLogPosition {
	var savedBinLog common.BinLogPosition
	positionPath := filepath.Join(s.settings.Storage, common.PositionFilename)

	if io_utils.IsFile(positionPath) {
		if err := conf.LoadSettings(&savedBinLog, positionPath); err == nil {
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
	if _, err := s.bolt.Bucket(common.StorageTables).ForEach(func(bucket *bbolt.Bucket, kv *utils.KV) error {
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

// GetTable 通过别名获取table的结构
func (s *Storage) GetTable(alias string) *schema.Table {
	table, ok := s.tables[alias]
	if !ok {
		table, _ = s.tables[common.CleanTableName(alias)]
	}
	return table
}

// SaveAndGetTableAlias 保存当前table，并返回别名
func (s *Storage) SaveAndGetTableAlias(table *schema.Table) string {
	tableName := common.BuildTableName(table.Schema, table.Name, table.Columns)

	if _, ok := s.tables[tableName]; ok {
		return tableName
	}

	s.tables[tableName] = table                                            // 存储table的快照结构
	s.tables[common.BuildTableName(table.Schema, table.Name, nil)] = table // 存储Schema.Table的结构

	if err := s.bolt.Bucket(common.StorageTables).Set(tableName, table); err != nil {
		s.logger.Error("[Storage]table write to storage error", zap.Error(err))
	}

	return tableName
}

// SaveEvents 保存binlog事件到storage
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
			key := common.BuildEventKey(id, event.Schema, event.Table, event.Action)
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

// ClearEvents 清除在storage中所有binlog事件
func (s *Storage) ClearEvents() {
	if err := s.bolt.Bucket(common.StorageEvents).Clear(); err != nil {
		s.logger.Error("[Storage]clear events bucket error", zap.Error(err))
	}
}

// EventCount 当前在storage中缓存的binlog事件数量
func (s *Storage) EventCount() uint64 {
	return uint64(s.bolt.Bucket(common.StorageEvents).Count())
}

func (s *Storage) LatestID() uint64 {
	return s.latestID
}

func (s *Storage) EventForEach(keyStart string, callback func(key string, event consumer.RowEvent) bool) string {
	nextKey, _, err := s.bolt.Bucket(common.StorageEvents).RangeCallback(keyStart, "", "", int64(s.settings.TaskOptions.MaxBulkSize), func(bucket *bbolt.Bucket, kv *utils.KV) error {
		var event consumer.RowEvent
		if err := text_utils.GobDecode(kv.Value, &event); err != nil {
			return err
		}
		if !callback(kv.Key, event) { // 返回false跳出循环
			return storage.ErrForEachBreak
		}

		return nil
	})

	if err != nil && !errors.Is(err, storage.ErrForEachBreak) {
		s.logger.Error("[Storage]for each of event error", zap.Error(err))
	}

	return nextKey
}

func (s *Storage) DeleteEventsTo(toKey string) {
	n, err := s.bolt.Bucket(common.StorageEvents).BatchDeleteRange("", toKey, "")
	if err != nil {
		s.logger.Error("[Storage]delete events error", zap.Error(err))
	} else {
		s.logger.Debug(fmt.Sprintf("[Storage]deleted %d events to key: %s", n, toKey))
	}
}
