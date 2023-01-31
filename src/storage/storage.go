package storage

import (
	"fmt"
	obadger "github.com/dgraph-io/badger/v3"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/pkg/errors"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"gopkg.in/go-mixed/dm-consumer.v1"
	"gopkg.in/go-mixed/dm.v1/src/common"
	"gopkg.in/go-mixed/dm.v1/src/settings"
	"gopkg.in/go-mixed/go-common.v1/badger.v1"
	"gopkg.in/go-mixed/go-common.v1/logger.v1"
	"gopkg.in/go-mixed/go-common.v1/utils"
	"gopkg.in/go-mixed/go-common.v1/utils/text"
	"gopkg.in/go-mixed/go-common.v1/utils/time"
	"path/filepath"
)

type Storage struct {
	settings *settings.Settings
	logger   *logger.Logger

	db *badger.Badger

	tables map[string]*schema.Table

	PositionStatus  positionStatus
	gcTicker        *time_utils.Ticker
	flushTicker     *time_utils.Ticker
	lastConsumedKey string
}

func NewStorage(settings *settings.Settings, logger *logger.Logger) (*Storage, error) {
	// 运行在内存中的lsm树
	db := badger.NewBadger(filepath.Join(settings.StorageOptions.Dir, "data"), logger.Sugar(), settings.StorageOptions.MemoryMode).SetEncoderFunc(text_utils.GobEncode).SetDecoderFunc(text_utils.GobDecode)

	s := &Storage{
		settings: settings,
		logger:   logger,
		db:       db,
		tables:   make(map[string]*schema.Table),

		PositionStatus: buildPositionStatus(logger),
	}
	return s, nil
}

func (s *Storage) Initial() error {
	eventCount := s.db.Bucket(common.StorageEventBucket).Count() // 读取badger中剩余的数据
	if err := s.PositionStatus.Initial(s.settings.StorageOptions, eventCount); err != nil {
		return err
	}
	s.PositionStatus.load(s.settings.StorageOptions.MemoryMode)

	s.gcTicker = time_utils.NewTicker(s.settings.StorageOptions.GCInterval, s.gcHandle, 1)
	if !s.settings.StorageOptions.IsImmediateFlush() {
		s.flushTicker = time_utils.NewTicker(s.settings.StorageOptions.FlushInterval, s.flushHandle, 1)
	}

	return nil
}

func (s *Storage) Close() error {
	if s.flushTicker != nil {
		s.flushTicker.Stop()
	}
	if s.gcTicker != nil {
		s.gcTicker.Stop()
	}
	err := s.PositionStatus.Close()
	return multierr.Append(err, s.db.Close())
}

func (s *Storage) GetLatestBinLogPosition(currentBinLog common.BinLogPosition) common.BinLogPosition {
	var newPos common.BinLogPosition

	// 内存模式取latestConsumeBinLogPosition，文件模式取latestCanalBinLogPosition
	if s.settings.StorageOptions.MemoryMode {
		newPos = s.PositionStatus.consumeBinLogPosition
	} else {
		newPos = s.PositionStatus.canalBinLogPosition
	}

	if newPos.GreaterThan(currentBinLog) {
		return newPos
	}

	return currentBinLog
}

// GetTable 通过别名获取table的结构
func (s *Storage) GetTable(alias string) *schema.Table {
	table, ok := s.tables[alias]
	if !ok {
		table, _ = s.tables[common.CleanTableName(alias)]
	}
	return table
}

// UpdateAndGetTableAlias 保存当前table，并返回别名
func (s *Storage) UpdateAndGetTableAlias(table *schema.Table) string {
	tableName := common.BuildTableName(table.Schema, table.Name, table.Columns)

	if _, ok := s.tables[tableName]; ok {
		return tableName
	}

	s.tables[tableName] = table                                            // 存储table的快照结构
	s.tables[common.BuildTableName(table.Schema, table.Name, nil)] = table // 存储Schema.Table的结构

	return tableName
}

// AddEvents 将binlog事件添加到storage中
func (s *Storage) AddEvents(events []consumer.RowEvent) {
	l := len(events)
	if l <= 0 {
		return
	}

	if err := s.db.Bucket(common.StorageEventBucket).Update(func(txn *obadger.Txn) error {
		for _, event := range events {
			id := s.PositionStatus.AddLatestEventID(1)
			key := common.BuildEventKey(id, event.Schema, event.Table, event.Action)
			event.ID = id

			buf, err := s.db.EncoderFunc(event)
			if err != nil {
				s.logger.Error(fmt.Sprintf("[Storage]encode event \"%s\" error", key), zap.Error(err))
				return err
			}
			if err = txn.Set([]byte(key), buf); err != nil {
				s.logger.Error(fmt.Sprintf("[Storage]write event \"%s\" error", key), zap.Error(err))
				return errors.WithStack(err)
			}
		}

		return nil
	}); err != nil {
		if errors.Is(err, utils.ErrForEachQuit) {
			return
		}
		s.logger.Error(fmt.Sprintf("[Storage]write %d events of %s error", l, events[0].Action), zap.Error(err))
		return
	}

	s.PositionStatus.AddEventCount(int64(l))
	s.logger.Debug(fmt.Sprintf("[Storage]wrote %d events of %s", l, events[0].Action), zap.Int64("latest event id", s.PositionStatus.LatestEventID()))
	return
}

func (s *Storage) AddCanalBinLogPosition(pos common.BinLogPosition) {
	if pos.IsEmpty() {
		return
	}

	// 将binlog加入到badger
	key := common.BuildBinLogKey(s.PositionStatus.AddLatestEventID(1), pos)
	if err := s.db.Bucket(common.StorageEventBucket).Set(key, pos); err != nil {
		s.logger.Error(fmt.Sprintf("[Storage]write binlog position \"%s\" error", key), zap.Error(err))
	}

	s.PositionStatus.AddEventCount(1)
	s.PositionStatus.UpdateCanalBinLogPosition(pos)

	s.logger.Debug("[Storage]canal binlog pos", zap.String("file", pos.File), zap.Uint32("position", pos.Position))
}

func (s *Storage) UpdateConsumeBinLogPosition(pos common.BinLogPosition) {
	if pos.IsEmpty() {
		return
	}

	s.PositionStatus.UpdateConsumeBinLogPosition(pos)
}

func (s *Storage) EventForEach(keyStart string, callback func(key string, event consumer.RowEvent) error) (startKey, endKey string, lastPos common.BinLogPosition, err error) {
	var pos common.BinLogPosition
	if _, _, err = s.db.Bucket(common.StorageEventBucket).RangeCallback(keyStart, "", "", s.settings.TaskOptions.MaxBulkSize, func(txn *obadger.Txn, kv *utils.KV) error {
		var err1 error
		// 是 binlog position
		if common.IsBinLogKey(kv.Key) {
			if err1 = s.db.DecoderFunc(kv.Value, &pos); err != nil {
				return err1
			}
			lastPos = pos
		} else {
			var event consumer.RowEvent
			if err1 = s.db.DecoderFunc(kv.Value, &event); err1 != nil {
				return err1
			}
			if err1 = callback(kv.Key, event); err1 != nil {
				return err1
			}
		}

		if startKey == "" {
			startKey = kv.Key
		}
		// 非错误的（含主动跳出）的key为最后遍历的key
		endKey = kv.Key
		return nil
	}); err != nil {
		return "", "", common.BinLogPosition{}, err
	}

	return startKey, endKey, lastPos, nil
}

func (s *Storage) DeleteEventsUtil(keyEnd string) {
	n, err := s.db.Bucket(common.StorageEventBucket).DeleteRange("", keyEnd, "")
	if err != nil {
		s.logger.Error("[Storage]delete events error", zap.Error(err))
	} else {
		s.PositionStatus.AddEventCount(-n)
		s.logger.Info(fmt.Sprintf("[Storage]deleted %d events to key: %s", n, keyEnd))
	}
}

func (s *Storage) FlushTo(keyEnd string) {
	s.lastConsumedKey = keyEnd

	if s.settings.StorageOptions.IsImmediateFlush() {
		s.flushHandle()
	}
}

func (s *Storage) flushHandle() {
	s.DeleteEventsUtil(s.lastConsumedKey)
}

func (s *Storage) gcHandle() {
	s.logger.Info("badger GC")
	s.db.GC()
	s.logger.Info("badger GC completed")
}
