package storage

import (
	"go.uber.org/zap"
	"gopkg.in/go-mixed/dm.v1/src/common"
	"gopkg.in/go-mixed/dm.v1/src/settings"
	"gopkg.in/go-mixed/go-common.v1/logger.v1"
	"gopkg.in/go-mixed/go-common.v1/utils/time"
	"gopkg.in/yaml.v3"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"
)

type positionStatus struct {
	// 最后一条消费的binlog，内存模式启动时使用此pos
	consumeBinLogPosition common.BinLogPosition
	// 最后一条canal的binlog，文件模式启动时使用此pos
	// 如果上次是内存模式，启动时会将canalBinLogPosition设置为consumeBinLogPosition
	canalBinLogPosition common.BinLogPosition
	// 记录当前存储模式
	memoryMode bool
	// storage中存储的最后一个event ID
	latestEventID atomic.Int64
	// 由于使用的延时删除，所以需要记录下一个消费的ID
	nextConsumeEventID atomic.Int64
	// 当前事件的数量（不精确）
	eventCount atomic.Int64

	fd     *os.File
	logger *logger.Logger
	ticker *time_utils.Ticker
}

type posStatusFile struct {
	ConsumeBinLogPosition common.BinLogPosition `json:"consume_bin_log_position" yaml:"consume_bin_log_position"`
	CanalBinLogPosition   common.BinLogPosition `json:"canal_bin_log_position" yaml:"canal_bin_log_position"`
	LatestEventID         int64                 `json:"latest_event_id" yaml:"latest_event_id"`
	NextConsumeEventID    int64                 `json:"next_consume_event_id" yaml:"next_consume_event_id"`
	MemoryMode            bool                  `json:"memory_mode" yaml:"memory_mode"`
	At                    time.Time             `json:"at" yaml:"at"`
}

func buildPositionStatus(logger *logger.Logger) positionStatus {
	return positionStatus{
		consumeBinLogPosition: common.BinLogPosition{},
		latestEventID:         atomic.Int64{},
		nextConsumeEventID:    atomic.Int64{},
		eventCount:            atomic.Int64{},
		memoryMode:            false,
		logger:                logger,
	}
}

func (s *positionStatus) Initial(options settings.StorageOptions, eventCount int64) (err error) {
	s.fd, err = os.OpenFile(filepath.Join(options.Dir, common.StoragePositionStatusFilename), os.O_CREATE|os.O_RDWR, 0o644)
	s.ticker = time_utils.NewTicker(options.PositionStatusSyncInterval, s.sync, 1)
	s.eventCount.Store(eventCount)
	return
}

func (s *positionStatus) Close() error {
	if s.ticker != nil {
		s.ticker.Stop()
	}
	if s.fd != nil {
		return s.fd.Close()
	}
	return nil
}

func (s *positionStatus) load(currentMemoryMode bool) {
	s.fd.Seek(0, io.SeekStart)
	buf, err := io.ReadAll(s.fd)
	if err != nil {
		s.logger.Error("[StoragePositionStatus]load the config of positions error", zap.Error(err))
		return
	}

	// 新文件
	if len(buf) <= 0 {
		return
	}
	var psf posStatusFile
	if err = yaml.Unmarshal(buf, &psf); err != nil {
		s.logger.Error("[StoragePositionStatus]yaml decode error", zap.Error(err))
		return
	}

	s.from(psf)

	// 如果上次启动是内存模式，则不存在历史events
	// 将canal的position修正为consume的position，以便从正确的position开始dump
	if s.memoryMode {
		s.canalBinLogPosition = s.consumeBinLogPosition
	}

	// 修改为当前模式
	s.memoryMode = currentMemoryMode
}

func (s *positionStatus) sync() {
	s.fd.Sync()

	s.logger.Info("[StoragePositionStatus]saved",
		zap.Any("consume", s.consumeBinLogPosition),
		zap.Any("canal", s.canalBinLogPosition),
		zap.Int64("event id", s.LatestEventID()),
		zap.Int64("next consume id", s.NextConsumeEventID()),
	)
}

func (s *positionStatus) save() {
	buf, err := yaml.Marshal(s.to())
	if err != nil {
		s.logger.Error("[StoragePositionStatus]yaml encode error", zap.Error(err))
		return
	}

	currentLen, _ := s.fd.Seek(0, io.SeekEnd)
	if int64(len(buf)) < currentLen {
		s.fd.Truncate(int64(len(buf)))
	}
	s.fd.Seek(0, io.SeekStart)

	if _, err = s.fd.Write(buf); err != nil {
		s.logger.Error("[StoragePositionStatus]save the config of positions error", zap.Error(err))
		return
	}
}

func (s *positionStatus) UpdateNextConsumeEventID(u int64) *positionStatus {
	s.nextConsumeEventID.Store(u)
	s.save()
	return s
}
func (s *positionStatus) AddLatestEventID(delta int64) int64 {
	n := s.latestEventID.Add(delta)
	s.save()
	return n
}
func (s *positionStatus) AddEventCount(delta int64) int64 {
	n := s.eventCount.Add(delta)
	s.save()
	return n
}

func (s *positionStatus) UpdateConsumeBinLogPosition(position common.BinLogPosition) {
	s.consumeBinLogPosition = position
	s.save()
}

func (s *positionStatus) UpdateCanalBinLogPosition(position common.BinLogPosition) {
	s.canalBinLogPosition = position
	s.save()
}

func (s *positionStatus) CanalBinLogPosition() common.BinLogPosition {
	return s.canalBinLogPosition
}

func (s *positionStatus) ConsumeBinLogPosition() common.BinLogPosition {
	return s.consumeBinLogPosition
}

func (s *positionStatus) NextConsumeEventID() int64 {
	return s.nextConsumeEventID.Load()
}

func (s *positionStatus) EventCount() int64 {
	return s.eventCount.Load()
}

func (s *positionStatus) LatestEventID() int64 {
	return s.latestEventID.Load()
}

func (s *positionStatus) from(sc posStatusFile) {
	s.canalBinLogPosition = sc.CanalBinLogPosition
	s.consumeBinLogPosition = sc.ConsumeBinLogPosition
	s.latestEventID.Store(sc.LatestEventID)
	s.nextConsumeEventID.Store(sc.NextConsumeEventID)
	s.memoryMode = sc.MemoryMode
}

func (s *positionStatus) to() posStatusFile {
	return posStatusFile{
		CanalBinLogPosition:   s.canalBinLogPosition,
		ConsumeBinLogPosition: s.consumeBinLogPosition,
		LatestEventID:         s.LatestEventID(),
		NextConsumeEventID:    s.NextConsumeEventID(),
		MemoryMode:            s.memoryMode,
		At:                    time.Now(),
	}
}
