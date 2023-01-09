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

type conf struct {
	// 最后一条消费的binlog，内存模式启动时使用此pos
	consumeBinLogPosition common.BinLogPosition
	// 最后一条canal的binlog，文件模式启动时使用此pos
	// 如果上次是内存模式，启动时会将canalBinLogPosition设置为consumeBinLogPosition
	canalBinLogPosition common.BinLogPosition
	// 记录当前模式
	memoryMode bool
	// 每条event都有自增ID
	latestEventID atomic.Int64
	// 由于使用的延时删除，所以需要记录下一个消费的ID
	// 不然幻读会导致来不及删除的ID重复消费
	nextConsumeEventID atomic.Int64
	// 当前事件的数量（不精确）
	eventCount atomic.Int64

	fd     *os.File
	logger *logger.Logger
	ticker *time_utils.Ticker
}

type savedConf struct {
	ConsumeBinLogPosition common.BinLogPosition `json:"consume_bin_log_position" yaml:"consume_bin_log_position"`
	CanalBinLogPosition   common.BinLogPosition `json:"canal_bin_log_position" yaml:"canal_bin_log_position"`
	LatestEventID         int64                 `json:"latest_event_id" yaml:"latest_event_id"`
	NextConsumeEventID    int64                 `json:"next_consume_event_id" yaml:"next_consume_event_id"`
	MemoryMode            bool                  `json:"memory_mode" yaml:"memory_mode"`
	At                    time.Time             `json:"at" yaml:"at"`
}

func buildConf(logger *logger.Logger) conf {
	return conf{
		consumeBinLogPosition: common.BinLogPosition{},
		latestEventID:         atomic.Int64{},
		nextConsumeEventID:    atomic.Int64{},
		eventCount:            atomic.Int64{},
		memoryMode:            false,
		logger:                logger,
	}
}

func (c *conf) Initial(options settings.StorageOptions, eventCount int64) (err error) {
	c.fd, err = os.OpenFile(filepath.Join(options.Dir, common.StorageConfFilename), os.O_CREATE|os.O_RDWR, 0o644)
	c.ticker = time_utils.NewTicker(options.ConfSyncTimer, c.sync)
	c.eventCount.Store(eventCount)
	return
}

func (c *conf) Close() error {
	if c.ticker != nil {
		c.ticker.Stop()
	}
	if c.fd != nil {
		return c.fd.Close()
	}
	return nil
}

func (c *conf) load(currentMemoryMode bool) {
	c.fd.Seek(0, io.SeekStart)
	buf, err := io.ReadAll(c.fd)
	if err != nil {
		c.logger.Error("[StorageConf]load conf error", zap.Error(err))
		return
	}

	// 新文件
	if len(buf) <= 0 {
		return
	}
	var sc savedConf
	if err = yaml.Unmarshal(buf, &sc); err != nil {
		c.logger.Error("[StorageConf]yaml decode error", zap.Error(err))
		return
	}

	c.from(sc)

	// 如果上次启动是内存模式，则不存在历史events
	// 将canal的position修正为consume的position，以便从正确的position开始dump
	if c.memoryMode {
		c.canalBinLogPosition = c.consumeBinLogPosition
	}

	// 修改为当前模式
	c.memoryMode = currentMemoryMode
}

func (c *conf) sync() {
	c.fd.Sync()

	c.logger.Info("[StorageConf]saved",
		zap.Any("consume", c.consumeBinLogPosition),
		zap.Any("canal", c.canalBinLogPosition),
		zap.Int64("event id", c.LatestEventID()),
		zap.Int64("next consume id", c.NextConsumeEventID()),
	)
}

func (c *conf) save() {
	buf, err := yaml.Marshal(c.to())
	if err != nil {
		c.logger.Error("[StorageConf]yaml encode error", zap.Error(err))
		return
	}

	currentLen, _ := c.fd.Seek(0, io.SeekEnd)
	if int64(len(buf)) > currentLen {
		c.fd.Truncate(int64(len(buf)))
	}
	c.fd.Seek(0, io.SeekStart)

	if _, err = c.fd.Write(buf); err != nil {
		c.logger.Error("[StorageConf]save conf error", zap.Error(err))
		return
	}
}

func (c *conf) UpdateNextConsumeEventID(u int64) *conf {
	c.nextConsumeEventID.Store(u)
	c.save()
	return c
}
func (c *conf) AddLatestEventID(delta int64) int64 {
	n := c.latestEventID.Add(delta)
	c.save()
	return n
}
func (c *conf) AddEventCount(delta int64) int64 {
	n := c.eventCount.Add(delta)
	c.save()
	return n
}

func (c *conf) UpdateConsumeBinLogPosition(position common.BinLogPosition) {
	c.consumeBinLogPosition = position
	c.save()
}

func (c *conf) UpdateCanalBinLogPosition(position common.BinLogPosition) {
	c.canalBinLogPosition = position
	c.save()
}

func (c *conf) CanalBinLogPosition() common.BinLogPosition {
	return c.canalBinLogPosition
}

func (c *conf) ConsumeBinLogPosition() common.BinLogPosition {
	return c.consumeBinLogPosition
}

func (c *conf) NextConsumeEventID() int64 {
	return c.nextConsumeEventID.Load()
}

func (c *conf) EventCount() int64 {
	return c.eventCount.Load()
}

func (c *conf) LatestEventID() int64 {
	return c.latestEventID.Load()
}

func (c *conf) from(sc savedConf) {
	c.canalBinLogPosition = sc.CanalBinLogPosition
	c.consumeBinLogPosition = sc.ConsumeBinLogPosition
	c.latestEventID.Store(sc.LatestEventID)
	c.nextConsumeEventID.Store(sc.NextConsumeEventID)
	c.memoryMode = sc.MemoryMode
}

func (c *conf) to() savedConf {
	return savedConf{
		CanalBinLogPosition:   c.canalBinLogPosition,
		ConsumeBinLogPosition: c.consumeBinLogPosition,
		LatestEventID:         c.LatestEventID(),
		NextConsumeEventID:    c.NextConsumeEventID(),
		MemoryMode:            c.memoryMode,
		At:                    time.Now(),
	}
}
