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
	// 最后一条修改的binlog，如果是内存模式，重启后使用此pos
	consumeBinLogPosition common.BinLogPosition
	// canal得到的最后一条binlog，如果文件模式，重启后使用此pos
	// 当启动内存模式后，此项会和latestConsumeBinLogPosition相同，以免之后切换成文件模式后，读取的pos错误
	canalBinLogPosition common.BinLogPosition
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
	At                    time.Time             `json:"at" yaml:"at"`
}

func buildConf(logger *logger.Logger) conf {
	return conf{
		consumeBinLogPosition: common.BinLogPosition{},
		latestEventID:         atomic.Int64{},
		nextConsumeEventID:    atomic.Int64{},
		eventCount:            atomic.Int64{},
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

func (c *conf) load() {
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
}

func (c *conf) sync() {
	c.fd.Sync()

	c.logger.Info("[StorageConf]saved",
		zap.String("latest consume file", c.consumeBinLogPosition.File),
		zap.Uint32("latest consume position", c.consumeBinLogPosition.Position),
		zap.String("latest canal file", c.canalBinLogPosition.File),
		zap.Uint32("latest canal position", c.canalBinLogPosition.Position),
		zap.Int64("latest event id", c.LatestEventID()),
		zap.Int64("next consumed event id", c.NextConsumeEventID()),
	)
}

func (c *conf) save() {
	buf, err := yaml.Marshal(c.to())
	if err != nil {
		c.logger.Error("[StorageConf]yaml encode error", zap.Error(err))
		return
	}

	c.fd.Truncate(int64(len(buf)))
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
}

func (c *conf) to() savedConf {
	return savedConf{
		CanalBinLogPosition:   c.canalBinLogPosition,
		ConsumeBinLogPosition: c.consumeBinLogPosition,
		LatestEventID:         c.LatestEventID(),
		NextConsumeEventID:    c.NextConsumeEventID(),
		At:                    time.Now(),
	}
}
