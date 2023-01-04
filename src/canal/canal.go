package canal

import (
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/pingcap/errors"
	"github.com/siddontang/go-log/log"
	"gopkg.in/go-mixed/dm.v1/src/common"
	"gopkg.in/go-mixed/dm.v1/src/component"
	"gopkg.in/go-mixed/go-common.v1/utils/core"
	"gopkg.in/go-mixed/go-common.v1/utils/io"
	"path/filepath"
	"runtime"
	"time"
)

type Canal struct {
	*component.Components

	canal    *canal.Canal
	handler  canal.EventHandler
	canalCfg *canal.Config
}

func NewCanal(components *component.Components, handler canal.EventHandler) *Canal {
	zone, err := time.LoadLocation(components.Settings.MySqlOptions.TimeZone)
	if err != nil {
		panic(err.Error())
	}

	streamHandler, _ := log.NewTimeRotatingFileHandler(filepath.Join(filepath.Dir(components.Settings.LoggerOptions.FilePath), common.LogCanalFilename), log.WhenDay, 1)
	canalLogger := log.NewDefault(streamHandler)

	return &Canal{
		Components: components,
		handler:    handler,
		canalCfg: &canal.Config{
			Addr:                  components.Settings.MySqlOptions.Host,
			User:                  components.Settings.MySqlOptions.Username,
			Password:              components.Settings.MySqlOptions.Password,
			Charset:               components.Settings.MySqlOptions.Charset,
			ServerID:              components.Settings.MySqlOptions.ServerID,
			Flavor:                components.Settings.MySqlOptions.Flavor,
			HeartbeatPeriod:       components.Settings.MySqlOptions.HeartbeatPeriod,
			ReadTimeout:           components.Settings.MySqlOptions.ReadTimeout,
			IncludeTableRegex:     components.Settings.TaskOptions.GetTablePatterns(),
			ExcludeTableRegex:     nil,
			DiscardNoMetaRowEvent: false,
			Dump: canal.DumpConfig{
				ExecutionPath:  filepath.Join(io_utils.GetCurrentDir(), "third-party", "mysql", core.If(runtime.GOOS == "windows", "mysqldump.exe", "mysqldump")),
				DiscardErr:     false,
				SkipMasterData: true,
			},
			UseDecimal:              false,
			ParseTime:               false,
			TimestampStringLocation: zone,
			SemiSyncEnabled:         false,
			MaxReconnectAttempts:    components.Settings.MySqlOptions.MaxReconnectAttempts,
			DisableRetrySync:        false,
			TLSConfig:               nil,
			Logger:                  canalLogger,
		},
	}
}

func (c *Canal) Start(binlog common.BinLogPosition) error {
	c.Stop()

	// 原版一个canal实例的只能运行一次，本canal实例可以支持多次启动和停止，所以在Start的时候才NewCanal对象
	var err error
	c.canal, err = canal.NewCanal(c.canalCfg)
	if err != nil {
		return errors.WithStack(err)
	}
	c.canal.SetEventHandler(c.handler)
	return errors.WithStack(c.canal.RunFrom(binlog.ToMysqlPos()))
}

func (c *Canal) Stop() {
	if c.canal != nil {
		c.canal.Close()
	}
}
