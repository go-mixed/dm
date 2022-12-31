package canal

import (
	"context"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/pingcap/errors"
	"github.com/siddontang/go-log/log"
	"gopkg.in/go-mixed/dm.v1/src/common"
	"gopkg.in/go-mixed/dm.v1/src/component"
	"gopkg.in/go-mixed/go-common.v1/utils/core"
	io_utils "gopkg.in/go-mixed/go-common.v1/utils/io"
	"path/filepath"
	"runtime"
	"time"
)

type Canal struct {
	*component.Components

	canal *canal.Canal
}

func NewCanal(components *component.Components, handler canal.EventHandler) (*Canal, error) {
	zone, err := time.LoadLocation(components.Settings.MySqlOptions.TimeZone)
	if err != nil {
		panic(err.Error())
	}

	cfg := &canal.Config{
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
		Logger:                  nil,
	}

	streamHandler, _ := log.NewTimeRotatingFileHandler(filepath.Join(filepath.Dir(components.Settings.LoggerOptions.FilePath), common.LogCanalFilename), log.WhenDay, 1)
	cfg.Logger = log.NewDefault(streamHandler)

	c, err := canal.NewCanal(cfg)
	if err != nil {
		return nil, err
	}

	_c := &Canal{
		Components: components,
		canal:      c,
	}

	c.SetEventHandler(handler)

	return _c, nil
}

func (c *Canal) Start(binlog common.BinLogPosition) error {
	return errors.WithStack(c.canal.RunFrom(binlog.ToMysqlPos()))
}

func (c *Canal) Stop() {
	c.canal.Close()
}

func (c *Canal) Wait(ctx context.Context) {
	select {
	case <-ctx.Done():
	case <-c.canal.Ctx().Done():
	}
}
