package main

import (
	"context"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"gopkg.in/go-mixed/dm.v1/src/component"
	"gopkg.in/go-mixed/dm.v1/src/exporter"
	"gopkg.in/go-mixed/dm.v1/src/mysql"
	conf "gopkg.in/go-mixed/dm.v1/src/settings"
	"gopkg.in/go-mixed/dm.v1/src/storage"
	"gopkg.in/go-mixed/dm.v1/src/target"
	"gopkg.in/go-mixed/dm.v1/src/task"
	"gopkg.in/go-mixed/go-common.v1/logger.v1"
	"gopkg.in/go-mixed/go-common.v1/utils/core"
	"gopkg.in/go-mixed/go-common.v1/utils/io"
	"path/filepath"
)

func main() {
	currentDir := io_utils.GetCurrentDir()
	rootCmd := &cobra.Command{
		Use:   "dm",
		Short: "read binlog from Canal(MySQL), and transfer with golang plugin",
		Run: func(cmd *cobra.Command, args []string) {
			config, _ := cmd.PersistentFlags().GetString("config")
			log, _ := cmd.PersistentFlags().GetString("log")
			run(config, log)
		},
	}

	// 读取CLI
	rootCmd.PersistentFlags().StringP("config", "c", filepath.Join(currentDir, "conf/settings.yml"), "config file")
	err := rootCmd.Execute()
	if err != nil {
		panic(err.Error())
	}
}

func readSettings(_configFile string) *conf.Settings {
	// 读取配置文件
	settings, err := conf.LoadSettings(_configFile)
	if err != nil {
		panic(err.Error())
	}

	if settings == nil {
		panic("read settings fatal.")
	}

	return settings
}

func buildLogger(options logger.LoggerOptions) *logger.Logger {
	// 初始化日志
	l, err := logger.NewLogger(options)
	if err != nil {
		panic(err.Error())
	}
	l.Info("Loaded settings")

	return l
}

func buildMySql(components *component.Components) *mysql.MySql {
	_mysql := mysql.NewMySql(components.Settings, components.Logger)
	if err := _mysql.Connect(); err != nil {
		panic(err.Error())
	}

	return _mysql
}

func buildTarget(components *component.Components) *target.Target {
	_target := target.NewTarget(components.Settings, components.Logger)
	if err := _target.Connect(); err != nil {
		panic(err.Error())
	}

	return _target
}

func buildStorage(components *component.Components) *storage.Storage {
	_storage, err := storage.NewStorage(components.Settings, components.Logger)
	if err != nil {
		panic(err.Error())
	}
	if err = _storage.Initial(); err != nil {
		panic(err.Error())
	}

	return _storage
}

func runTask(components *component.Components) {
	t := task.NewTask(components)

	if err := t.Initial(); err != nil {
		panic(err.Error())
	}

	// stop when ctrl+c
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	core.ListenStopSignal(ctx, func() {
		components.Logger.Info("signal: ctrl+c")
		cancel()
	})

	// always block run except called cancel()
	t.Run(ctx)
}

func export(components *component.Components) {
	exporter.SetLogger(components.Logger)
	exporter.SetRedis(components.Target.Redis)
	exporter.SetGetTableFn(components.Storage.GetTable)
	exporter.Export()
}

func run(_configFile, _logPath string) {
	components := &component.Components{}
	defer func() {
		if err := components.CloseComponents(); err != nil && components.Logger != nil {
			components.Logger.Error("components close error", zap.Error(err))
		}
	}()

	components.Settings = readSettings(_configFile)
	if _logPath != "" {
		components.Settings.LoggerOptions.FilePath = _logPath
	}
	components.Logger = buildLogger(components.Settings.LoggerOptions)
	components.Mysql = buildMySql(components)
	components.Target = buildTarget(components)
	components.Storage = buildStorage(components)

	// 一定要在task之前运行
	export(components)

	runTask(components) // 阻塞运行

	components.Logger.Info("application exit.")
}
