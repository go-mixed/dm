package main

import (
	"context"
	"github.com/fly-studio/dm/src/component"
	"github.com/fly-studio/dm/src/consumer"
	"github.com/fly-studio/dm/src/mysql"
	conf "github.com/fly-studio/dm/src/settings"
	"github.com/fly-studio/dm/src/storage"
	"github.com/fly-studio/dm/src/target"
	"github.com/fly-studio/dm/src/task"
	"github.com/spf13/cobra"
	"go-common/utils"
	"go-common/utils/core"
	"go-common/utils/io"
	"go.uber.org/zap"
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

func readSettings(_configFile, _logPath string) *conf.Settings {
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

func buildLogger(options utils.LoggerOptions) *utils.Logger {
	// 初始化日志
	logger, err := utils.NewLogger(options)
	if err != nil {
		panic(err.Error())
	}
	logger.Info("Loaded settings")

	return logger
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
	core.ListenStopSignal(ctx, cancel)

	// always block run except called cancel()
	t.Run(ctx)
}

func export(components *component.Components) {
	consumer.SetLogger(components.Logger)
	consumer.SetRedis(components.Target.Redis)
	consumer.SetEtcd(components.Target.Etcd)
	consumer.SetGetTableFn(components.Storage.GetTable)
	consumer.Export()
}

func run(_configFile, _logPath string) {
	components := &component.Components{}
	defer func() {
		if err := components.Close(); err != nil && components.Logger != nil {
			components.Logger.Error("components close error", zap.Error(err))
		}
	}()

	components.Settings = readSettings(_configFile, _logPath)
	components.Logger = buildLogger(components.Settings.LoggerOptions)
	components.Mysql = buildMySql(components)
	components.Target = buildTarget(components)
	components.Storage = buildStorage(components)

	// 一定要在task之前运行
	export(components)

	runTask(components)

	components.Logger.Info("application exit.")
}
