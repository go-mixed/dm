package settings

import (
	"gopkg.in/go-mixed/go-common.v1/conf.v1"
	"gopkg.in/go-mixed/go-common.v1/logger.v1"
	"gopkg.in/go-mixed/go-common.v1/utils/io"
	"path/filepath"
)

type Settings struct {
	MySqlOptions    MySqlOptions    `yaml:"mysql"`
	DumplingOptions DumplingOptions `yaml:"dumpling"`
	TargetOptions   TargetOptions   `yaml:"targets"`
	TaskOptions     TaskOptions     `yaml:"task"`

	Storage       string               `yaml:"storage"`
	LoggerOptions logger.LoggerOptions `yaml:"log"`
}

func LoadSettings(confPath string) (*Settings, error) {
	cfg := &Settings{
		MySqlOptions:    defaultMySqlOptions(),
		DumplingOptions: defaultDumplingOptions(),
		TaskOptions:     defaultTaskOptions(),
		TargetOptions:   defaultTargetOptions(),

		Storage:       filepath.Join(io_utils.GetCurrentDir(), "storage"),
		LoggerOptions: logger.DefaultLoggerOptions(),
	}

	if err := conf.LoadSettings(cfg, confPath); err != nil {
		return nil, err
	}

	return cfg, checkSettings(cfg)
}

func checkSettings(cfg *Settings) error {
	if err := cfg.TaskOptions.Initial(); err != nil {
		return err
	}
	return nil
}
