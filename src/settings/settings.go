package settings

import (
	"go-common/utils"
	"go-common/utils/io"
	"path/filepath"
)

type Settings struct {
	MySqlOptions    MySqlOptions    `yaml:"mysql"`
	DumplingOptions DumplingOptions `yaml:"dumpling"`
	TargetOptions   TargetOptions   `yaml:"targets"`
	TaskOptions     TaskOptions     `yaml:"task"`

	Storage       string              `yaml:"storage"`
	LoggerOptions utils.LoggerOptions `yaml:"log"`
}

func LoadSettings(confPath string) (*Settings, error) {
	conf := &Settings{
		MySqlOptions:    defaultMySqlOptions(),
		DumplingOptions: defaultDumplingOptions(),
		TaskOptions:     defaultTaskOptions(),
		TargetOptions:   defaultTargetOptions(),

		Storage:       filepath.Join(io_utils.GetCurrentDir(), "storage"),
		LoggerOptions: utils.DefaultLoggerOptions(),
	}

	if err := utils.LoadSettings(conf, confPath); err != nil {
		return nil, err
	}

	return conf, checkSettings(conf)
}

func checkSettings(conf *Settings) error {
	if err := conf.TaskOptions.Initial(); err != nil {
		return err
	}
	return nil
}
