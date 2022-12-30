package consumer

import (
	"github.com/pkg/errors"
	"go-common/utils"
	"go-common/utils/core"
)

type ILogger interface {
	Fatal(v ...any)
	Fatalf(format string, v ...any)
	Error(v ...any)
	Errorf(format string, v ...any)
	Panic(v ...any)
	Panicf(format string, v ...any)

	Debug(v ...any)
	Debugf(format string, v ...any)
	Info(v ...any)
	Infof(format string, v ...any)
	Warn(v ...any)
	Warnf(format string, v ...any)
}

type iLogger struct {
	logger utils.ILogger
}

func ToConsumerILogger(logger utils.ILogger) ILogger {
	// struct 转为 interface后，nil需要反射判断
	if core.IsInterfaceNil(logger) {
		return &iLogger{nil}
	}
	return &iLogger{logger}
}

func (i iLogger) Fatal(v ...any) {
	if i.logger == nil {
		panic(errors.New("logger is nil in \"Fatal\""))
	}
	i.logger.Fatal(v...)
}

func (i iLogger) Fatalf(format string, v ...any) {
	if i.logger == nil {
		panic(errors.New("logger is nil in \"Fatalf\""))
	}
	i.logger.Fatalf(format, v...)
}

func (i iLogger) Error(v ...any) {
	if i.logger == nil {
		panic(errors.New("logger is nil in \"Error\""))
	}
	i.logger.Error(v...)
}

func (i iLogger) Errorf(format string, v ...any) {
	if i.logger == nil {
		panic(errors.New("logger is nil in \"Errorf\""))
	}
	i.logger.Errorf(format, v...)
}

func (i iLogger) Panic(v ...any) {
	if i.logger == nil {
		panic(errors.New("logger is nil in \"Panic\""))
	}
	i.logger.Panic(v...)
}

func (i iLogger) Panicf(format string, v ...any) {
	if i.logger == nil {
		panic(errors.New("logger is nil in \"Panicf\""))
	}
	i.logger.Panicf(format, v...)
}

func (i iLogger) Debug(v ...any) {
	if i.logger == nil {
		panic(errors.New("logger is nil in \"Debug\""))
	}
	i.logger.Debug(v...)
}

func (i iLogger) Debugf(format string, v ...any) {
	if i.logger == nil {
		panic(errors.New("logger is nil in \"Debugf\""))
	}
	i.logger.Debugf(format, v...)
}

func (i iLogger) Info(v ...any) {
	if i.logger == nil {
		panic(errors.New("logger is nil in \"Info\""))
	}
	i.logger.Info(v...)
}

func (i iLogger) Infof(format string, v ...any) {
	if i.logger == nil {
		panic(errors.New("logger is nil in \"Infof\""))
	}
	i.logger.Infof(format, v...)
}

func (i iLogger) Warn(v ...any) {
	if i.logger == nil {
		panic(errors.New("logger is nil in \"Warn\""))
	}
	i.logger.Warn(v...)
}

func (i iLogger) Warnf(format string, v ...any) {
	if i.logger == nil {
		panic(errors.New("logger is nil in \"Warnf\""))
	}
	i.logger.Warnf(format, v...)
}
