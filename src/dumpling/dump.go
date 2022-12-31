package dumpling

import (
	"context"
	"errors"
	"fmt"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/go-mixed/dm.v1/src/settings"
	"gopkg.in/go-mixed/go-common.v1/cmd.v1"
	"gopkg.in/go-mixed/go-common.v1/logger.v1"
	"gopkg.in/go-mixed/go-common.v1/utils/conv"
	"gopkg.in/go-mixed/go-common.v1/utils/core"
	"gopkg.in/go-mixed/go-common.v1/utils/io"
	"net"
	"path/filepath"
	"runtime"
)

type Dumpling struct {
	settings *settings.Settings
	logger   *logger.Logger
}

func boolToStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

func NewDumpling(settings *settings.Settings, logger *logger.Logger) *Dumpling {
	return &Dumpling{
		settings: settings,
		logger:   logger,
	}
}

func (d *Dumpling) RunDump(ctx context.Context) error {

	host, port, err := net.SplitHostPort(d.settings.MySqlOptions.Host)
	if err != nil {
		return fmt.Errorf("the host \"%s\" error: %w", d.settings.MySqlOptions.Host, err)
	}
	// https://github.com/pingcap/tidb/blob/master/dumpling/export/config.go
	dumpConfig := map[string]string{
		"--host":                      host,
		"--user":                      d.settings.MySqlOptions.Username,
		"--password":                  d.settings.MySqlOptions.Password,
		"--port":                      port,
		"--status-addr":               "",
		"--transactional-consistency": boolToStr(d.settings.DumplingOptions.TransactionalConsistency), // 这个默认是true, 并且参数被隐藏
		"--complete-insert":           "true",                                                         // always keep column name in `INSERT INTO` statements.
		"--no-views":                  boolToStr(d.settings.DumplingOptions.NoViews),                  // Do not dump views
		"--consistency":               d.settings.DumplingOptions.Consistency,
		"--threads":                   conv.Itoa(d.settings.DumplingOptions.Threads),
		"--escape-backslash":          boolToStr(d.settings.DumplingOptions.EscapeBackslash),
		"--where":                     d.settings.DumplingOptions.Where,
		"--snapshot":                  d.settings.DumplingOptions.SnapshotPosition,
		"--params":                    "time_zone=task.TimeZone",
		"--filesize":                  conv.I64toa(int64(d.settings.DumplingOptions.ChunkSize)),
		"--statement-size":            conv.I64toa(d.settings.DumplingOptions.StatementSize),
		"--rows":                      conv.I64toa(d.settings.DumplingOptions.MaxRows),
		"--filter":                    "",
		"--filetype":                  "sql",
	}

	// record exit position when consistency is none, to support scenarios like Aurora upstream
	//if d.task.Dumpling.Consistency == "none" {
	//	dumpConfig["PosAfterConnect"] = 'true'
	//}

	var args []string
	for k, v := range dumpConfig {
		if v != "" {
			args = append(args, k, v)
		}
	}

	command := cmd.NewCommand(
		filepath.Join(io_utils.GetCurrentDir(), "third-party", "dumpling", core.If(runtime.GOOS == "windows", "dumpling.exe", "dumpling")),
		args,
		cmd.WithCustomStdout(d.logger.With(zap.Bool("stdout", true)).ToWriter(zapcore.InfoLevel)),
		cmd.WithCustomStderr(d.logger.With(zap.Bool("stderr", true)).ToWriter(zapcore.ErrorLevel)),
		cmd.WithoutTimeout)

	err = command.ExecuteContext(ctx)
	if !command.Executed() {
		if err != nil {
			return err
		} else {
			return fmt.Errorf("cannot execute command \"%s\"", command.String())
		}
	}

	if command.ExitCode() != 0 && command.Stderr() != "" {
		return errors.New(command.Stderr())
	}

	return nil
}
