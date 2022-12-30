package settings

import (
	"github.com/fly-studio/dm/src/common"
	"go-common/utils/io"
	"path/filepath"
	"regexp"
	"time"
)

type RuleOptions struct {
	Schema string `yaml:"schema" validate:"required"`
	Table  string `yaml:"table" validate:"required"`

	TableRegexp *regexp.Regexp `yaml:"-"`

	// execute the "call(events, args)" on the task.ScriptDir
	Call      string   `yaml:"call" validate:"required"`
	Arguments []string `yaml:"arguments" validate:""`
}

type TaskOptions struct {
	TaskMode common.TaskMode `yaml:"task_mode" validate:"required"`

	ScriptDir     string `yaml:"script_dir" validate:"required"`
	ScriptVerbose bool   `yaml:"script_verbose"`

	BinLog common.BinLogPosition `yaml:"binlog" validate:"required_if=TaskMode incremental"`

	Rules []*RuleOptions `yaml:"rules" validate:"required,gt=0"`

	MaxWait     time.Duration `yaml:"max_wait"`
	MaxBulkSize uint64        `yaml:"max_bulk_size"`
}

func defaultTaskOptions() TaskOptions {
	return TaskOptions{
		MaxWait:     100 * time.Millisecond,
		MaxBulkSize: 10_000,

		ScriptDir:     filepath.Join(io_utils.GetCurrentDir(), "scripts"),
		ScriptVerbose: false,
	}
}

func (r *RuleOptions) pattern() string {
	return "^" + r.Schema + "\\." + r.Table + "$"
}

func (r *RuleOptions) Match(table string) bool {
	return r.TableRegexp.MatchString(table)
}

func (o *TaskOptions) Initial() error {
	var err error
	for _, rule := range o.Rules {
		if rule.TableRegexp, err = regexp.Compile(rule.pattern()); err != nil {
			return err
		}
	}

	return nil
}

func (o *TaskOptions) GetTablePatterns() []string {
	var patterns []string
	for _, rule := range o.Rules {
		patterns = append(patterns, rule.pattern())
	}
	return patterns
}

func (o *TaskOptions) MatchRule(schema, table string) *RuleOptions {
	_t := common.BuildTableName(schema, table, nil)
	for _, rule := range o.Rules {
		if rule.Match(_t) {
			return rule
		}
	}

	return nil
}

func (o *TaskOptions) MatchRules(schema, table string) []*RuleOptions {
	_t := common.BuildTableName(schema, table, nil)
	var rules []*RuleOptions
	for _, rule := range o.Rules {
		if rule.Match(_t) {
			rules = append(rules, rule)
		}
	}

	return rules
}
