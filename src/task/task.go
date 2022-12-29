package task

import (
	"context"
	"fmt"
	"github.com/fly-studio/dm/src/canal"
	"github.com/fly-studio/dm/src/common"
	"github.com/fly-studio/dm/src/component"
	"github.com/fly-studio/igop/mod"
	"go.uber.org/zap"
)

type Task struct {
	*component.Components

	canal *canal.Canal

	binLog common.BinLogPosition

	trigger *common.Trigger

	igopCtx *mod.Context
}

func NewTask(components *component.Components) *Task {
	t := &Task{
		Components: components,
		binLog:     components.Settings.TaskOptions.BinLog,
		canal:      nil,
	}

	t.trigger = common.NewAtomicTrigger(components.Settings.TaskOptions.MaxBulkSize, components.Settings.TaskOptions.MaxWait, t.triggerJob)

	return t
}

func (t *Task) Initial() error {
	c, err := canal.NewCanal(t.Components, t)
	if err != nil {
		return err
	}
	t.canal = c

	t.igopCtx, err = buildIgop(t.Settings.TaskOptions.ScriptDir, t.Settings.TaskOptions.ScriptVerbose)

	// 启动时 需要触发
	t.trigger.OnCountChanged(t.Storage.EventCount())

	return err
}

func (t *Task) String() string {
	return fmt.Sprintf("canal task of \"%s\"", t.Settings.MySqlOptions.Host)
}

func (t *Task) Run(ctx context.Context) {
	go t.runCanal()
	go t.trigger.Run(ctx)

	<-ctx.Done()
	t.canal.Stop()
}

func (t *Task) runCanal() {
	if err := t.canal.Start(t.Storage.GetLatestBinLogPosition(t.binLog)); err != nil {
		t.Logger.Error("[Task]canal work error", zap.Error(err))
	}
}

func (t *Task) triggerJob() {
	count := t.Storage.EventCount()

	if count <= 0 {
		return
	}

	t.Logger.Debug("[Task]trigger job",
		zap.Uint64("latest_id", t.Storage.LatestID()),
		zap.Uint64("event count", count),
	)

	t.trigger.OnCountChanged(0)

}
