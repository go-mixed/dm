package task

import (
	"context"
	"fmt"
	"github.com/fly-studio/dm/src/canal"
	"github.com/fly-studio/dm/src/common"
	"github.com/fly-studio/dm/src/component"
	"github.com/fly-studio/dm/src/consumer"
	"github.com/fly-studio/dm/src/settings"
	"github.com/fly-studio/igop/mod"
	"github.com/goplus/igop"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

type Task struct {
	*component.Components

	canal *canal.Canal

	binLog common.BinLogPosition

	trigger *common.Trigger
	// 由于使用的延时删除，所以需要记录下一个消费的ID
	// 不然幻读会导致随机ID重复消费
	nextConsumeEventID uint64

	igopCtx *mod.Context
}

func NewTask(components *component.Components) *Task {
	t := &Task{
		Components: components,
		binLog:     components.Settings.TaskOptions.BinLog,
		canal:      nil,
	}

	t.trigger = common.NewAtomicTrigger(components.Settings.TaskOptions.MaxBulkSize, components.Settings.TaskOptions.MaxWait, t.consumer)

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

// 消费events
func (t *Task) consumer(taskId uint64) {
	count := t.Storage.EventCount()

	if count <= 0 {
		return
	}

	t.Logger.Debug("[Task]need consume",
		zap.Uint64("latest_id", t.Storage.LatestID()),
		zap.Uint64("event remain count", count),
	)

	// 将同一个rule的events分配在一起
	var lastRule *settings.RuleOptions
	var keyEnd string
	var events []consumer.RowEvent

	t.Storage.EventForEach(common.BuildEventKey(t.nextConsumeEventID, "", "", ""), func(key string, event consumer.RowEvent) bool {
		t.Logger.Debug(fmt.Sprintf("------%d------%s----", taskId, key))
		rule := t.Settings.TaskOptions.MatchRule(event.Schema, event.Table)
		if rule == nil { // 无rule匹配项，继续循环
			keyEnd = key
			return true
		} else if lastRule != nil && rule != lastRule { // 和上一个匹配的rule不一样, 终止匹配
			return false
		}
		keyEnd = key
		lastRule = rule
		events = append(events, event)
		return true
	})

	c := len(events)
	if c > 0 {
		methodErr, panicErr := igopCall(t.igopCtx, lastRule.Call, []igop.Value{events, lastRule.Arguments})
		_methodErr, _ := methodErr.(error)
		err := multierr.Append(_methodErr, panicErr)
		if err != nil {
			t.Logger.Error("[Task]execute igop error",
				zap.String("method", lastRule.Call),
				zap.Error(err),
			)
		} else {
			t.nextConsumeEventID = events[c-1].ID + 1
			t.Storage.DeleteEventsTo(keyEnd) // 删除符合要求的keys
			t.Logger.Info("[Task]executed igop",
				zap.String("method", lastRule.Call),
				zap.Uint64("next-id", t.nextConsumeEventID),
				zap.Uint64("start-id", events[0].ID),
				zap.Uint64("end-id", events[c-1].ID),
				zap.Int("count", c),
			)
		}
	}

	// 触发消费之后的的数量
	t.trigger.OnCountChanged(t.Storage.EventCount())
}
