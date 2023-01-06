package task

import (
	"context"
	"fmt"
	"github.com/goplus/igop"
	"github.com/pkg/errors"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"gopkg.in/go-mixed/dm-consumer.v1"
	"gopkg.in/go-mixed/dm.v1/src/canal"
	"gopkg.in/go-mixed/dm.v1/src/common"
	"gopkg.in/go-mixed/dm.v1/src/component"
	"gopkg.in/go-mixed/dm.v1/src/settings"
	"gopkg.in/go-mixed/go-common.v1/utils"
	"gopkg.in/go-mixed/igop.v1/mod"
	"sync"
	"time"
)

type Task struct {
	*component.Components

	canal *canal.Canal

	trigger *common.Trigger

	igopCtx        *mod.Context
	canalWaitGroup sync.WaitGroup
}

func NewTask(components *component.Components) *Task {
	t := &Task{
		Components:     components,
		canal:          nil,
		canalWaitGroup: sync.WaitGroup{},
	}

	t.trigger = common.NewSingleFlightTrigger(components.Settings.TaskOptions.MaxBulkSize, components.Settings.TaskOptions.MaxWait, t.consumer)

	return t
}

func (t *Task) Initial() (err error) {
	t.canal = canal.NewCanal(t.Components, t)
	t.igopCtx, err = buildIgop(t.Settings.TaskOptions.ScriptDir, t.Settings.TaskOptions.ScriptVerbose)
	return
}

func (t *Task) String() string {
	return fmt.Sprintf("canal task of \"%s\"", t.Settings.MySqlOptions.Host)
}

func (t *Task) Run(ctx context.Context) {
	go t.runCanal(ctx)

	// 启动时 需要触发数量
	t.trigger.OnCountChanged(t.Storage.Conf.EventCount())

	// 触发器阻塞运行, t.consumer运行于当前Run的协程
	// 当ctx退出时，t.consumer在读取event阶段时，也会强制退出（并且不消耗bolt的events），但如果在igop call阶段，无法强制退出
	t.trigger.Run(ctx)

	// 关闭canal
	t.canal.Stop()

	// 等待canal执行完毕
	t.canalWaitGroup.Wait()
}

func (t *Task) runCanal(ctx context.Context) {
	t.canalWaitGroup.Add(1)
	defer t.canalWaitGroup.Done()
	for {
		if err := t.canal.Start(t.Storage.GetLatestBinLogPosition(t.Settings.TaskOptions.BinLog)); err != nil {
			select {
			case <-ctx.Done():
				t.Logger.Error("[Task]canal quit error", zap.Error(err))
				return
			default:
				t.Logger.Error("[Task]canal work exception, restart after 5s", zap.Error(err))
				time.Sleep(5 * time.Second) // 5s后重启
			}
		} else {
			t.Logger.Info("[Task]canal normally stop")
			return // 正常退出
		}
	}
}

// 消费events
func (t *Task) consumer(ctx context.Context, taskId uint64) {
	count := t.Storage.Conf.EventCount()
	if count <= 0 {
		return
	}

	// 触发消费之后的的数量
	defer t.trigger.OnCountChanged(t.Storage.Conf.EventCount())

	t.Logger.Info("[Task]need to consume",
		zap.Int64("latest event id", t.Storage.Conf.LatestEventID()),
		zap.Int64("next consume event id", t.Storage.Conf.NextConsumeEventID()),
		zap.Int64("event remain count", count),
	)

	// 将同一个rule的events分配在一起
	var lastRule *settings.RuleOptions
	var events []consumer.RowEvent

	lastKey, lastID, lastConsumePos, err := t.Storage.EventForEach(common.BuildKeyPrefix(t.Storage.Conf.NextConsumeEventID()), func(key string, event consumer.RowEvent) error {
		select {
		case <-ctx.Done():
			return utils.ErrForEachQuit
		default:

		}

		t.Logger.Debug("[Task]read event from storage", zap.Uint64("task-id", taskId), zap.String("key", key))
		rule := t.Settings.TaskOptions.MatchRule(event.Schema, event.Table)
		if rule == nil { // 无rule匹配项，继续循环
			return nil
		} else if lastRule != nil && rule != lastRule { // 和上一个匹配的rule不一样, 终止匹配
			return utils.ErrForEachBreak
		}
		lastRule = rule
		events = append(events, event)
		return nil
	})

	if err != nil {
		// 在读取event时强制退出消费，但是注意：如果运行到了igopCall，只能等待它运行完毕
		if errors.Is(err, utils.ErrForEachQuit) {
			t.Logger.Debug("[Task]force quit the consumer")
			return
		} else if errors.Is(err, utils.ErrForEachBreak) { // 自然跳出

		} else {
			t.Logger.Error("[Task]for each of event error", zap.Error(err))
			return
		}
	}

	c := len(events)
	if c > 0 {
		now := time.Now()
		methodErr, panicErr := igopCall(t.igopCtx, lastRule.Call, []igop.Value{events, lastRule.Arguments})
		_methodErr, _ := methodErr.(error)
		err = multierr.Append(_methodErr, panicErr)
		if err != nil {
			t.Logger.Error("[Task]execute igop error",
				zap.String("method", lastRule.Call),
				zap.Error(err),
				zap.Duration("duration", time.Since(now)),
			)
			return
		}

		t.Logger.Info("[Task]executed igop",
			zap.String("method", lastRule.Call),
			zap.Int64("start id", events[0].ID),
			zap.Int64("end id", events[c-1].ID),
			zap.Int("count", c),
			zap.Duration("duration", time.Since(now)),
		)
	}

	t.Storage.UpdateConsumeBinLogPosition(lastConsumePos)
	if lastKey != "" {
		t.Storage.DeleteEventsUtil(lastKey) // 删除开头~lastKey（含）的keys
	}
	if lastID != 0 {
		t.Storage.Conf.UpdateNextConsumeEventID(lastID + 1)
	}
}
