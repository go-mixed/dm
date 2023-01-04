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
	"gopkg.in/go-mixed/go-common.v1/storage.v1"
	"gopkg.in/go-mixed/igop.v1/mod"
	"sync"
	"time"
)

type Task struct {
	*component.Components

	canal *canal.Canal

	binLog common.BinLogPosition

	trigger *common.Trigger
	// 由于使用的延时删除，所以需要记录下一个消费的ID
	// 不然幻读会导致随机ID重复消费
	nextConsumeEventID uint64

	igopCtx        *mod.Context
	canalWaitGroup sync.WaitGroup
}

func NewTask(components *component.Components) *Task {
	t := &Task{
		Components:     components,
		binLog:         components.Settings.TaskOptions.BinLog,
		canal:          nil,
		canalWaitGroup: sync.WaitGroup{},
	}

	t.trigger = common.NewSingleFlightTrigger(components.Settings.TaskOptions.MaxBulkSize, components.Settings.TaskOptions.MaxWait, t.consumer)

	return t
}

func (t *Task) Initial() (err error) {
	t.canal = canal.NewCanal(t.Components, t)
	t.igopCtx, err = buildIgop(t.Settings.TaskOptions.ScriptDir, t.Settings.TaskOptions.ScriptVerbose)
	if err != nil {
		return
	}
	return
}

func (t *Task) String() string {
	return fmt.Sprintf("canal task of \"%s\"", t.Settings.MySqlOptions.Host)
}

func (t *Task) Run(ctx context.Context) {
	go t.runCanal(ctx)

	// 启动时 需要触发数量
	t.trigger.OnCountChanged(t.Storage.EventCount())

	// 触发器阻塞运行, t.consumer运行于当前Run的协程
	// 当ctx退出时，t.consumer在读取event阶段时，会强制退出
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
		if err := t.canal.Start(t.Storage.GetLatestBinLogPosition(t.binLog)); err != nil {
			select {
			case <-ctx.Done():
				t.Logger.Error("[Task]canal quit error", zap.Error(err))
				return
			default:
				t.Logger.Error("[Task]canal work error, restart after 5s", zap.Error(err))
				time.Sleep(5 * time.Second) // 5s后重启
			}
		} else {
			t.Logger.Info("[Task]canal normal stop")
			return // 正常退出
		}
	}
}

// 消费events
func (t *Task) consumer(ctx context.Context, taskId uint64) {

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

	_, err := t.Storage.EventForEach(common.BuildEventKey(t.nextConsumeEventID, "", "", ""), func(key string, event consumer.RowEvent) error {
		select {
		case <-ctx.Done():
			return storage.ErrForEachQuit
		default:

		}
		t.Logger.Debug("[Task]read event from storage", zap.Uint64("task-id", taskId), zap.String("key", key))
		rule := t.Settings.TaskOptions.MatchRule(event.Schema, event.Table)
		if rule == nil { // 无rule匹配项，继续循环
			keyEnd = key
			return nil
		} else if lastRule != nil && rule != lastRule { // 和上一个匹配的rule不一样, 终止匹配
			return storage.ErrForEachBreak
		}
		keyEnd = key
		lastRule = rule
		events = append(events, event)
		return nil
	})

	if err != nil {
		// 在读取event时强制退出消费，但是注意：如果运行到了igopCall，只能等待它运行完毕
		if errors.Is(err, storage.ErrForEachQuit) {
			t.Logger.Debug("[Task]force quit the consumer")
			return
		} else if errors.Is(err, storage.ErrForEachBreak) { // 自然跳出

		} else {
			t.Logger.Error("[Task]for each of event error", zap.Error(err))
			return
		}
	}

	c := len(events)
	if c > 0 {
		methodErr, panicErr := igopCall(t.igopCtx, lastRule.Call, []igop.Value{events, lastRule.Arguments})
		_methodErr, _ := methodErr.(error)
		err = multierr.Append(_methodErr, panicErr)
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
