package common

import (
	"context"
	"sync/atomic"
	"time"
)

type Trigger struct {
	lastTrigger time.Time
	lastID      atomic.Uint64

	maxWait      time.Duration
	maxCount     int64
	currentCount atomic.Int64

	triggerCallback func(context.Context, uint64)

	queue chan struct{}
}

const triggerMaxQueueSize = 10

// NewSingleFlightTrigger 当达到以下任一条件将触发triggerCallback：达到数量阈值，或者超过等待时间
//
//	同一时刻，只会执行1个触发的任务，任务均运行在一个协程中
//	1. 当maxCount触发的任务时，时间触发条件会从该任务【启动时】开始重新计算；当maxWait触发的任务执行完毕时，不影响继续触发maxCount；
//	2. 当数量为0时，不会触发任务。
//	为什么不使用sync/singleflight的原因：singleflight正在运行任务时，会阻塞OnCountChanged的调用，而dm业务中OnCountChanged调用不允许阻塞（虽然可以通过go OnCountChanged来避免，但是在高峰期时会导致海量协程被创建）。并且dm业务是可以丢弃重复的触发，只需要遵循按时和按量一个条件即可
func NewSingleFlightTrigger(maxCount int64, maxWait time.Duration, triggerCallback func(context.Context, uint64)) *Trigger {
	return &Trigger{
		lastTrigger:  time.Time{},
		lastID:       atomic.Uint64{},
		maxWait:      maxWait,
		maxCount:     maxCount,
		currentCount: atomic.Int64{},

		triggerCallback: triggerCallback,

		queue: make(chan struct{}, triggerMaxQueueSize),
	}
}

// Run 运行触发器
func (t *Trigger) Run(ctx context.Context) {

	// tick 定时触发器
	go func() {
		tick := time.NewTicker(t.maxWait)
		for {
			select {
			case <-ctx.Done():
				tick.Stop()
				return
			case <-tick.C:
				t.addQueue()
			}
		}
	}()

	// 和上面分开的原因：t.triggerCallback是阻塞运行的，所以分开后，上面的计时器不会阻塞，更准确
	for {
		select {
		case <-ctx.Done(): // 放前面，先触发
			return
		case <-t.queue:
			count := t.currentCount.Load()
			if count > 0 && (count >= t.maxCount || time.Since(t.lastTrigger) >= t.maxWait) {
				t.lastTrigger = time.Now()
				t.triggerCallback(ctx, t.lastID.Add(1))
			}
		}
	}
}

func (t *Trigger) addQueue() {
	select {
	case t.queue <- struct{}{}: // 能够插入
	default: // 否则跳过

	}
}

// OnCountChanged count的任何一次修改，都需要调用本函数
func (t *Trigger) OnCountChanged(count int64) {
	old := t.currentCount.Swap(count)
	if count != old && count >= t.maxCount {
		t.addQueue()
	}
}
