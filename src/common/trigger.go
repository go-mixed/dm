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
	maxCount     uint64
	currentCount atomic.Uint64

	triggerCallback func(uint64)

	queueCount atomic.Int32
	queue      chan struct{}
}

const triggerMaxQueueSize = 10

// NewAtomicTrigger 当达到以下任一条件将触发triggerCallback：达到数量阈值，或者超过等待时间
//
//	同一时刻，只会执行1个触发的任务，任务均运行在一个协程中
//	1. 当maxCount触发的任务时，时间触发条件会从该任务【启动时】开始重新计算；
//	2. 当maxWait触发的任务执行完毕时，不影响继续触发maxCount；
//	3. 当数量为0时，不会触发任务。
func NewAtomicTrigger(maxCount uint64, maxWait time.Duration, triggerCallback func(uint64)) *Trigger {
	return &Trigger{
		lastTrigger:  time.Time{},
		lastID:       atomic.Uint64{},
		maxWait:      maxWait,
		maxCount:     maxCount,
		currentCount: atomic.Uint64{},

		triggerCallback: triggerCallback,

		queueCount: atomic.Int32{},
		queue:      make(chan struct{}, triggerMaxQueueSize),
	}
}

// Run 运行触发器
func (t *Trigger) Run(ctx context.Context) {

	// tick 定时触发器
	go func() {
		tick := time.NewTicker(t.maxWait)
		for {
			select {
			case <-tick.C:
				t.addQueue()
			case <-ctx.Done():
				tick.Stop()
				return
			}
		}
	}()

	for {
		select {
		case <-t.queue:
			count := t.currentCount.Load()
			if count > 0 && (count >= t.maxCount || time.Since(t.lastTrigger) >= t.maxWait) {
				t.lastTrigger = time.Now()
				t.triggerCallback(t.lastID.Add(1))
			}
			t.queueCount.Add(-1) // 减去消耗的任务
		case <-ctx.Done():
			return
		}

	}
}

func (t *Trigger) addQueue() {
	if t.queueCount.Load() < triggerMaxQueueSize { // 有剩余的ch
		t.queue <- struct{}{}
		t.queueCount.Add(1) // 增加任务
	}
}

// OnCountChanged count的任何一次修改，都需要调用本函数
func (t *Trigger) OnCountChanged(count uint64) {
	old := t.currentCount.Swap(count)
	if count != old && count >= t.maxCount {
		t.addQueue()
	}
}
