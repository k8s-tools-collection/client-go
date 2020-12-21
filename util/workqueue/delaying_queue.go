/*
Copyright 2016 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package workqueue

import (
	"container/heap"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/util/clock"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
)

// DelayingInterface is an Interface that can Add an item at a later time. This makes it easier to
// requeue items after failures without ending up in a hot-loop.
type DelayingInterface interface {
	Interface // 继承了通用队列所有接口
	// AddAfter adds an item to the workqueue after the indicated duration has passed
	AddAfter(item interface{}, duration time.Duration) // 增加了延迟添加的接口
}

// NewDelayingQueue constructs a new workqueue with delayed queuing ability
func NewDelayingQueue() DelayingInterface {
	return NewDelayingQueueWithCustomClock(clock.RealClock{}, "")
}

// NewDelayingQueueWithCustomQueue constructs a new workqueue with ability to
// inject custom queue Interface instead of the default one
func NewDelayingQueueWithCustomQueue(q Interface, name string) DelayingInterface {
	return newDelayingQueue(clock.RealClock{}, q, name)
}

// NewNamedDelayingQueue constructs a new named workqueue with delayed queuing ability
func NewNamedDelayingQueue(name string) DelayingInterface {
	return NewDelayingQueueWithCustomClock(clock.RealClock{}, name)
}

// NewDelayingQueueWithCustomClock constructs a new named workqueue
// with ability to inject real or fake clock for testing purposes
func NewDelayingQueueWithCustomClock(clock clock.Clock, name string) DelayingInterface {
	return newDelayingQueue(clock, NewNamed(name), name)
}

func newDelayingQueue(clock clock.Clock, q Interface, name string) *delayingType {
	ret := &delayingType{
		Interface:       q,
		clock:           clock,
		heartbeat:       clock.NewTicker(maxWait),
		stopCh:          make(chan struct{}),
		waitingForAddCh: make(chan *waitFor, 1000),
		metrics:         newRetryMetrics(name),
	}

	go ret.waitingLoop()
	return ret
}

// delayingType wraps an Interface and provides delayed re-enquing
type delayingType struct {
	Interface

	// clock tracks time for delayed firing
	clock clock.Clock // 时钟，用于获取时间

	// stopCh lets us signal a shutdown to the waiting loop
	stopCh chan struct{} // ，接收异步退出信号
	// stopOnce guarantees we only signal shutdown a single time
	stopOnce sync.Once // 确保只接收一次退出信号

	// heartbeat ensures we wait no more than maxWait before firing
	heartbeat clock.Ticker //心跳，定时器，在没有任何数据操作时可以定时的唤醒处理协程

	// waitingForAddCh is a buffered channel that feeds waitingForAdd
	waitingForAddCh chan *waitFor // 所有延迟添加的元素封装成waitFor放到chan中


	// metrics counts the number of retries
	metrics retryMetrics // 统计重试次数
}

// waitFor holds the data to add and the time it should be added
type waitFor struct {
	data    t // 元素数据，这个t就是在通用队列中定义的类型
	readyAt time.Time // 添加到队列的时间
	// index in the priority queue (heap)
	index int //索引，优先级队列
}

// waitForPriorityQueue implements a priority queue for waitFor items.
//
// waitForPriorityQueue implements heap.Interface. The item occurring next in
// time (i.e., the item with the smallest readyAt) is at the root (index 0).
// Peek returns this minimum item at index 0. Pop returns the minimum item after
// it has been removed from the queue and placed at index Len()-1 by
// container/heap. Push adds an item at index Len(), and container/heap
// percolates it into the correct location.
// 把需要延迟的元素形成了一个队列，队列按照元素的延时添加的时间(readyAt)从小到大排序
// 实现的策略就是实现了go/src/container/heap/heap.go中的Interface类型，
// waitForPriorityQueue这个数组是有序的，排序方式是按照时间从小到大
type waitForPriorityQueue []*waitFor

// 实现heap的接口，获取队列的长度
func (pq waitForPriorityQueue) Len() int {
	return len(pq)
}
// 判断i元素是否比j小
func (pq waitForPriorityQueue) Less(i, j int) bool {
	return pq[i].readyAt.Before(pq[j].readyAt)
}
// heap的接口实现，交换i，j元素的值
func (pq waitForPriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	// 记录索引
	pq[i].index = i
	pq[j].index = j
}

// Push adds an item to the queue. Push should not be called directly; instead,
// use `heap.Push`.
// 向队列中添加数据
func (pq *waitForPriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*waitFor)
	item.index = n //记录索引值
	*pq = append(*pq, item) // 放到了数组尾部
}

// Pop removes an item from the queue. Pop should not be called directly;
// instead, use `heap.Pop`.
// 从队列中弹出最后一个数据
func (pq *waitForPriorityQueue) Pop() interface{} {
	n := len(*pq)
	item := (*pq)[n-1]
	item.index = -1
	*pq = (*pq)[0:(n - 1)] // 缩小数组，去掉了最后一个元素
	return item
}

// Peek returns the item at the beginning of the queue, without removing the
// item or otherwise mutating the queue. It is safe to call directly.
// 返回第一个元素
func (pq waitForPriorityQueue) Peek() interface{} {
	return pq[0]
}

// ShutDown stops the queue. After the queue drains, the returned shutdown bool
// on Get() will be true. This method may be invoked more than once.
func (q *delayingType) ShutDown() {
	q.stopOnce.Do(func() {
		q.Interface.ShutDown()
		close(q.stopCh)
		q.heartbeat.Stop()
	})
}

// AddAfter adds the given item to the work queue after the given delay
func (q *delayingType) AddAfter(item interface{}, duration time.Duration) {
	// don't add if we're already shutting down
	// 队列是否关闭
	if q.ShuttingDown() {
		return
	}

	q.metrics.retry()

	// immediately add things with no delay
	// 不需要延迟，如通用队列一样直接执行
	if duration <= 0 {
		q.Add(item)
		return
	}

	// 把元素封装成waitFor传入chan，切记select没有default，所以可能会被阻塞
	// 这里面用到了stopChan，因为有阻塞的可能，所以用stopChan可以保证退出
	select {
	case <-q.stopCh:
		// unblock if ShutDown() is called
	case q.waitingForAddCh <- &waitFor{data: item, readyAt: q.clock.Now().Add(duration)}:
	}
}

// maxWait keeps a max bound on the wait time. It's just insurance against weird things happening.
// Checking the queue every 10 seconds isn't expensive and we know that we'll never end up with an
// expired item sitting for more than 10 seconds.
const maxWait = 10 * time.Second

// waitingLoop runs until the workqueue is shutdown and keeps a check on the list of items to be added.
// 队列的核心代码
func (q *delayingType) waitingLoop() {
	defer utilruntime.HandleCrash()

	// Make a placeholder channel to use when there are no items in our list
	// 当没有元素需要延时添加的时候，用never于实现长时间等待
	never := make(<-chan time.Time)

	// Make a timer that expires when the item at the head of the waiting queue is ready
	// 使计时器在等待队列开头的项目准备就绪时到期
	var nextReadyAtTimer clock.Timer

	// 构造我们上面的有序队列，并且初始化
	waitingForQueue := &waitForPriorityQueue{}
	heap.Init(waitingForQueue)

	// 避免对象重复添加的，如果重复添加就只更新时间
	waitingEntryByData := map[t]*waitFor{}

	// 开始无限循环
	for {
		// 队列是否关闭
		if q.Interface.ShuttingDown() {
			return
		}
        // 获取当前时间
		now := q.clock.Now()

		// Add ready entries
		// 有序队列中是否有元素
		for waitingForQueue.Len() > 0 {
			// 获取第一个元素
			entry := waitingForQueue.Peek().(*waitFor)
			// 是否已经过了元素指定添加的时间,没有跳出循环
			if entry.readyAt.After(now) {
				break
			}

			// 时间已经过了，从有序队列拿出来放入通用队列中，这里面需要注意：
			// 1.heap.Pop()弹出的是第一个元素，waitingForQueue.Pop()弹出的是最后一个元素
			// 2.从有序队列把元素弹出，同时要把元素从上面提到的map删除，不用再判断重复添加
			// 3.此处是唯一一个地方把元素从有序队列移到通用队列，后面主要是等待时间到过程
			entry = heap.Pop(waitingForQueue).(*waitFor)
			q.Add(entry.data)
			delete(waitingEntryByData, entry.data)
		}

		// Set up a wait for the first item's readyAt (if one exists)
		// 如果有序队列中没有元素，就永久等下去
		// 如果有序队列中有元素，那就用第一个元素指定的时间减去当前时间作为等待时间
		// 有序队列是用时间排序的，后面的元素需要等待的时间更长，所以先处理排序靠前面的元素
		nextReadyAt := never
		if waitingForQueue.Len() > 0 {
			if nextReadyAtTimer != nil {
				nextReadyAtTimer.Stop()
			}
			entry := waitingForQueue.Peek().(*waitFor)
			nextReadyAtTimer = q.clock.NewTimer(entry.readyAt.Sub(now))
			nextReadyAt = nextReadyAtTimer.C()
		}

		// 进入各种等待
		select {
		// 退出信号
		case <-q.stopCh:
			return

			// 定时器，没过一段时间没有任何数据，那就再执行一次大循环，从理论上讲这个没用，但是这个具备容错能力，避免BUG死等
		case <-q.heartbeat.C():
			// continue the loop, which will add ready items

			// 这个就是有序队列里面需要等待时间信号了，时间到就会有信号
		case <-nextReadyAt:
			// continue the loop, which will add ready items

			// 这里是从chan中获取元素的，AddAfter()放入chan中的元素
		case waitEntry := <-q.waitingForAddCh:
			// 如果时间已经过了就直接放入通用队列，没过就插入到有序队列
			if waitEntry.readyAt.After(q.clock.Now()) {
				insert(waitingForQueue, waitingEntryByData, waitEntry)
			} else {
				q.Add(waitEntry.data)
			}
            // 把chan中的元素一口气全部取干净，注意用了default意味着chan中没有数据就会立刻停止
			drained := false
			for !drained {
				select {
				case waitEntry := <-q.waitingForAddCh:
					if waitEntry.readyAt.After(q.clock.Now()) {
						insert(waitingForQueue, waitingEntryByData, waitEntry)
					} else {
						q.Add(waitEntry.data)
					}
				default:
					drained = true
				}
			}
		}
	}
}

// insert adds the entry to the priority queue, or updates the readyAt if it already exists in the queue
// 将数据插入有序队列
func insert(q *waitForPriorityQueue, knownEntries map[t]*waitFor, entry *waitFor) {
	// if the entry already exists, update the time only if it would cause the item to be queued sooner
	// 检测元素是否添加过
	existing, exists := knownEntries[entry.data]
	if exists {
		if existing.readyAt.After(entry.readyAt) {
			existing.readyAt = entry.readyAt
			heap.Fix(q, existing.index)
		}

		return
	}
	// 把元素放入有序队列中，并记录在map里面,这个map就是上面那个用于判断对象是否重复添加的map
	// 这里面调用的是heap.Push，不是waitForPriorityQueue.Push
	heap.Push(q, entry)
	knownEntries[entry.data] = entry
}
