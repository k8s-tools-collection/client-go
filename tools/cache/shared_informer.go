/*
Copyright 2015 The Kubernetes Authors.

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

package cache

import (
	"fmt"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/clock"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/utils/buffer"

	"k8s.io/klog/v2"
)

// SharedInformer provides eventually consistent linkage of its
// clients to the authoritative state of a given collection of
// objects.  An object is identified by its API group, kind/resource,
// namespace (if any), and name; the `ObjectMeta.UID` is not part of
// an object's ID as far as this contract is concerned.  One
// SharedInformer provides linkage to objects of a particular API
// group and kind/resource.  The linked object collection of a
// SharedInformer may be further restricted to one namespace (if
// applicable) and/or by label selector and/or field selector.
//
// The authoritative state of an object is what apiservers provide
// access to, and an object goes through a strict sequence of states.
// An object state is either (1) present with a ResourceVersion and
// other appropriate content or (2) "absent".
//
// A SharedInformer maintains a local cache --- exposed by GetStore(),
// by GetIndexer() in the case of an indexed informer, and possibly by
// machinery involved in creating and/or accessing the informer --- of
// the state of each relevant object.  This cache is eventually
// consistent with the authoritative state.  This means that, unless
// prevented by persistent communication problems, if ever a
// particular object ID X is authoritatively associated with a state S
// then for every SharedInformer I whose collection includes (X, S)
// eventually either (1) I's cache associates X with S or a later
// state of X, (2) I is stopped, or (3) the authoritative state
// service for X terminates.  To be formally complete, we say that the
// absent state meets any restriction by label selector or field
// selector.
//
// For a given informer and relevant object ID X, the sequence of
// states that appears in the informer's cache is a subsequence of the
// states authoritatively associated with X.  That is, some states
// might never appear in the cache but ordering among the appearing
// states is correct.  Note, however, that there is no promise about
// ordering between states seen for different objects.
//
// The local cache starts out empty, and gets populated and updated
// during `Run()`.
//
// As a simple example, if a collection of objects is henceforth
// unchanging, a SharedInformer is created that links to that
// collection, and that SharedInformer is `Run()` then that
// SharedInformer's cache eventually holds an exact copy of that
// collection (unless it is stopped too soon, the authoritative state
// service ends, or communication problems between the two
// persistently thwart achievement).
//
// As another simple example, if the local cache ever holds a
// non-absent state for some object ID and the object is eventually
// removed from the authoritative state then eventually the object is
// removed from the local cache (unless the SharedInformer is stopped
// too soon, the authoritative state service ends, or communication
// problems persistently thwart the desired result).
//
// The keys in the Store are of the form namespace/name for namespaced
// objects, and are simply the name for non-namespaced objects.
// Clients can use `MetaNamespaceKeyFunc(obj)` to extract the key for
// a given object, and `SplitMetaNamespaceKey(key)` to split a key
// into its constituent parts.
//
// Every query against the local cache is answered entirely from one
// snapshot of the cache's state.  Thus, the result of a `List` call
// will not contain two entries with the same namespace and name.
//
// A client is identified here by a ResourceEventHandler.  For every
// update to the SharedInformer's local cache and for every client
// added before `Run()`, eventually either the SharedInformer is
// stopped or the client is notified of the update.  A client added
// after `Run()` starts gets a startup batch of notifications of
// additions of the objects existing in the cache at the time that
// client was added; also, for every update to the SharedInformer's
// local cache after that client was added, eventually either the
// SharedInformer is stopped or that client is notified of that
// update.  Client notifications happen after the corresponding cache
// update and, in the case of a SharedIndexInformer, after the
// corresponding index updates.  It is possible that additional cache
// and index updates happen before such a prescribed notification.
// For a given SharedInformer and client, the notifications are
// delivered sequentially.  For a given SharedInformer, client, and
// object ID, the notifications are delivered in order.  Because
// `ObjectMeta.UID` has no role in identifying objects, it is possible
// that when (1) object O1 with ID (e.g. namespace and name) X and
// `ObjectMeta.UID` U1 in the SharedInformer's local cache is deleted
// and later (2) another object O2 with ID X and ObjectMeta.UID U2 is
// created the informer's clients are not notified of (1) and (2) but
// rather are notified only of an update from O1 to O2. Clients that
// need to detect such cases might do so by comparing the `ObjectMeta.UID`
// field of the old and the new object in the code that handles update
// notifications (i.e. `OnUpdate` method of ResourceEventHandler).
//
// A client must process each notification promptly; a SharedInformer
// is not engineered to deal well with a large backlog of
// notifications to deliver.  Lengthy processing should be passed off
// to something else, for example through a
// `client-go/util/workqueue`.
//
// A delete notification exposes the last locally known non-absent
// state, except that its ResourceVersion is replaced with a
// ResourceVersion in which the object is actually absent.
type SharedInformer interface {
	// AddEventHandler adds an event handler to the shared informer using the shared informer's resync
	// period.  Events to a single handler are delivered sequentially, but there is no coordination
	// between different handlers.
	// 添加事件资源处理器，ResourceEventHandler定义于controller.go中
	// 注册回调函数，当资源有变化通知调用方，可以支持多个handler
	AddEventHandler(handler ResourceEventHandler)
	// AddEventHandlerWithResyncPeriod adds an event handler to the
	// shared informer with the requested resync period; zero means
	// this handler does not care about resyncs.  The resync operation
	// consists of delivering to the handler an update notification
	// for every object in the informer's local cache; it does not add
	// any interactions with the authoritative storage.  Some
	// informers do no resyncs at all, not even for handlers added
	// with a non-zero resyncPeriod.  For an informer that does
	// resyncs, and for each handler that requests resyncs, that
	// informer develops a nominal resync period that is no shorter
	// than the requested period but may be longer.  The actual time
	// between any two resyncs may be longer than the nominal period
	// because the implementation takes time to do work and there may
	// be competing load and scheduling noise.
	// 添加需要周期同步的处理器
	AddEventHandlerWithResyncPeriod(handler ResourceEventHandler, resyncPeriod time.Duration)
	// GetStore returns the informer's local cache as a Store.
	// 获取store的接口
	GetStore() Store
	// GetController is deprecated, it does nothing useful
	// 获取controller
	GetController() Controller
	// Run starts and runs the shared informer, returning after it stops.
	// The informer will be stopped when stopCh is closed.
	// informer的核心逻辑
	Run(stopCh <-chan struct{})
	// HasSynced returns true if the shared informer's store has been
	// informed by at least one full LIST of the authoritative state
	// of the informer's object collection.  This is unrelated to "resync".
	// 告知使用者Store里面是否已经同步了apiserver的资源，这个接口很有用
    // 当创建完SharedInformer后，通过Reflector从apiserver同步全量对象，
    // 然后在通过DeltaFIFO一个一个的同志到cache,这个接口就是告知使用者，
    // 全量的对象是不是已经同步到了cache,这样就可以从cache列举或者查询了
    HasSynced() bool
	// LastSyncResourceVersion is the resource version observed when last synced with the underlying
	// store. The value returned is not synchronized with access to the underlying store and is not
	// thread-safe.
	// 最新同步资源的版本，
	LastSyncResourceVersion() string

	// The WatchErrorHandler is called whenever ListAndWatch drops the
	// connection with an error. After calling this handler, the informer
	// will backoff and retry.
	//
	// The default implementation looks at the error type and tries to log
	// the error message at an appropriate level.
	//
	// There's only one handler, so if you call this multiple times, last one
	// wins; calling after the informer has been started returns an error.
	//
	// The handler is intended for visibility, not to e.g. pause the consumers.
	// The handler should return quickly - any expensive processing should be
	// offloaded.
	// 处理ListAndWatch丢弃连接并出现错误。调用此处理程序后，informer将退出并重试。
	SetWatchErrorHandler(handler WatchErrorHandler) error
}

// SharedIndexInformer provides add and get Indexers ability based on SharedInformer.
//  扩展了SharedInformer类型，共享的是Indexer,Indexer也是一种Store的实现
type SharedIndexInformer interface {
	// 继承sharedinformer
	SharedInformer
	// AddIndexers add indexers to the informer before it starts.
	// 添加扩展indexer相关的接口
	AddIndexers(indexers Indexers) error
	GetIndexer() Indexer
}

// NewSharedInformer creates a new instance for the listwatcher.
// lw:这个是apiserver客户端相关的，用于Reflector从apiserver获取资源，所以需要外部提供
// objType:这个SharedInformer监控的对象类型
// resyncPeriod:同步周期，SharedInformer需要多长时间给使用者发送一次全量对象的同步时间
func NewSharedInformer(lw ListerWatcher, exampleObject runtime.Object, defaultEventHandlerResyncPeriod time.Duration) SharedInformer {
	return NewSharedIndexInformer(lw, exampleObject, defaultEventHandlerResyncPeriod, Indexers{})
}

// NewSharedIndexInformer creates a new instance for the listwatcher.
// The created informer will not do resyncs if the given
// defaultEventHandlerResyncPeriod is zero.  Otherwise: for each
// handler that with a non-zero requested resync period, whether added
// before or after the informer starts, the nominal resync period is
// the requested resync period rounded up to a multiple of the
// informer's resync checking period.  Such an informer's resync
// checking period is established when the informer starts running,
// and is the maximum of (a) the minimum of the resync periods
// requested before the informer starts and the
// defaultEventHandlerResyncPeriod given here and (b) the constant
// `minimumResyncPeriod` defined in this file.
// 创建SharedIndexInformer对象
// indexers:需要外部提供计算对象索引键的函数，也就是这里面的对象需要通过什么方式创建索引
func NewSharedIndexInformer(lw ListerWatcher, exampleObject runtime.Object, defaultEventHandlerResyncPeriod time.Duration, indexers Indexers) SharedIndexInformer {
	realClock := &clock.RealClock{}
	sharedIndexInformer := &sharedIndexInformer{
		// 管理所有的监听器
		processor:                       &sharedProcessor{clock: realClock},
		// 其实就是在构造cache，读者可以自行查看NewIndexer()的实现，
		// 在cache中的对象用DeletionHandlingMetaNamespaceKeyFunc计算对象键，用indexers计算索引键
		// 可以想象成每个对象键是Namespace/Name，每个索引键是Namespace，即按照Namesapce分类
		// 因为objType决定了只有一种类型对象，所以Namesapce是最大的分类
		indexer:                         NewIndexer(DeletionHandlingMetaNamespaceKeyFunc, indexers),
		// 给Controller用，确切的说是给Reflector用的
		listerWatcher:                   lw,
		objectType:                      exampleObject,
		// 无论是否需要定时同步，SharedInformer都提供了一个默认的同步时间，当然这个是外部设置的
		resyncCheckPeriod:               defaultEventHandlerResyncPeriod,
		defaultEventHandlerResyncPeriod: defaultEventHandlerResyncPeriod,
		// 默认没有开启的对象突变检测器，
		cacheMutationDetector:           NewCacheMutationDetector(fmt.Sprintf("%T", exampleObject)),
		clock:                           realClock,
	}
	return sharedIndexInformer
}

// InformerSynced is a function that can be used to determine if an informer has synced.  This is useful for determining if caches have synced.
// InformerSynced是可用于确定informer是否已同步的函数。对于确定缓存是否已同步非常有用
type InformerSynced func() bool

const (
	// syncedPollPeriod controls how often you look at the status of your sync funcs
	syncedPollPeriod = 100 * time.Millisecond

	// initialBufferSize is the initial number of event notifications that can be buffered.
	initialBufferSize = 1024
)

// WaitForNamedCacheSync is a wrapper around WaitForCacheSync that generates log messages
// indicating that the caller identified by name is waiting for syncs, followed by
// either a successful or failed sync.
func WaitForNamedCacheSync(controllerName string, stopCh <-chan struct{}, cacheSyncs ...InformerSynced) bool {
	klog.Infof("Waiting for caches to sync for %s", controllerName)

	if !WaitForCacheSync(stopCh, cacheSyncs...) {
		utilruntime.HandleError(fmt.Errorf("unable to sync caches for %s", controllerName))
		return false
	}

	klog.Infof("Caches are synced for %s ", controllerName)
	return true
}

// WaitForCacheSync waits for caches to populate.  It returns true if it was successful, false
// if the controller should shutdown
// callers should prefer WaitForNamedCacheSync()
func WaitForCacheSync(stopCh <-chan struct{}, cacheSyncs ...InformerSynced) bool {
	err := wait.PollImmediateUntil(syncedPollPeriod,
		func() (bool, error) {
			for _, syncFunc := range cacheSyncs {
				if !syncFunc() {
					return false, nil
				}
			}
			return true, nil
		},
		stopCh)
	if err != nil {
		klog.V(2).Infof("stop requested")
		return false
	}

	klog.V(4).Infof("caches populated")
	return true
}

// `*sharedIndexInformer` implements SharedIndexInformer and has three
// main components.  One is an indexed local cache, `indexer Indexer`.
// The second main component is a Controller that pulls
// objects/notifications using the ListerWatcher and pushes them into
// a DeltaFIFO --- whose knownObjects is the informer's local cache
// --- while concurrently Popping Deltas values from that fifo and
// processing them with `sharedIndexInformer::HandleDeltas`.  Each
// invocation of HandleDeltas, which is done with the fifo's lock
// held, processes each Delta in turn.  For each Delta this both
// updates the local cache and stuffs the relevant notification into
// the sharedProcessor.  The third main component is that
// sharedProcessor, which is responsible for relaying those
// notifications to each of the informer's clients.
type sharedIndexInformer struct {
	// Indexer也是一种Store,Controller负责把Reflector和FIFO逻辑串联起来
	// 这两个变量就涵盖Reflector、DeltaFIFO和LocalStore(cache)
	indexer    Indexer
	controller Controller

	// sharedIndexInformer将ResourceEventHandler进行了封装，
	// 并统一由sharedProcessor管理
	processor             *sharedProcessor
	// CacheMutationDetector一个调试工具，用来发现对象突变的
	// 实现方法:DeltaFIFO弹出的对象在处理前先备份(深度拷贝)一份,
	// 然后定期比对两个对象是否相同,如果不同那就报警,处理过程中有人修改过对象，这个功能默认是关闭
	cacheMutationDetector MutationDetector

	// reflector 使用
	listerWatcher ListerWatcher

	// objectType is an example object of the type this informer is
	// expected to handle.  Only the type needs to be right, except
	// that when that is `unstructured.Unstructured` the object's
	// `"apiVersion"` and `"kind"` must also be right.
	objectType runtime.Object

	// resyncCheckPeriod is how often we want the reflector's resync timer to fire so it can call
	// shouldResync to check if any of our listeners need a resync.
	// 定期同步的周期，可能存在多个ResourceEventHandler，
	// 就有可能存在多个同步周期，sharedIndexInformer采用最小的周期
	// 周期值就存储在resyncCheckPeriod中，
	// 通过AddEventHandler()添加的处理器都采用defaultEventHandlerResyncPeriod
	resyncCheckPeriod time.Duration
	// defaultEventHandlerResyncPeriod is the default resync period for any handlers added via
	// AddEventHandler (i.e. they don't specify one and just want to use the shared informer's default
	// value).
	defaultEventHandlerResyncPeriod time.Duration
	// clock allows for testability
	// 时钟
	clock clock.Clock

	// 启动停止标记，三个状态，启动前，已启动和已停止
	started, stopped bool
	startedLock      sync.Mutex

	// blockDeltas gives a way to stop all event distribution so that a late event handler
	// can safely join the shared informer.
	// DeltaFIFO每次Pop()的时候需要传入一个函数用来处理Deltas
	// 处理Deltas也就意味着要把消息通知给处理器，如果此时调用了AddEventHandler()
	// 就会存在崩溃的问题，所以要有这个锁，阻塞Deltas
	blockDeltas sync.Mutex

	// Called whenever the ListAndWatch drops the connection with an error.
	watchErrorHandler WatchErrorHandler
}

// dummyController hides the fact that a SharedInformer is different from a dedicated one
// where a caller can `Run`.  The run method is disconnected in this case, because higher
// level logic will decide when to start the SharedInformer and related controller.
// Because returning information back is always asynchronous, the legacy callers shouldn't
// notice any change in behavior.
type dummyController struct {
	informer *sharedIndexInformer
}

func (v *dummyController) Run(stopCh <-chan struct{}) {
}

func (v *dummyController) HasSynced() bool {
	return v.informer.HasSynced()
}

func (v *dummyController) LastSyncResourceVersion() string {
	return ""
}

type updateNotification struct {
	oldObj interface{}
	newObj interface{}
}

type addNotification struct {
	newObj interface{}
}

type deleteNotification struct {
	oldObj interface{}
}

func (s *sharedIndexInformer) SetWatchErrorHandler(handler WatchErrorHandler) error {
	s.startedLock.Lock()
	defer s.startedLock.Unlock()

	if s.started {
		return fmt.Errorf("informer has already started")
	}

	s.watchErrorHandler = handler
	return nil
}

// sharedIndexInformer核心逻辑
// 启动了Controller和sharedProcess()，
// Controller通过DeltaFIFO.Pop()函数弹出Deltas，并调用函数sharedIndexInformer.HandleDeltas()处理
func (s *sharedIndexInformer) Run(stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()

	//    // 在此处构造的DeltaFIFO
	fifo := NewDeltaFIFOWithOptions(DeltaFIFOOptions{
		KnownObjects:          s.indexer,
		EmitDeltaTypeReplaced: true,
	})

	//Reflecto中的config
	cfg := &Config{
		// Reflector输入到DeltaFIFO
		Queue:            fifo,
		// Reflector
		ListerWatcher:    s.listerWatcher,
		ObjectType:       s.objectType,
		FullResyncPeriod: s.resyncCheckPeriod,
		RetryOnError:     false,
		ShouldResync:     s.processor.shouldResync,

		//Controller调用DeltaFIFO.Pop()接口传入的HandleDeltas这个回调函数
		Process:           s.HandleDeltas,
		WatchErrorHandler: s.watchErrorHandler,
	}

	// 创建controller
	func() {
		s.startedLock.Lock()
		defer s.startedLock.Unlock()

		s.controller = New(cfg)
		s.controller.(*controller).clock = s.clock
		s.started = true
	}()

	// Separate stop channel because Processor should be stopped strictly after controller
	// 这个processorStopCh 是给sharedProcessor和cacheMutationDetector传递退出信号的
	// 因为这里要创建两个协程运行sharedProcessor和cacheMutationDetector的核心函数
	processorStopCh := make(chan struct{})
	var wg wait.Group
	defer wg.Wait()              // Wait for Processor to stop
	defer close(processorStopCh) // Tell Processor to stop
	wg.StartWithChannel(processorStopCh, s.cacheMutationDetector.Run)
	wg.StartWithChannel(processorStopCh, s.processor.run)

	// run函数退出，设置结束标志
	defer func() {
		s.startedLock.Lock()
		defer s.startedLock.Unlock()
		s.stopped = true // Don't want any new listeners
	}()
	// 启动Controller，Controller一旦运行，整个流程就开始启动了,
	// 所以Contr是SharedInformer的发动机
	s.controller.Run(stopCh)
}

func (s *sharedIndexInformer) HasSynced() bool {
	s.startedLock.Lock()
	defer s.startedLock.Unlock()

	if s.controller == nil {
		return false
	}
	return s.controller.HasSynced()
}

func (s *sharedIndexInformer) LastSyncResourceVersion() string {
	s.startedLock.Lock()
	defer s.startedLock.Unlock()

	if s.controller == nil {
		return ""
	}
	return s.controller.LastSyncResourceVersion()
}

func (s *sharedIndexInformer) GetStore() Store {
	return s.indexer
}

func (s *sharedIndexInformer) GetIndexer() Indexer {
	return s.indexer
}

func (s *sharedIndexInformer) AddIndexers(indexers Indexers) error {
	s.startedLock.Lock()
	defer s.startedLock.Unlock()

	if s.started {
		return fmt.Errorf("informer has already started")
	}

	return s.indexer.AddIndexers(indexers)
}

func (s *sharedIndexInformer) GetController() Controller {
	return &dummyController{informer: s}
}

// 添加没有指定同步周期的事件处理器
func (s *sharedIndexInformer) AddEventHandler(handler ResourceEventHandler) {
	// defaultEventHandlerResyncPeriod是默认的同步周期，在创建SharedInformer的时候设置的
	s.AddEventHandlerWithResyncPeriod(handler, s.defaultEventHandlerResyncPeriod)
}

func determineResyncPeriod(desired, check time.Duration) time.Duration {
	if desired == 0 {
		return desired
	}
	if check == 0 {
		klog.Warningf("The specified resyncPeriod %v is invalid because this shared informer doesn't support resyncing", desired)
		return 0
	}
	if desired < check {
		klog.Warningf("The specified resyncPeriod %v is being increased to the minimum resyncCheckPeriod %v", desired, check)
		return check
	}
	return desired
}

const minimumResyncPeriod = 1 * time.Second

// 添加需要定期同步的事件处理器
func (s *sharedIndexInformer) AddEventHandlerWithResyncPeriod(handler ResourceEventHandler, resyncPeriod time.Duration) {
	// 是否已经开始对于添加事件处理器，将会影响处理方式
	s.startedLock.Lock()
	defer s.startedLock.Unlock()

	// 是否已经结束
	if s.stopped {
		klog.V(2).Infof("Handler %v was not added to shared informer because it has stopped already", handler)
		return
	}

	// ===0 永远不用同步
	if resyncPeriod > 0 {
		// 同步周期不能太短，太短对于系统来说是个负担，
		// 大量的无效计算浪费
		if resyncPeriod < minimumResyncPeriod {
			klog.Warningf("resyncPeriod %v is too small. Changing it to the minimum allowed value of %v", resyncPeriod, minimumResyncPeriod)
			resyncPeriod = minimumResyncPeriod
		}
		// SharedInformer管理了很多处理器，每个处理器都有自己的同步周期，所以此处要统一成一个，称之为对齐
		// SharedInformer会选择所有处理器中最小的那个作为所有处理器的同步周期，称为对齐后的同步周期
		// 此处就要判断是不是比当前对齐后的同步周期还要小
		if resyncPeriod < s.resyncCheckPeriod {
			//已经启动，使用相同的周期
			if s.started {
				klog.Warningf("resyncPeriod %v is smaller than resyncCheckPeriod %v and the informer has already started. Changing it to %v", resyncPeriod, s.resyncCheckPeriod, s.resyncCheckPeriod)
				resyncPeriod = s.resyncCheckPeriod
			} else {
				// 如果没启动，将之前的的周期更新为新的周期
				// if the event handler's resyncPeriod is smaller than the current resyncCheckPeriod, update
				// resyncCheckPeriod to match resyncPeriod and adjust the resync periods of all the listeners
				// accordingly
				s.resyncCheckPeriod = resyncPeriod
				s.processor.resyncCheckPeriodChanged(resyncPeriod)
			}
		}
	}

	// 创建监听器
	listener := newProcessListener(handler, resyncPeriod, determineResyncPeriod(resyncPeriod, s.resyncCheckPeriod), s.clock.Now(), initialBufferSize)

	// 没有启动直接添加监听器
	if !s.started {
		s.processor.addListener(listener)
		return
	}

	// in order to safely join, we have to
	// 1. stop sending add/update/delete notifications
	// 2. do a list against the store
	// 3. send synthetic "Add" events to the new handler
	// 4. unblock
	// 暂停所有的监听器
	s.blockDeltas.Lock()
	defer s.blockDeltas.Unlock()

	// 添加处理器
	s.processor.addListener(listener)
	// 遍历缓冲中的所有对象，通知处理器，因为SharedInformer已经启动了，可能很多对象已经让其他的处理器处理过了，
	// 处理对象不会再通知新添加的处理器问题，
	for _, item := range s.indexer.List() {
		listener.add(addNotification{newObj: item})
	}
}

// 衔接Controller和sharedProcess,
// 把Deltas转换为sharedProcess需要的各种Notification类型
func (s *sharedIndexInformer) HandleDeltas(obj interface{}) error {
	// 阻塞处理器Deltas，事件分发到处理器
	s.blockDeltas.Lock()
	defer s.blockDeltas.Unlock()

	// from oldest to newest
	// Deltas里面包含了一个对象的多个增量操作，
	// 要从最老的Delta到最先的Delta遍历处理
	for _, d := range obj.(Deltas) {
		// 根据不同的Delta做不同的操作，大致分为对象添加、删除两大类操作
		// 所有的操作都要先同步到cache在通知处理器，
		// 这样保持处理器和cache的状态是一致的
		switch d.Type {
		// 同步、添加、更新都是对象添加类，至于是否是更新还要看cache是否有这个对象
		case Sync, Replaced, Added, Updated:
			s.cacheMutationDetector.AddObject(d.Object)
			if old, exists, err := s.indexer.Get(d.Object); err == nil && exists {
				if err := s.indexer.Update(d.Object); err != nil {
					return err
				}

				// 是否有同步产生事件
				isSync := false
				switch {
				case d.Type == Sync:
					// Sync events are only propagated to listeners that requested resync
					isSync = true
				case d.Type == Replaced:
					if accessor, err := meta.Accessor(d.Object); err == nil {
						if oldAccessor, err := meta.Accessor(old); err == nil {
							// Replaced events that didn't change resourceVersion are treated as resync events
							// and only propagated to listeners that requested resync
							isSync = accessor.GetResourceVersion() == oldAccessor.GetResourceVersion()
						}
					}
				}
				// 事件分发，通知处理器处理事件
				s.processor.distribute(updateNotification{oldObj: old, newObj: d.Object}, isSync)
			} else {
				// 将对象添加到cache
				if err := s.indexer.Add(d.Object); err != nil {
					return err
				}
				s.processor.distribute(addNotification{newObj: d.Object}, false)
			}
			//对象被删除
		case Deleted:
			// 从cache中删除对象
			if err := s.indexer.Delete(d.Object); err != nil {
				return err
			}
			// 事件分发通知所有的监听器对象被删除
			s.processor.distribute(deleteNotification{oldObj: d.Object}, false)
		}
	}
	return nil
}

// sharedProcessor has a collection of processorListener and can
// distribute a notification object to its listeners.  There are two
// kinds of distribute operations.  The sync distributions go to a
// subset of the listeners that (a) is recomputed in the occasional
// calls to shouldResync and (b) every listener is initially put in.
// The non-sync distributions go to every listener.
type sharedProcessor struct {
	// 所有处理器是否已经启动的标识
	listenersStarted bool
	listenersLock    sync.RWMutex
	// 通用的处理器
	listeners        []*processorListener
	// 需要定时同步的处理器
	syncingListeners []*processorListener
	// 时钟
	clock            clock.Clock
	// processorListener每个需要两个协程,用wait.Group来管理所有处理器的协程，都能退出
	wg               wait.Group
}

// 添加处理器,sharedIndexInformer.AddEventHandler()就会调用这个函数实现处理器的添加
func (p *sharedProcessor) addListener(listener *processorListener) {
	// 加锁
	p.listenersLock.Lock()
	defer p.listenersLock.Unlock()

	// 将处理器加入数组
	p.addListenerLocked(listener)
	//  wait.Group启动两个协程
	//  processorListener.run,processorListener.pop
	//  p.listenersStarted,sharedProcessor在启动前、后都可以添加处理器
	if p.listenersStarted {
		p.wg.Start(listener.run)
		p.wg.Start(listener.pop)
	}
}

// 将处理器添加到数组中
func (p *sharedProcessor) addListenerLocked(listener *processorListener) {
	// 两类(定时同步和不同步)的处理器数组都添加，因为没有定时同步的也会用默认的时间
	p.listeners = append(p.listeners, listener)
	p.syncingListeners = append(p.syncingListeners, listener)
}

// 事件分发 sync指明obj对象是否为同步事件
func (p *sharedProcessor) distribute(obj interface{}, sync bool) {
	p.listenersLock.RLock()
	defer p.listenersLock.RUnlock()

	// 无论是否为sync，添加监听器的代码中两个数组都会被添加，代码的更新二者的界限已经没那么明显
	if sync {
		for _, listener := range p.syncingListeners {
			listener.add(obj)
		}
	} else {
		for _, listener := range p.listeners {
			listener.add(obj)
		}
	}
}

// 等待退出信号然后关闭所有的处理器
func (p *sharedProcessor) run(stopCh <-chan struct{}) {
	// 启动前、后对于添加处理器的逻辑是不同，启动前的处理器是不会立刻启动连个协程执行处理器的pop()和run()函数的
	// 而是在这里统一的启动
	func() {
		p.listenersLock.RLock()
		defer p.listenersLock.RUnlock()
		// 遍历所有的处理器，然后为处理器启动两个后台协程
		for _, listener := range p.listeners {
			p.wg.Start(listener.run)
			p.wg.Start(listener.pop)
		}
		p.listenersStarted = true
	}()
	// 等待退出信号
	<-stopCh
	p.listenersLock.RLock()
	defer p.listenersLock.RUnlock()
	// 关闭addCh，processorListener.pop()这个协程就会退出，
	// 因为processorListener.pop()会关闭processorListener.nextCh，processorListener.run()就会退出
	// 所以这里只要关闭processorListener.addCh就可以自动实现两个协程的退出，
	for _, listener := range p.listeners {
		close(listener.addCh) // Tell .pop() to stop. .pop() will tell .run() to stop
	}
	//等待所有的协程退出
	p.wg.Wait() // Wait for all .pop() and .run() to stop
}

// shouldResync queries every listener to determine if any of them need a resync, based on each
// listener's resyncPeriod.
func (p *sharedProcessor) shouldResync() bool {
	p.listenersLock.Lock()
	defer p.listenersLock.Unlock()

	p.syncingListeners = []*processorListener{}

	resyncNeeded := false
	now := p.clock.Now()
	for _, listener := range p.listeners {
		// need to loop through all the listeners to see if they need to resync so we can prepare any
		// listeners that are going to be resyncing.
		if listener.shouldResync(now) {
			resyncNeeded = true
			p.syncingListeners = append(p.syncingListeners, listener)
			listener.determineNextResync(now)
		}
	}
	return resyncNeeded
}

func (p *sharedProcessor) resyncCheckPeriodChanged(resyncCheckPeriod time.Duration) {
	p.listenersLock.RLock()
	defer p.listenersLock.RUnlock()

	for _, listener := range p.listeners {
		resyncPeriod := determineResyncPeriod(listener.requestedResyncPeriod, resyncCheckPeriod)
		listener.setResyncPeriod(resyncPeriod)
	}
}

// processorListener relays notifications from a sharedProcessor to
// one ResourceEventHandler --- using two goroutines, two unbuffered
// channels, and an unbounded ring buffer.  The `add(notification)`
// function sends the given notification to `addCh`.  One goroutine
// runs `pop()`, which pumps notifications from `addCh` to `nextCh`
// using storage in the ring buffer while `nextCh` is not keeping up.
// Another goroutine runs `run()`, which receives notifications from
// `nextCh` and synchronously invokes the appropriate handler method.
//
// processorListener also keeps track of the adjusted requested resync
// period of the listener.
// 实现了事件的缓冲和处理，处理是使用者传入的函数实现。
// 在没有事件的时候可以阻塞处理器，当事件较多是可以把事件缓冲起来，
// 实现了事件分发器与处理器的异步处理,run()和pop()函数是sharedProcessor启动的协程调用
type processorListener struct {
	// 这四个变量实现了事件的输入、缓冲、处理，事件就是apiserver资源的变化
	nextCh chan interface{}
	addCh  chan interface{} //无缓冲chan

	handler ResourceEventHandler

	// pendingNotifications is an unbounded ring buffer that holds all notifications not yet distributed.
	// There is one per listener, but a failing/stalled listener will have infinite pendingNotifications
	// added until we OOM.
	// TODO: This is no worse than before, since reflectors were backed by unbounded DeltaFIFOs, but
	// we should try to do something better.
	pendingNotifications buffer.RingGrowing

	// requestedResyncPeriod is how frequently the listener wants a
	// full resync from the shared informer, but modified by two
	// adjustments.  One is imposing a lower bound,
	// `minimumResyncPeriod`.  The other is another lower bound, the
	// sharedProcessor's `resyncCheckPeriod`, that is imposed (a) only
	// in AddEventHandlerWithResyncPeriod invocations made after the
	// sharedProcessor starts and (b) only if the informer does
	// resyncs at all.
	// 下面四个变量就是跟定时同步相关，
	//  requestedResyncPeriod是处理器设定的定时同步周期
	// resyncPeriod是跟sharedIndexInformer对齐的同步时间，
	// 因为sharedIndexInformer管理了多个处理器
	// 最终所有的处理器都会对齐到一个周期上，nextResync就是下一次同步的时间点
	requestedResyncPeriod time.Duration
	// resyncPeriod is the threshold that will be used in the logic
	// for this listener.  This value differs from
	// requestedResyncPeriod only when the sharedIndexInformer does
	// not do resyncs, in which case the value here is zero.  The
	// actual time between resyncs depends on when the
	// sharedProcessor's `shouldResync` function is invoked and when
	// the sharedIndexInformer processes `Sync` type Delta objects.
	resyncPeriod time.Duration
	// nextResync is the earliest time the listener should get a full resync
	nextResync time.Time
	// resyncLock guards access to resyncPeriod and nextResync
	resyncLock sync.Mutex
}

func newProcessListener(handler ResourceEventHandler, requestedResyncPeriod, resyncPeriod time.Duration, now time.Time, bufferSize int) *processorListener {
	ret := &processorListener{
		nextCh:                make(chan interface{}),
		addCh:                 make(chan interface{}),
		handler:               handler,
		pendingNotifications:  *buffer.NewRingGrowing(bufferSize),
		requestedResyncPeriod: requestedResyncPeriod,
		resyncPeriod:          resyncPeriod,
	}

	ret.determineNextResync(now)

	return ret
}

//通过addCh传入notification事件
func (p *processorListener) add(notification interface{}) {
	p.addCh <- notification
}

// 通过sharedProcessor利用wait.Group启动,处理接收、缓冲、发送
func (p *processorListener) pop() {
	defer utilruntime.HandleCrash()
	defer close(p.nextCh) // Tell .run() to stop

	var nextCh chan<- interface{}
	var notification interface{}
	// 死循环
	for {
		select {
	    	// 有两种情况，nextCh还没有初始化，这个语句就会被阻塞
			//nextChan后面会赋值为p.nextCh，因为p.nextCh也是无缓冲的chan,数据不发送成功就阻塞
		case nextCh <- notification:
			// Notification dispatched
			// 如果发送成功了，那就从缓冲中再取一个事件出来
			var ok bool
			notification, ok = p.pendingNotifications.ReadOne()
			if !ok { // Nothing to pop
				// 如果没有事件，那就把nextCh再次设置为nil，接下来对于nextCh操作还会被阻塞
				nextCh = nil // Disable this select case
			}
			// 消费p.addCh,从p.addCh读取一个事件出来
		case notificationToAdd, ok := <-p.addCh:
			// 说明p.addCh关闭了，只能退出
			if !ok {
				return
			}
			// notification为空说明当前还没发送任何事件给处理器
			if notification == nil { // No notification to pop (and pendingNotifications is empty)
				// Optimize the case - skip adding to pendingNotifications
				// 把刚刚获取的事件通过p.nextCh发送个处理器
				notification = notificationToAdd
				nextCh = p.nextCh
			} else { // There is already a notification waiting to be dispatched
				// 上一个事件还没有发送成功，先放到缓存中
				// pendingNotifications可以想象为一个slice
				// 是一个动态的缓存，
				p.pendingNotifications.WriteOne(notificationToAdd)
			}
		}
	}
}

//通过sharedProcessor利用wait.Group启动
func (p *processorListener) run() {
	// this call blocks until the channel is closed.  When a panic happens during the notification
	// we will catch it, **the offending item will be skipped!**, and after a short delay (one second)
	// the next notification will be attempted.  This is usually better than the alternative of never
	// delivering again.
	// wait.Until需要传入退出信号的chan
	stopCh := make(chan struct{})
	//没有收到退出信号就会周期的执行传入的函数
	wait.Until(func() {
		// wait.ExponentialBackoff()和wait.Until()类似，wait.Until()是无限循环
		// wait.ExponentialBackoff()是尝试几次，每次等待时间会以指数上涨
		for next := range p.nextCh {
			// 判断事件类型，这里面的handler就是调用SharedInfomer.AddEventHandler()传入的Deltas，
			// 由SharedInformer做的二次封装变成了其他类型，
			switch notification := next.(type) {
			case updateNotification:
				p.handler.OnUpdate(notification.oldObj, notification.newObj)
			case addNotification:
				p.handler.OnAdd(notification.newObj)
			case deleteNotification:
				p.handler.OnDelete(notification.oldObj)
			default:
				utilruntime.HandleError(fmt.Errorf("unrecognized notification: %T", next))
			}
		}
		// the only way to get here is if the p.nextCh is empty and closed
		//nextCh已经被关闭，关闭stopCh,通知wait.Until()退出
		close(stopCh)
	}, 1*time.Second, stopCh)
}

// shouldResync deterimines if the listener needs a resync. If the listener's resyncPeriod is 0,
// this always returns false.
func (p *processorListener) shouldResync(now time.Time) bool {
	p.resyncLock.Lock()
	defer p.resyncLock.Unlock()

	if p.resyncPeriod == 0 {
		return false
	}

	return now.After(p.nextResync) || now.Equal(p.nextResync)
}

func (p *processorListener) determineNextResync(now time.Time) {
	p.resyncLock.Lock()
	defer p.resyncLock.Unlock()

	p.nextResync = now.Add(p.resyncPeriod)
}

func (p *processorListener) setResyncPeriod(resyncPeriod time.Duration) {
	p.resyncLock.Lock()
	defer p.resyncLock.Unlock()

	p.resyncPeriod = resyncPeriod
}
