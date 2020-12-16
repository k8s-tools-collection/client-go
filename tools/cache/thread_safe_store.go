/*
Copyright 2014 The Kubernetes Authors.

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

	"k8s.io/apimachinery/pkg/util/sets"
)

// ThreadSafeStore is an interface that allows concurrent indexed
// access to a storage backend.  It is like Indexer but does not
// (necessarily) know how to extract the Store key from a given
// object.
//
// TL;DR caveats: you must not modify anything returned by Get or List as it will break
// the indexing feature in addition to not being thread safe.
//
// The guarantees of thread safety provided by List/Get are only valid if the caller
// treats returned items as read-only. For example, a pointer inserted in the store
// through `Add` will be returned as is by `Get`. Multiple clients might invoke `Get`
// on the same key and modify the pointer in a non-thread-safe way. Also note that
// modifying objects stored by the indexers (if any) will *not* automatically lead
// to a re-index. So it's not a good idea to directly modify the objects returned by
// Get/List, in general.
// 线程安全的存储接口
type ThreadSafeStore interface {
	Add(key string, obj interface{})
	Update(key string, obj interface{})
	Delete(key string)
	Get(key string) (item interface{}, exists bool)
	List() []interface{}
	ListKeys() []string
	Replace(map[string]interface{}, string)
	Index(indexName string, obj interface{}) ([]interface{}, error)
	IndexKeys(indexName, indexKey string) ([]string, error)
	ListIndexFuncValues(name string) []string
	ByIndex(indexName, indexKey string) ([]interface{}, error)
	GetIndexers() Indexers

	// AddIndexers adds more indexers to this store.  If you call this after you already have data
	// in the store, the results are undefined.
	AddIndexers(newIndexers Indexers) error
	// Resync is a no-op and is deprecated
	Resync() error
}

// threadSafeMap implements ThreadSafeStore
// 实现ThreadSafeStore的接口
type threadSafeMap struct {
	lock  sync.RWMutex // 读写锁,读多写少
	items map[string]interface{} // 存储对象的map，对象键:对象

	// indexers maps a name to an IndexFunc
	// 用于按类计算索引键的函数map 分类名:索引键的计算函数
	indexers Indexers
	// indices maps a name to an Index
	// 快速索引表，通过索引可以快速找到对象键，然后再从items中取出对象
	// 索引键是用于对象快速查找的，经过索引建在map中排序查找会更快
	// 对象键是为对象在存储中的唯一命名的，对象是通过名字+对象的方式存储的
	indices Indices
}

/************** 存储相关的函数 ******************/
func (c *threadSafeMap) Add(key string, obj interface{}) {
	c.lock.Lock()
	defer c.lock.Unlock()
	// 把老的对象取出来
	oldObject := c.items[key]
	// 写入新的对象
	c.items[key] = obj
	// 由于对象的添加就要更新索引
	c.updateIndices(oldObject, obj, key)
}

// 更新对象
func (c *threadSafeMap) Update(key string, obj interface{}) {
	c.lock.Lock()
	defer c.lock.Unlock()
	oldObject := c.items[key]
	c.items[key] = obj
	c.updateIndices(oldObject, obj, key)
}

// 删除对象
func (c *threadSafeMap) Delete(key string) {
	// 写操作，加锁
	c.lock.Lock()
	defer c.lock.Unlock()
	// 是否存在对象键
	if obj, exists := c.items[key]; exists {
		// 删除对象的所有索引
		c.deleteFromIndices(obj, key)
		// 删除对象本身
		delete(c.items, key)
	}
}

// 获取对象，依据对象键获取
func (c *threadSafeMap) Get(key string) (item interface{}, exists bool) {
	// 读锁
	c.lock.RLock()
	defer c.lock.RUnlock()
	// 取出对象
	item, exists = c.items[key]
	return item, exists
}

// 列举对象
func (c *threadSafeMap) List() []interface{} {
	c.lock.RLock()
	defer c.lock.RUnlock()
	list := make([]interface{}, 0, len(c.items))
	// 遍历对象
	for _, item := range c.items {
		list = append(list, item)
	}
	return list
}

// ListKeys returns a list of all the keys of the objects currently
// in the threadSafeMap.
// 列举对象键
func (c *threadSafeMap) ListKeys() []string {
	c.lock.RLock()
	defer c.lock.RUnlock()
	list := make([]string, 0, len(c.items))
	// 获取对象键
	for key := range c.items {
		list = append(list, key)
	}
	return list
}

// 取代所有对象，重新构造了一遍threadSafeMap
func (c *threadSafeMap) Replace(items map[string]interface{}, resourceVersion string) {
	// 全局锁
	c.lock.Lock()
	defer c.lock.Unlock()
	// 覆盖对象
	c.items = items

	// rebuild any index
	// 重建索引
	c.indices = Indices{}
	for key, item := range c.items {
		c.updateIndices(nil, item, key)
	}
}
/************** end 存储相关 ******************/

/************** 索引相关的函数 ******************/
// Index returns a list of items that match the given object on the index function.
// Index is thread-safe so long as you treat all items as immutable.
// 通过指定的索引函数计算对象的索引键，然后把索引键的对象全部取出来
// 如取出一个Pod所在节点上的所有Pod
// 取出符合对象某些特征的所有对象，而这个特征就是我们指定的索引函数计算出来
func (c *threadSafeMap) Index(indexName string, obj interface{}) ([]interface{}, error) {
	// 只读，读锁
	c.lock.RLock()
	defer c.lock.RUnlock()

	// 取出indexName分类索引函数
	indexFunc := c.indexers[indexName]
	if indexFunc == nil {
		return nil, fmt.Errorf("Index with name %s does not exist", indexName)
	}

	// 计算对象的索引键
	indexedValues, err := indexFunc(obj)
	if err != nil {
		return nil, err
	}
	// 取出indexName这个分类所有索引
	index := c.indices[indexName]

	// 返回对象的对象键的集合
	var storeKeySet sets.String
	if len(indexedValues) == 1 {
		// In majority of cases, there is exactly one value matching.
		// Optimize the most common path - deduping is not needed here.
		storeKeySet = index[indexedValues[0]]
	} else {
		// Need to de-dupe the return list.
		// Since multiple keys are allowed, this can happen.
		// 遍历刚刚计算出来的所有索引键
		storeKeySet = sets.String{}
		for _, indexedValue := range indexedValues {
			// 把所有的对象键输出到对象键的集合中
			for key := range index[indexedValue] {
				storeKeySet.Insert(key)
			}
		}
	}

	// 通过对象键逐一取出对象
	list := make([]interface{}, 0, storeKeySet.Len())
	for storeKey := range storeKeySet {
		list = append(list, c.items[storeKey])
	}
	return list, nil
}

// ByIndex returns a list of the items whose indexed values in the given index include the given indexed value
// 通过指定的索引函数,索引键，把索引键的对象全部取出来
func (c *threadSafeMap) ByIndex(indexName, indexedValue string) ([]interface{}, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	// indexName的索引分类是否存在
	indexFunc := c.indexers[indexName]
	if indexFunc == nil {
		return nil, fmt.Errorf("Index with name %s does not exist", indexName)
	}

	// 取出索引分类的所有索引
	index := c.indices[indexName]

	// 取出索引键的所有对象键
	set := index[indexedValue]
	// 遍历对象键输出
	list := make([]interface{}, 0, set.Len())
	for key := range set {
		list = append(list, c.items[key])
	}

	return list, nil
}

// IndexKeys returns a list of the Store keys of the objects whose indexed values in the given index include the given indexed value.
// IndexKeys is thread-safe so long as you treat all items as immutable.
// 通过指定的索引函数,索引键，把索引键的对象键全部取出来
func (c *threadSafeMap) IndexKeys(indexName, indexedValue string) ([]string, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	// 判断indexName的索引分类是否存在
	indexFunc := c.indexers[indexName]
	if indexFunc == nil {
		return nil, fmt.Errorf("Index with name %s does not exist", indexName)
	}

	// 取出索引分类的所有索引
	index := c.indices[indexName]

	// 直接输出索引键内的所有对象键
	set := index[indexedValue]
	return set.List(), nil
}

// 获取索引分类内的所有索引键的
func (c *threadSafeMap) ListIndexFuncValues(indexName string) []string {
	c.lock.RLock()
	defer c.lock.RUnlock()

	// 获取分类下的所有索引
	index := c.indices[indexName]
	// 直接遍历后输出索引键
	names := make([]string, 0, len(index))
	for key := range index {
		names = append(names, key)
	}
	return names
}

// 获取 分类名:索引键的计算函数
func (c *threadSafeMap) GetIndexers() Indexers {
	return c.indexers
}

// 添加 分类:索引计算函数
func (c *threadSafeMap) AddIndexers(newIndexers Indexers) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if len(c.items) > 0 {
		return fmt.Errorf("cannot add indexers to running index")
	}

	oldKeys := sets.StringKeySet(c.indexers)
	newKeys := sets.StringKeySet(newIndexers)

	// 检测 old是否存在new keys
	if oldKeys.HasAny(newKeys.List()...) {
		return fmt.Errorf("indexer conflict: %v", oldKeys.Intersection(newKeys))
	}

	// 添加
	for k, v := range newIndexers {
		c.indexers[k] = v
	}
	return nil
}

// updateIndices modifies the objects location in the managed indexes, if this is an update, you must provide an oldObj
// updateIndices must be called from a function that already has a lock on the cache
// 当有对象添加或者更新是，需要更新索引，因为代用该函数的函数已经加锁了
// 所以这个函数没有加锁操作
// 老的队象，新的对象，对象键
func (c *threadSafeMap) updateIndices(oldObj interface{}, newObj interface{}, key string) {
	// if we got an old object, we need to remove it before we add it again
	// 在添加和更新的时候都会获取老对象，如果存在老对象，那么就要删除老对象的索引
	if oldObj != nil {
		c.deleteFromIndices(oldObj, key)
	}
	// 遍历所有的索引函数，为对象在所有的索引分类中创建索引键
	for name, indexFunc := range c.indexers {
		// 计算索引键
		indexValues, err := indexFunc(newObj)
		if err != nil {
			panic(fmt.Errorf("unable to calculate an index entry for key %q on index %q: %v", key, name, err))
		}
		// 获取索引分类的所有索引
		index := c.indices[name]
		if index == nil {
			// 这个索引分类还没有任何索引
			index = Index{}
			c.indices[name] = index
		}
		// 遍历对象的索引键，用索引函数计算出来的
		for _, indexValue := range indexValues {
			// 找到索引键的对象集合
			set := index[indexValue]
			// 为空说明这个索引键下还没有对象
			if set == nil {
				// 创建对象键集合
				set = sets.String{}
				index[indexValue] = set
			}
			// 把对象键添加到集合中
			set.Insert(key)
		}
	}
}

// deleteFromIndices removes the object from each of the managed indexes
// it is intended to be called from a function that already has a lock on the cache
// 删除对象索引
func (c *threadSafeMap) deleteFromIndices(obj interface{}, key string) {
	// 遍历索引函数
	for name, indexFunc := range c.indexers {
		// 计算对象的所有索引键
		indexValues, err := indexFunc(obj)
		if err != nil {
			panic(fmt.Errorf("unable to calculate an index entry for key %q on index %q: %v", key, name, err))
		}

		// 获取索引分类的所有索引
		index := c.indices[name]
		if index == nil {
			continue
		}
		// 遍历对象的索引键
		for _, indexValue := range indexValues {
			// 把对象从索引键指定对对象集合删除
			set := index[indexValue]
			if set != nil {
				set.Delete(key)

				// If we don't delete the set when zero, indices with high cardinality
				// short lived resources can cause memory to increase over time from
				// unused empty sets. See `kubernetes/kubernetes/issues/84959`.
				if len(set) == 0 {
					delete(index, indexValue)
				}
			}
		}
	}
}

func (c *threadSafeMap) Resync() error {
	// Nothing to do
	return nil
}

// NewThreadSafeStore creates a new instance of ThreadSafeStore.
func NewThreadSafeStore(indexers Indexers, indices Indices) ThreadSafeStore {
	return &threadSafeMap{
		items:    map[string]interface{}{},
		indexers: indexers,
		indices:  indices,
	}
}
