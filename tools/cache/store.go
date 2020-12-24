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
	"strings"

	"k8s.io/apimachinery/pkg/api/meta"
)

// Store is a generic object storage and processing interface.  A
// Store holds a map from string keys to accumulators, and has
// operations to add, update, and delete a given object to/from the
// accumulator currently associated with a given key.  A Store also
// knows how to extract the key from a given object, so many operations
// are given only the object.
//
// In the simplest Store implementations each accumulator is simply
// the last given object, or empty after Delete, and thus the Store's
// behavior is simple storage.
//
// Reflector knows how to watch a server and update a Store.  This
// package provides a variety of implementations of Store.
type Store interface {

	// Add adds the given object to the accumulator associated with the given object's key
	// 添加对象
	Add(obj interface{}) error

	// Update updates the given object in the accumulator associated with the given object's key
	// 更新对象
	Update(obj interface{}) error

	// Delete deletes the given object from the accumulator associated with the given object's key
	// 删除对象
	Delete(obj interface{}) error

	// List returns a list of all the currently non-empty accumulators
	// 列举对象
	List() []interface{}

	// ListKeys returns a list of all the keys currently associated with non-empty accumulators
	// 列举对象键
	ListKeys() []string

	// Get returns the accumulator associated with the given object's key
	// 返回obj相同对象键的对象，对象键是通过对象计算出来的字符串
	Get(obj interface{}) (item interface{}, exists bool, err error)

	// GetByKey returns the accumulator associated with the given key
	// 通过对象键获取对象
	GetByKey(key string) (item interface{}, exists bool, err error)

	// Replace will delete the contents of the store, using instead the
	// given list. Store takes ownership of the list, you should not reference
	// it after calling this function.
	// 用[]interface{}替换Store存储的所有对象，等同于删除全部原有对象在逐一添加新的对象
	Replace([]interface{}, string) error

	// Resync is meaningless in the terms appearing here but has
	// meaning in some implementations that have non-trivial
	// additional behavior (e.g., DeltaFIFO).
	// 重新同步
	Resync() error
}

// KeyFunc knows how to make a key from an object. Implementations should be deterministic.
// 计算对象键的函数
type KeyFunc func(obj interface{}) (string, error)

// KeyError will be returned any time a KeyFunc gives an error; it includes the object
// at fault.
type KeyError struct {
	Obj interface{}
	Err error
}

// Error gives a human-readable description of the error.
func (k KeyError) Error() string {
	return fmt.Sprintf("couldn't create key for object %+v: %v", k.Obj, k.Err)
}

// ExplicitKey can be passed to MetaNamespaceKeyFunc if you have the key for
// the object but not the object itself.
type ExplicitKey string

// MetaNamespaceKeyFunc is a convenient default KeyFunc which knows how to make
// keys for API objects which implement meta.Interface.
// The key uses the format <namespace>/<name> unless <namespace> is empty, then
// it's just <name>.
//
// TODO: replace key-as-string with a key-as-struct so that this
// packing/unpacking won't be necessary.
// keyFuncd对象键函数的实现: 获取对象的namespace/name作为键
func MetaNamespaceKeyFunc(obj interface{}) (string, error) {
	if key, ok := obj.(ExplicitKey); ok {
		return string(key), nil
	}
	meta, err := meta.Accessor(obj)
	if err != nil {
		return "", fmt.Errorf("object has no meta: %v", err)
	}
	if len(meta.GetNamespace()) > 0 {
		return meta.GetNamespace() + "/" + meta.GetName(), nil
	}
	return meta.GetName(), nil
}

// SplitMetaNamespaceKey returns the namespace and name that
// MetaNamespaceKeyFunc encoded into key.
//
// TODO: replace key-as-string with a key-as-struct so that this
// packing/unpacking won't be necessary.
func SplitMetaNamespaceKey(key string) (namespace, name string, err error) {
	parts := strings.Split(key, "/")
	switch len(parts) {
	case 1:
		// name only, no namespace
		return "", parts[0], nil
	case 2:
		// namespace and name
		return parts[0], parts[1], nil
	}

	return "", "", fmt.Errorf(  "unexpected key format: %q", key)
}

// `*cache` implements Indexer in terms of a ThreadSafeStore and an
// associated KeyFunc.
// `* cache`根据ThreadSafeStore和
// 相关的KeyFunc实现了Indexer接口。
type cache struct {
	// cacheStorage bears the burden of thread safety for the cache
	////线程安全的存储
	cacheStorage ThreadSafeStore
	// keyFunc is used to make the key for objects stored in and retrieved from items, and
	// should be deterministic.
	// 计算对象键的函数，创建cache对象的时候需要指定
	keyFunc KeyFunc
}

var _ Store = &cache{}

// Add inserts an item into the cache.
func (c *cache) Add(obj interface{}) error {
	key, err := c.keyFunc(obj) // 生成对象键的
	if err != nil {
		return KeyError{obj, err}
	}
	// 对象键:对象存储
	c.cacheStorage.Add(key, obj)
	return nil
}

// Update sets an item in the cache to its updated state.
// 更新对象
func (c *cache) Update(obj interface{}) error {
	key, err := c.keyFunc(obj)
	if err != nil {
		return KeyError{obj, err}
	}
	// 更新存储
	c.cacheStorage.Update(key, obj)
	return nil
}

// Delete removes an item from the cache.
// 删除对象
func (c *cache) Delete(obj interface{}) error {
	key, err := c.keyFunc(obj)
	if err != nil {
		return KeyError{obj, err}
	}
	c.cacheStorage.Delete(key)
	return nil
}

// List returns a list of all the items.
// List is completely threadsafe as long as you treat all items as immutable.
// 列举对象
func (c *cache) List() []interface{} {
	return c.cacheStorage.List()
}

// ListKeys returns a list of all the keys of the objects currently
// in the cache.
// 列举对象键
func (c *cache) ListKeys() []string {
	return c.cacheStorage.ListKeys()
}

// GetIndexers returns the indexers of cache
// 获取 分类名:索引键的计算函数
func (c *cache) GetIndexers() Indexers {
	return c.cacheStorage.GetIndexers()
}

// Index returns a list of items that match on the index function
// Index is thread-safe so long as you treat all items as immutable
// 通过指定的索引函数计算对象的索引键，然后把索引键的对象全部取出来
func (c *cache) Index(indexName string, obj interface{}) ([]interface{}, error) {
	return c.cacheStorage.Index(indexName, obj)
}

// 通过指定的索引函数,索引键，把索引键的对象键全部取出来
func (c *cache) IndexKeys(indexName, indexKey string) ([]string, error) {
	return c.cacheStorage.IndexKeys(indexName, indexKey)
}

// ListIndexFuncValues returns the list of generated values of an Index func
func (c *cache) ListIndexFuncValues(indexName string) []string {
	return c.cacheStorage.ListIndexFuncValues(indexName)
}

func (c *cache) ByIndex(indexName, indexKey string) ([]interface{}, error) {
	return c.cacheStorage.ByIndex(indexName, indexKey)
}

func (c *cache) AddIndexers(newIndexers Indexers) error {
	return c.cacheStorage.AddIndexers(newIndexers)
}

// Get returns the requested item, or sets exists=false.
// Get is completely threadsafe as long as you treat all items as immutable.
func (c *cache) Get(obj interface{}) (item interface{}, exists bool, err error) {
	key, err := c.keyFunc(obj)
	if err != nil {
		return nil, false, KeyError{obj, err}
	}
	return c.GetByKey(key)
}

// GetByKey returns the request item, or exists=false.
// GetByKey is completely threadsafe as long as you treat all items as immutable.
func (c *cache) GetByKey(key string) (item interface{}, exists bool, err error) {
	item, exists = c.cacheStorage.Get(key)
	return item, exists, nil
}

// Replace will delete the contents of 'c', using instead the given list.
// 'c' takes ownership of the list, you should not reference the list again
// after calling this function.
// 替换存储中item
func (c *cache) Replace(list []interface{}, resourceVersion string) error {
	items := make(map[string]interface{}, len(list))
	for _, item := range list {
		key, err := c.keyFunc(item)
		if err != nil {
			return KeyError{item, err}
		}
		items[key] = item
	}
	c.cacheStorage.Replace(items, resourceVersion)
	return nil
}

// Resync is meaningless for one of these
func (c *cache) Resync() error {
	return nil
}

// 初始化cache
// NewStore returns a Store implemented simply with a map and a lock.
func NewStore(keyFunc KeyFunc) Store {
	return &cache{
		cacheStorage: NewThreadSafeStore(Indexers{}, Indices{}),// 数据载体
		keyFunc:      keyFunc,
	}
}

// NewIndexer returns an Indexer implemented simply with a map and a lock.
func NewIndexer(keyFunc KeyFunc, indexers Indexers) Indexer {
	return &cache{
		cacheStorage: NewThreadSafeStore(indexers, Indices{}),// 数据载体
		keyFunc:      keyFunc,
	}
}
