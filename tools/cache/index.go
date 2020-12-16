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

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/util/sets"
)

//Indexer保存了来自apiServer的资源。
//使用listWatch方式来维护资源的增量变化。
//通过这种方式可以减小对apiServer的访问，
//减轻apiServer端的压力
// cache实现Indexer的接口
// Indexer extends Store with multiple indices and restricts each
// accumulator to simply hold the current object (and be empty after
// Delete).
//
// There are three kinds of strings here:
// 1. a storage key, as defined in the Store interface,
// 2. a name of an index, and
// 3. an "indexed value", which is produced by an IndexFunc and
//    can be a field value or any other string computed from the object.
type Indexer interface {
	// 继承store的接口，Store中定义了对对象的增删改查等方法
	Store
	// Index returns the stored objects whose set of indexed values
	// intersects the set of indexed values of the given object, for
	// the named index
	// indexName索引类名,obj是对象,计算obj在indexName类中的索引索引键,
	// 通过索引键把所有的对象取出来,即获取符合obj特征的所有对象,
	// 所谓的特征就是对象在索引类中的索引键
	Index(indexName string, obj interface{}) ([]interface{}, error)
	// IndexKeys returns the storage keys of the stored objects whose
	// set of indexed values for the named index includes the given
	// indexed value
	// indexKey是indexName索引类中一个索引键，函数返回indexKey指定的所有对象键
	// 这个对象键是Indexer内唯一的，在添加的时候会计算
	IndexKeys(indexName, indexedValue string) ([]string, error)
	// ListIndexFuncValues returns all the indexed values of the given index
	// 获取indexName索引类中的所有索引键
	ListIndexFuncValues(indexName string) []string
	// ByIndex returns the stored objects whose set of indexed values
	// for the named index includes the given indexed value
	// 通过索引,返回所有的对象
	ByIndex(indexName, indexedValue string) ([]interface{}, error)
	// GetIndexer return the indexers
	// 返回Indexers
	GetIndexers() Indexers

	// AddIndexers adds more indexers to this store.  If you call this after you already have data
	// in the store, the results are undefined.
	// 添加Indexers，增加更多的索引分类
	AddIndexers(newIndexers Indexers) error
}

// IndexFunc knows how to compute the set of indexed values for an object.
// IndexFunc知道如何计算对象的一组索引值,传入对象，输出字符串索引。
type IndexFunc func(obj interface{}) ([]string, error)

// IndexFuncToKeyFuncAdapter adapts an indexFunc to a keyFunc.  This is only useful if your index function returns
// unique values for every object.  This conversion can create errors when more than one key is found.  You
// should prefer to make proper key and index functions.
func IndexFuncToKeyFuncAdapter(indexFunc IndexFunc) KeyFunc {
	return func(obj interface{}) (string, error) {
		indexKeys, err := indexFunc(obj)
		if err != nil {
			return "", err
		}
		if len(indexKeys) > 1 {
			return "", fmt.Errorf("too many keys: %v", indexKeys)
		}
		if len(indexKeys) == 0 {
			return "", fmt.Errorf("unexpected empty indexKeys")
		}
		return indexKeys[0], nil
	}
}

const (
	// NamespaceIndex is the lookup name for the most comment index function, which is to index by the namespace field.
	NamespaceIndex string = "namespace"
)

// MetaNamespaceIndexFunc is a default index function that indexes based on an object's namespace
// 根据对象的namespace分类
func MetaNamespaceIndexFunc(obj interface{}) ([]string, error) {
	meta, err := meta.Accessor(obj)
	if err != nil {
		return []string{""}, fmt.Errorf("object has no meta: %v", err)
	}
	return []string{meta.GetNamespace()}, nil
}

// Index maps the indexed value to a set of keys in the store that match on that value
// 每种计算索引的方式会输出多个索引,而多个目标可能会算出相同索引，所以就有了这个类型
type Index map[string]sets.String //  sets.String map[string]Empty string,对象键，Empty为对象

// Indexers maps a name to a IndexFunc
// 计算索引的方式:计算索引的函数有很多，用名字分类
type Indexers map[string]IndexFunc

// Indices maps a name to an Index
// 索引计算索引的方式:由于有多种计算索引的方式，那就又要按照计算索引的方式组织索引
type Indices map[string]Index
