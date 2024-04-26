package ikundb

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/emirpasic/gods/sets/treeset"
	"github.com/emirpasic/gods/utils"
	"github.com/google/btree"
	art "github.com/plar/go-adaptive-radix-tree"
)

type mapIndex interface {
	set(key []byte, value *pos)
	get(key []byte) (*pos, bool)
	del(key []byte)
	iter(f func(key []byte, value *pos) bool)
	show()
	close() error
}

var indexers = map[IndexType]mapIndex{
	SyncMap:  newkvMap(),
	Btree:    newBTreeIndex(),
	Art:      newArtIndex(),
	OrderMap: newOrderMapIndex(),
	DeFault:  newkvMap(),
}

type kvMap struct {
	km sync.Map
}

func newkvMap() mapIndex {
	return &kvMap{km: sync.Map{}}
}
func (k *kvMap) set(key []byte, value *pos) {
	k.km.Store(string(key), value)
}

func (k *kvMap) get(key []byte) (*pos, bool) {
	value, ok := k.km.Load(string(key))
	if !ok {
		return nil, false
	}
	return value.(*pos), true
}

func (k *kvMap) del(key []byte) {
	k.km.Delete(string(key))
}

func (k *kvMap) show() {
	k.km.Range(func(ki, vi interface{}) bool {
		key := []byte(ki.(string))
		value := vi.(*pos)
		fmt.Println("key:", string(key), "value:", value)
		return true
	})
}
func (m *kvMap) iter(f func(key []byte, value *pos) bool) {
	m.km.Range(func(k, v interface{}) bool {
		key := []byte(k.(string))
		value := v.(*pos)
		return f(key, value)
	})
}

func (m *kvMap) close() error {
	return nil
}

// 实现btree部分
type btreeIndex struct {
	mu   sync.RWMutex
	tree *btree.BTree
}

func newBTreeIndex() *btreeIndex {
	return &btreeIndex{
		tree: btree.New(32),
	}
}

func (bi *btreeIndex) set(key []byte, value *pos) {
	bi.mu.Lock()
	defer bi.mu.Unlock()
	bi.tree.ReplaceOrInsert(&btreeItem{key: key, value: value})
}

func (bi *btreeIndex) get(key []byte) (*pos, bool) {
	bi.mu.RLock()
	defer bi.mu.RUnlock()
	item := bi.tree.Get(&btreeItem{key: key})
	if item == nil {
		return nil, false
	}
	return item.(*btreeItem).value, true
}

func (bi *btreeIndex) del(key []byte) {
	bi.mu.Lock()
	defer bi.mu.Unlock()
	bi.tree.Delete(&btreeItem{key: key})
}
func (bi *btreeIndex) iter(f func(key []byte, value *pos) bool) {
	bi.mu.RLock()
	defer bi.mu.RUnlock()
	bi.tree.Ascend(func(item btree.Item) bool {
		return f(item.(*btreeItem).key, item.(*btreeItem).value)
	})
}

func (bi *btreeIndex) show() {
	bi.mu.RLock()
	defer bi.mu.RUnlock()
	bi.tree.Ascend(func(item btree.Item) bool {
		fmt.Printf("Key: %s, Value: %+v\n", item.(*btreeItem).key, item.(*btreeItem).value)
		return true
	})
}

func (bi *btreeIndex) close() error {
	return nil
}

type btreeItem struct {
	key   []byte
	value *pos
}

func (a *btreeItem) Less(b btree.Item) bool {
	return string(a.key) < string(b.(*btreeItem).key)
}

type artIndex struct {
	mu   sync.RWMutex
	tree art.Tree
}

func newArtIndex() *artIndex {
	return &artIndex{
		tree: art.New(),
	}
}

func (ai *artIndex) set(key []byte, value *pos) {
	ai.mu.Lock()
	defer ai.mu.Unlock()
	ai.tree.Insert(key, value)
}

func (ai *artIndex) get(key []byte) (*pos, bool) {
	ai.mu.RLock()
	defer ai.mu.RUnlock()
	value, ok := ai.tree.Search(key)
	if !ok {
		return nil, false
	}
	return value.(*pos), true
}

func (ai *artIndex) del(key []byte) {
	ai.mu.Lock()
	defer ai.mu.Unlock()
	ai.tree.Delete(key)
}

func (ai *artIndex) iter(f func(key []byte, value *pos) bool) {
	ai.mu.RLock()
	defer ai.mu.RUnlock()
	ai.tree.ForEach(func(node art.Node) bool {
		key := node.Key()
		value := node.Value().(*pos)
		return f(key, value)
	})
}

func (ai *artIndex) show() {
	ai.mu.RLock()
	defer ai.mu.RUnlock()
	ai.iter(func(key []byte, value *pos) bool {
		fmt.Printf("Key: %s, Value: %+v\n", key, value)
		return true
	})
}

func (ai *artIndex) close() error {
	return nil
}

// orderMapIndex 是实现 mapIndex 接口的结构体
type orderMapIndex struct {
	data *treeset.Set
}

// newOrderMapIndex 创建一个新的 orderMapIndex 实例
func newOrderMapIndex() *orderMapIndex {
	return &orderMapIndex{
		data: treeset.NewWith(utils.Comparator(posComparator)),
	}
}

// 实现 set 方法
func (mi *orderMapIndex) set(key []byte, value *pos) {
	mi.data.Add(&tx{key, value})
}

// 实现 get 方法
func (mi *orderMapIndex) get(key []byte) (*pos, bool) {
	values := mi.data.Values()
	for _, v := range values {
		kv := v.(*tx)
		if bytes.Equal(kv.key, key) {
			return kv.po, true
		}
	}
	return nil, false
}

// 实现 del 方法
func (mi *orderMapIndex) del(key []byte) {
	values := mi.data.Values()
	for _, v := range values {
		kv := v.(*tx)
		if bytes.Equal(kv.key, key) {
			mi.data.Remove(kv)
			break
		}
	}
}

// 实现 iter 方法
func (mi *orderMapIndex) iter(f func(key []byte, value *pos) bool) {
	values := mi.data.Values()
	for _, v := range values {
		kv := v.(*tx)
		if !f(kv.key, kv.po) {
			break
		}
	}
}

// 实现 show 方法
func (mi *orderMapIndex) show() {
	values := mi.data.Values()
	for _, v := range values {
		kv := v.(*tx)
		fmt.Printf("Key: %s, Value: %+v\n", kv.key, kv.po)
	}
}

// 实现 close 方法
func (mi *orderMapIndex) close() error {
	return nil
}

// posComparator 比较 pos 结构体的比较器
func posComparator(a, b interface{}) int {
	keyA := a.(*tx).key
	keyB := b.(*tx).key
	return bytes.Compare(keyA, keyB)
}
