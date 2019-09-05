package syncmap

import (
	"math/rand"
	"strings"
	"sync"
)

const (
	defaultShardCount int = 128
)

type ShardMap struct {
	items map[string]interface{}
	sync.RWMutex
}

func (sd *ShardMap) GetItems() map[string]interface{} {
	return sd.items
}

func (sd *ShardMap) GetNotLock(key string) (interface{}, bool) {
	v, ok := sd.items[key]
	return v, ok
}

func (sd *ShardMap) SetNotLock(key string, val interface{}) {
	sd.items[key] = val
}

func (sd *ShardMap) DeleteNotLock(key string) {
	delete(sd.items, key)
}

func (sd *ShardMap) GetWithLock(key string) (interface{}, bool) {
	sd.RLock()
	v, ok := sd.items[key]
	sd.RUnlock()
	return v, ok
}

func (sd *ShardMap) SetWithLock(key string, val interface{}) {
	sd.Lock()
	sd.items[key] = val
	sd.Unlock()
}

func (sd *ShardMap) DeleteWithLock(key string) {
	sd.Lock()
	delete(sd.items, key)
	sd.Unlock()
}

type SyncMap struct {
	shardCount int
	shards     []*ShardMap
}

func New() *SyncMap {
	return NewWithShard(defaultShardCount)
}

func NewWithShard(shardCount int) *SyncMap {
	if shardCount == 0 {
		shardCount = defaultShardCount
	}

	m := new(SyncMap)
	m.shardCount = shardCount
	m.shards = make([]*ShardMap, m.shardCount)
	for i, _ := range m.shards {
		m.shards[i] = &ShardMap{items: make(map[string]interface{})}
	}
	return m
}

func (m *SyncMap) Locate(key string) *ShardMap {
	return m.locate(key)
}

func (m *SyncMap) locate(key string) *ShardMap {
	return m.shards[fnv32(key)&uint32((m.shardCount-1))]
}

func (m *SyncMap) GetJoinKey(key ...string) (value interface{}, ok bool) {
	return m.Get(strings.Join(key[:], "-"))
}

func (m *SyncMap) GetShards() []*ShardMap {
	return m.shards
}

func (m *SyncMap) Get(key string) (value interface{}, ok bool) {
	shard := m.locate(key)
	shard.RLock()
	value, ok = shard.items[key]
	shard.RUnlock()
	return
}

func (m *SyncMap) Set(key string, value interface{}) {
	shard := m.locate(key)
	shard.Lock()
	shard.items[key] = value
	shard.Unlock()
}

func (m *SyncMap) Delete(key string) {
	shard := m.locate(key)
	shard.Lock()
	delete(shard.items, key)
	shard.Unlock()
}

func (m *SyncMap) Pop() (string, interface{}) {
	if m.Size() == 0 {
		panic("syncmap: map is empty")
	}

	var (
		key   string
		value interface{}
		found = false
		n     = int(m.shardCount)
	)

	for !found {
		idx := rand.Intn(n)
		shard := m.shards[idx]
		shard.Lock()
		if len(shard.items) > 0 {
			found = true
			for key, value = range shard.items {
				break
			}
			delete(shard.items, key)
		}
		shard.Unlock()
	}

	return key, value
}

func (m *SyncMap) Has(key string) bool {
	_, ok := m.Get(key)
	return ok
}

func (m *SyncMap) Size() int {
	size := 0
	for _, shard := range m.shards {
		shard.RLock()
		size += len(shard.items)
		shard.RUnlock()
	}
	return size
}

func (m *SyncMap) Flush() int {
	size := 0
	for _, shard := range m.shards {
		shard.Lock()
		size += len(shard.items)
		shard.items = make(map[string]interface{})
		shard.Unlock()
	}
	return size
}

type IterKeyWithBreakFunc func(key string) bool

func (m *SyncMap) EachKeyWithBreak(iter IterKeyWithBreakFunc) {
	stop := false
	for _, shard := range m.shards {
		shard.RLock()
		for key, _ := range shard.items {
			if !iter(key) {
				stop = true
				break
			}
		}
		shard.RUnlock()
		if stop {
			break
		}
	}
}

type Item struct {
	Key   string
	Value interface{}
}

type IterItemWithBreakFunc func(item *Item) bool

func (m *SyncMap) EachItemWithBreak(iter IterItemWithBreakFunc) {
	stop := false
	for _, shard := range m.shards {
		shard.RLock()
		for key, value := range shard.items {
			if !iter(&Item{key, value}) {
				stop = true
				break
			}
		}
		shard.RUnlock()
		if stop {
			break
		}
	}
}

type IterItemFunc func(item *Item)

func (m *SyncMap) EachItem(iter IterItemFunc) {
	f := func(item *Item) bool {
		iter(item)
		return true
	}
	m.EachItemWithBreak(f)
}

func (m *SyncMap) IterItems() <-chan Item {
	ch := make(chan Item)
	go func() {
		m.EachItem(func(item *Item) {
			ch <- *item
		})
		close(ch)
	}()
	return ch
}

func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}

