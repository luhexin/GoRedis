package dict

import "sync"

// SyncDict 封装了一个映射，它不是线程安全的
type SyncDict struct {
	m sync.Map
}

// MakeSyncDict makes a new map
func MakeSyncDict() *SyncDict {
	return &SyncDict{}
}

// Get 返回value和是否存在
func (dict *SyncDict) Get(key string) (val interface{}, exists bool) {
	val, ok := dict.m.Load(key)
	return val, ok
}

// Len 返回dict的长度
func (dict *SyncDict) Len() int {
	lenth := 0
	dict.m.Range(func(k, v interface{}) bool {
		lenth++
		return true
	})
	return lenth
}

// Put 将键值放入 dict，并返回新插入键值的数量
func (dict *SyncDict) Put(key string, val interface{}) (result int) {
	_, existed := dict.m.Load(key)
	dict.m.Store(key, val)
	if existed {
		return 0
	}
	return 1
}

// PutIfAbsent 如果键不存在，则输入值，并返回更新键值的数量
func (dict *SyncDict) PutIfAbsent(key string, val interface{}) (result int) {
	_, existed := dict.m.Load(key)
	if existed {
		return 0
	}
	dict.m.Store(key, val)
	return 1
}

// PutIfExists 如果键存在，则输入值，并返回插入的键值个数
func (dict *SyncDict) PutIfExists(key string, val interface{}) (result int) {
	_, existed := dict.m.Load(key)
	if existed {
		dict.m.Store(key, val)
		return 1
	}
	return 0
}

// Remove 删除键值，并返回已删除键值的个数
func (dict *SyncDict) Remove(key string) (result int) {
	_, existed := dict.m.Load(key)
	dict.m.Delete(key)
	if existed {
		return 1
	}
	return 0
}

// Keys 返回所有的key
func (dict *SyncDict) Keys() []string {
	result := make([]string, dict.Len())
	i := 0
	dict.m.Range(func(key, value interface{}) bool {
		result[i] = key.(string)
		i++
		return true
	})
	return result
}

// ForEach 遍历 dict
func (dict *SyncDict) ForEach(consumer Consumer) {
	dict.m.Range(func(key, value interface{}) bool {
		consumer(key.(string), value)
		return true
	})
}

// RandomKeys 随机返回key，可能包含重复key
func (dict *SyncDict) RandomKeys(limit int) []string {
	result := make([]string, limit)
	for i := 0; i < limit; i++ {
		dict.m.Range(func(key, value interface{}) bool {
			result[i] = key.(string)
			return false
		})
	}
	return result

}

// RandomDistinctKeys 随机返回key，不包含重复key
func (dict *SyncDict) RandomDistinctKeys(limit int) []string {
	result := make([]string, limit)
	i := 0
	dict.m.Range(func(key, value interface{}) bool {
		result[i] = key.(string)
		i++
		if i == limit {
			return false
		}
		return true
	})
	return result
}

// Clear 删除所有key
func (dict *SyncDict) Clear() {
	*dict = *MakeSyncDict() //旧的由GC回收
}
