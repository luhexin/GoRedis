package consistenthash

import (
	"hash/crc32"
	"sort"
)

// HashFunc 定义哈希函数
type HashFunc func(data []byte) uint32

// NodeMap 存储所有节点的信息、所有节点一致性hash
type NodeMap struct {
	hashFunc    HashFunc       // 哈希函数
	nodeHashs   []int          // 记录node的哈希值; 为了排序
	nodehashMap map[int]string // key: 哈希值; val: 地址
}

func NewNodeMap(fn HashFunc) *NodeMap {
	m := &NodeMap{
		hashFunc:    fn,
		nodehashMap: make(map[int]string),
	}
	if m.hashFunc == nil {
		m.hashFunc = crc32.ChecksumIEEE
	}
	return m
}

// IsEmpty 判断NodeMap是否为空
func (m *NodeMap) IsEmpty() bool {
	return len(m.nodeHashs) == 0
}

// AddNode 将节点加入到一致性哈希环上
func (m *NodeMap) AddNode(keys ...string) { // key是唯一确定节点的东西; 可以是节点名称、ip
	for _, key := range keys {
		if key == "" {
			continue
		}
		hash := int(m.hashFunc([]byte(key)))    //计算哈希值
		m.nodeHashs = append(m.nodeHashs, hash) //记录哈希值
		m.nodehashMap[hash] = key               // 记录哈希值和节点间的映射
	}
	sort.Ints(m.nodeHashs) // 哈希值排序
}

// PickNode 根据当前k，返回所属的节点
func (m *NodeMap) PickNode(key string) string {
	// 0. 判空
	if m.IsEmpty() {
		return ""
	}
	// 1. 对key做哈希
	hash := int(m.hashFunc([]byte(key)))

	// 2. 搜索哈希值落在那两个哈希之间，从而确定应该操作的节点
	idx := sort.Search(len(m.nodeHashs), func(i int) bool {
		return m.nodeHashs[i] >= hash
	})
	if idx == len(m.nodeHashs) { // 如果落在之后，应该去0号节点
		idx = 0
	}

	return m.nodehashMap[m.nodeHashs[idx]]
}
