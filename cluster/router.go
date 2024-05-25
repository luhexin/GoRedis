// Package cluster 指令和执行模式之间做一个关系映射
package cluster

import "GoRedis/interface/resp"

type CmdLine = [][]byte

// 指令和执行方式(广播、转发、本地执行)之间的映射
func makeRouter() map[string]CmdFunc {
	routerMap := make(map[string]CmdFunc)

	routerMap["exists"] = defaultFunc //exits k
	routerMap["type"] = defaultFunc
	routerMap["set"] = defaultFunc
	routerMap["setnx"] = defaultFunc
	routerMap["get"] = defaultFunc
	routerMap["getset"] = defaultFunc

	routerMap["del"] = Del

	routerMap["rename"] = Rename
	routerMap["renamenx"] = Rename

	routerMap["ping"] = ping

	routerMap["flushdb"] = FlushDB
	routerMap["select"] = execSelect

	return routerMap
}

// relay 转发方法: GET Key、Set k1 v1
func defaultFunc(cluster *ClusterDatabase, c resp.Connection, args [][]byte) resp.Reply {
	key := string(args[1])
	peer := cluster.peerPicker.PickNode(key) // 一致性哈希，返回节点哈希
	return cluster.relay(peer, c, args)
}
