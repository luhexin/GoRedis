// Package cluster 为router.go服务, ping和本地执行之间的映射
package cluster

import "GoRedis/interface/resp"

// 本地执行
func ping(cluster *ClusterDatabase, c resp.Connection, cmdAndArgs [][]byte) resp.Reply {
	return cluster.db.Exec(c, cmdAndArgs)
}
