package cluster

import (
	"GoRedis/interface/resp"
	"GoRedis/resp/reply"
)

// Rename 重命名key，起始k v和目标k v必须在同一节点内, 否则返回错误
func Rename(cluster *ClusterDatabase, c resp.Connection, args [][]byte) resp.Reply {
	if len(args) != 3 { // rename k1 k2
		return reply.MakeErrReply("ERR wrong number of arguments for 'rename' command")
	}
	src := string(args[1])
	dest := string(args[2])

	srcPeer := cluster.peerPicker.PickNode(src)
	destPeer := cluster.peerPicker.PickNode(dest)

	if srcPeer != destPeer {
		return reply.MakeErrReply("ERR rename must within one slot in cluster mode")
	}
	return cluster.relay(srcPeer, c, args)
}
