package cluster

import (
	"GoRedis/interface/resp"
	"GoRedis/resp/reply"
)

// Del 从集群中原子移除给定的key，key可以分布在任何节点上
// del k1 k2 k3 k4 k5; return 成功删除的个数
func Del(cluster *ClusterDatabase, c resp.Connection, args [][]byte) resp.Reply {
	// 1. 广播删除指令
	replies := cluster.broadcast(c, args)
	var errReply reply.ErrorReply
	var deleted int64 = 0
	// 2. 遍历所有的回复
	for _, v := range replies {
		if reply.IsErrorReply(v) {
			errReply = v.(reply.ErrorReply)
			break
		}
		intReply, ok := v.(*reply.IntReply)
		if !ok {
			errReply = reply.MakeErrReply("error")
		}
		// 3. 记录删除的个数
		deleted += intReply.Code
	}

	if errReply == nil {
		return reply.MakeIntReply(deleted)
	}
	return reply.MakeErrReply("error occurs: " + errReply.Error())
}
