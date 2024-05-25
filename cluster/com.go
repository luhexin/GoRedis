package cluster

/*负责节点间的通信*/

import (
	"GoRedis/interface/resp"
	"GoRedis/lib/utils"
	"GoRedis/resp/client"
	"GoRedis/resp/reply"
	"context"
	"errors"
	"strconv"
)

// 在连接池里获取一个连接，进行转发指令使用
func (cluster *ClusterDatabase) getPeerClient(peer string) (*client.Client, error) {
	// 1. 根据传入的兄弟节点的地址拿到连接池
	factory, ok := cluster.peerConnection[peer]
	if !ok {
		return nil, errors.New("connection factory not found")
	}
	// 2. 在连接池里面取出一个客户端
	raw, err := factory.BorrowObject(context.Background())
	if err != nil {
		return nil, err
	}
	// 3. 转换客户端的类型
	conn, ok := raw.(*client.Client)
	if !ok {
		return nil, errors.New("connection factory make wrong type")
	}
	// 4. 返回
	return conn, nil
}

// 归坏连接client，防止连接池耗尽
func (cluster *ClusterDatabase) returnPeerClient(peer string, peerClient *client.Client) error {
	connectionFactory, ok := cluster.peerConnection[peer]
	if !ok {
		return errors.New("connection factory not found")
	}
	return connectionFactory.ReturnObject(context.Background(), peerClient)
}

// relay 将指令转发到正确的节点;
// 传入选好的目标节点、用户的连接信息、用户的指令
func (cluster *ClusterDatabase) relay(peer string, c resp.Connection, args [][]byte) resp.Reply {
	// 1. 判断目标节点是否是自己
	if peer == cluster.self {
		return cluster.db.Exec(c, args)
	}
	// 2. 操作兄弟节点; 拿一个连接出来
	peerClient, err := cluster.getPeerClient(peer)
	if err != nil {
		return reply.MakeErrReply(err.Error())
	}
	defer func() { // 避免连接耗尽，注册归还连接
		_ = cluster.returnPeerClient(peer, peerClient)
	}()
	// 3. 给目标节点发送SELECT dbNum, 用于切换具体的哪一个db(单机的redis包含16个db)
	peerClient.Send(utils.ToCmdLine("SELECT", strconv.Itoa(c.GetDBIndex())))
	// 4. 转发指令
	return peerClient.Send(args)
}

// broadcast 广播给所有节点
func (cluster *ClusterDatabase) broadcast(c resp.Connection, args [][]byte) map[string]resp.Reply {
	result := make(map[string]resp.Reply)
	for _, node := range cluster.nodes {
		reply := cluster.relay(node, c, args)
		result[node] = reply
	}
	return result
}
