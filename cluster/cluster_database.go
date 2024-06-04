package cluster

import (
	"GoRedis/config"
	"GoRedis/database"
	databaseface "GoRedis/interface/database"
	"GoRedis/interface/resp"
	"GoRedis/lib/consistenthash"
	"GoRedis/lib/logger"
	"GoRedis/resp/reply"
	"context"
	"fmt"
	pool "github.com/jolestar/go-commons-pool/v2"
	"runtime/debug"
	"strings"
)

type ClusterDatabase struct {
	self string //记录自己的名称地址

	nodes          []string                    //整个集群的节点
	peerPicker     *consistenthash.NodeMap     //节点选择器
	peerConnection map[string]*pool.ObjectPool //节点的地址：连接池; 三个节点需要两个连接池
	db             databaseface.Database       //下层：standalone_database
}

func MakeClusterDatabase() *ClusterDatabase {
	cluster := &ClusterDatabase{
		self: config.Properties.Self,

		db:             database.NewStandaloneDatabase(),
		peerPicker:     consistenthash.NewNodeMap(nil),
		peerConnection: make(map[string]*pool.ObjectPool),
	}
	nodes := make([]string, 0, len(config.Properties.Peers)+1)
	for _, peer := range config.Properties.Peers {
		nodes = append(nodes, peer)
	}
	nodes = append(nodes, config.Properties.Self)
	cluster.peerPicker.AddNode(nodes...)
	ctx := context.Background()
	for _, peer := range config.Properties.Peers {
		cluster.peerConnection[peer] = pool.NewObjectPoolWithDefaultConfig(ctx, &connectionFactory{
			Peer: peer,
		})
	}
	cluster.nodes = nodes
	return cluster
}

// CmdFunc 指令和执行模式之间的映射
type CmdFunc func(cluster *ClusterDatabase, c resp.Connection, cmdAndArgs [][]byte) resp.Reply

// Close 关闭集群层下面单机版的db
func (cluster *ClusterDatabase) Close() {
	cluster.db.Close()
}

// 启动路由表：指令和执行模式之间的关系
var router = makeRouter()

func (cluster *ClusterDatabase) Exec(c resp.Connection, cmdLine [][]byte) (result resp.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			result = &reply.UnknownErrReply{}
		}
	}()
	// 1. 识别传入的指令名称
	cmdName := strings.ToLower(string(cmdLine[0]))
	// 2. router：指令名称和执行方式一一对应，根据指令名称找到执行方式
	cmdFunc, ok := router[cmdName]
	if !ok {
		return reply.MakeErrReply("ERR unknown command '" + cmdName + "', or not supported in cluster mode")
	}
	// 3. 执行
	result = cmdFunc(cluster, c, cmdLine)
	return
}

func (cluster *ClusterDatabase) AfterClientClose(c resp.Connection) {
	cluster.db.AfterClientClose(c)
}
