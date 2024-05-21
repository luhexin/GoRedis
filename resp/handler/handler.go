package handler

import (
	"GoRedis/database"
	databaseface "GoRedis/interface/database"
	"GoRedis/lib/logger"
	"GoRedis/lib/sync/atomic"
	"GoRedis/resp/connection"
	"GoRedis/resp/parser"
	"GoRedis/resp/reply"
	"context"
	"io"
	"net"
	"strings"
	"sync"
)

/*
 * A tcp.RespHandler implements redis protocol
 */

var (
	unknownErrReplyBytes = []byte("-ERR unknown\r\n")
)

// RespHandler 实现 tcp.Handler 并充当 redis 处理程序
type RespHandler struct {
	activeConn sync.Map              // 记录了协议层保留的所有客户端
	db         databaseface.Database // 持有redis核心业务层，处理kv操作，实际业务逻辑
	closing    atomic.Boolean        // 连接是否关闭
}

// MakeHandler creates a RespHandler instance
func MakeHandler() *RespHandler {
	var db databaseface.Database
	db = database.NewEchoDatabase() //redis 内核
	return &RespHandler{
		db: db,
	}
}

// 关闭一个客户端
func (h *RespHandler) closeClient(client *connection.Connection) {
	_ = client.Close()
	h.db.AfterClientClose(client)
	h.activeConn.Delete(client)
}

// Handle 接收并执行 redis 命令
func (h *RespHandler) Handle(ctx context.Context, conn net.Conn) {
	if h.closing.Get() { // 如果handler当前正处于关闭中
		_ = conn.Close() // 关闭新的连接
	}

	client := connection.NewConn(conn)
	h.activeConn.Store(client, 1)

	ch := parser.ParseStream(conn) // 解析报文
	for payload := range ch {      //监听管道，死循环
		// 异常处理
		if payload.Err != nil {
			// 用户断开连接 / 意外EOF / 使用了被关闭的链接
			if payload.Err == io.EOF ||
				payload.Err == io.ErrUnexpectedEOF ||
				strings.Contains(payload.Err.Error(), "use of closed network connection") {
				// 关闭连接
				h.closeClient(client)
				logger.Info("connection closed: " + client.RemoteAddr().String())
				return
			}
			// 协议错误
			errReply := reply.MakeErrReply(payload.Err.Error())
			err := client.Write(errReply.ToBytes())
			if err != nil {
				h.closeClient(client)
				logger.Info("connection closed: " + client.RemoteAddr().String())
				return
			}
			continue
		}

		// 正常执行
		if payload.Data == nil {
			logger.Error("empty payload")
			continue
		}
		r, ok := payload.Data.(*reply.MultiBulkReply)
		if !ok {
			logger.Error("require multi bulk reply")
			continue
		}
		result := h.db.Exec(client, r.Args)
		if result != nil {
			_ = client.Write(result.ToBytes())
		} else {
			_ = client.Write(unknownErrReplyBytes)
		}
	}
}

// Close 关闭所有client
func (h *RespHandler) Close() error {
	logger.Info("handler shutting down...")
	h.closing.Set(true)
	// TODO: concurrent wait
	h.activeConn.Range(func(key interface{}, val interface{}) bool { //遍历连接的客户端
		client := key.(*connection.Connection)
		_ = client.Close()
		return true
	})
	h.db.Close()
	return nil
}
