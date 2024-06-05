package tcp

import (
	"GoRedis/lib/logger"
	"GoRedis/lib/sync/atomic"
	"GoRedis/lib/sync/wait"
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"time"
)

type EchoClient struct {
	Conn    net.Conn
	Watting wait.Wait //wait group
}

func (e *EchoClient) Close() error { //系统close 接口
	e.Watting.WaitWithTimeout(10 * time.Second) //等待客户端当前工作完成
	_ = e.Conn.Close()
	return nil
}

type EchoHandler struct {
	activeConn sync.Map //记录连接的EchoClient
	closing    atomic.Boolean
}

func MakeHandler() *EchoHandler {
	return &EchoHandler{}
}

func (handler *EchoHandler) Handle(ctx context.Context, conn net.Conn) {
	if handler.closing.Get() {
		_ = conn.Close()
	}

	client := &EchoClient{
		Conn: conn,
	}
	handler.activeConn.Store(client, struct{}{}) //记录正在连接的客户
	reader := bufio.NewReader(conn)              // 在缓存区接收用户发来的信息，因为信息时断时续，所以缓存区接收
	//服务客户, 如果信息换行则写回信息
	for true {
		msg, err := reader.ReadString('\n')
		if !errors.Is(err, nil) {
			if errors.Is(err, io.EOF) {
				logger.Info("Connection close")
				handler.activeConn.Delete(client)
			} else {
				logger.Warn(err)
			}
			return
		}
		client.Watting.Add(1)
		b := []byte(msg)
		_, _ = conn.Write(b)
		client.Watting.Done()
	}
}

func (handler *EchoHandler) Close() error {
	logger.Info("handler shutting down")
	handler.closing.Set(true)
	handler.activeConn.Range(func(key, value interface{}) bool { //关闭正在连接的客户端
		// 匿名方法内容，传递给Range，施加到每一个k v上
		client := key.(*EchoClient)
		_ = client.Conn.Close()
		return true // return true才可以施加到下一个 k v上
	})
	return nil
}
