package connection

import (
	"GoRedis/lib/sync/wait"
	"net"
	"sync"
	"time"
)

// Connection represents a connection with a redis-cli
type Connection struct {
	conn         net.Conn
	waitingReply wait.Wait //防止给客户端回发结果时，服务被kill，关闭server之前把reply处理完
	mu           sync.Mutex
	selectedDB   int
}

func NewConn(conn net.Conn) *Connection {
	return &Connection{
		conn: conn,
	}
}

func (c *Connection) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

// Close 与客户断开连接
func (c *Connection) Close() error {
	c.waitingReply.WaitWithTimeout(10 * time.Second) // 等待一次通信结束之后，断开连接
	_ = c.conn.Close()
	return nil
}

// Write 通过 tcp 连接向客户端发送响应
func (c *Connection) Write(b []byte) error {
	if len(b) == 0 {
		return nil
	}
	c.mu.Lock()
	c.waitingReply.Add(1)
	defer func() {
		c.waitingReply.Done()
		c.mu.Unlock()
	}()

	_, err := c.conn.Write(b)
	return err
}

// GetDBIndex returns selected db
func (c *Connection) GetDBIndex() int {
	return c.selectedDB
}

// SelectDB selects a database
func (c *Connection) SelectDB(dbNum int) {
	c.selectedDB = dbNum
}
