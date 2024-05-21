package database

import "GoRedis/interface/resp"

// CmdLine 命令行
type CmdLine = [][]byte

// Database redis风格存储引擎
type Database interface {
	Exec(client resp.Connection, args [][]byte) resp.Reply
	AfterClientClose(c resp.Connection) // 比如关闭之后清理数据
	Close()
}

// DataEntity Redis数据结构，包括字符串、列表、散列、集合等
type DataEntity struct {
	Data interface{}
}
