// Package database 代表Redis的分数据库
package database

import (
	"GoRedis/datastruct/dict"
	"GoRedis/interface/database"
	"GoRedis/interface/resp"
	"GoRedis/resp/reply"
	"strings"
)

// DB 存储数据并执行用户命令
type DB struct {
	index  int
	data   dict.Dict
	addAof func(CmdLine)
}

// ExecFunc 是Exec的接口
// args不包括cmd line
type ExecFunc func(db *DB, args [][]byte) resp.Reply

// CmdLine [][]byte, 代表命令行
type CmdLine = [][]byte

// makeDB 创建DB数据库
func makeDB() *DB {
	db := &DB{
		data:   dict.MakeSyncDict(),
		addAof: func(line CmdLine) {}, //防止回复数据的时候有错误
	}
	return db
}

// Exec 在一个db内执行命令
func (db *DB) Exec(c resp.Connection, cmdLine [][]byte) resp.Reply {
	//PING SET SETNX
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return reply.MakeErrReply("ERR unknown command '" + cmdName + "'")
	}
	// SET k
	if !validateArity(cmd.arity, cmdLine) {
		return reply.MakeArgNumErrReply(cmdName)
	}
	fun := cmd.executor
	//SET K V -> K V
	return fun(db, cmdLine[1:]) //调用具体的实现方法
}

//validateArity 校验参数个数是否合法
// SET K V -> 参数长度arity = 3
// EXISTS k1 k2 k3 k4... -> 参数长度-arity = -2; -2是负数最小SET K：2
func validateArity(arity int, cmdArgs [][]byte) bool {
	argNum := len(cmdArgs)
	if arity >= 0 { // SET K V
		return argNum == arity
	}
	// EXISTS k1 k2 k3 k4...
	return argNum >= -arity
}

/* ---- data Access ----- */

// GetEntity 在DB的层面根据 KEY 获取到 value; GET指令的内部调用该方法
func (db *DB) GetEntity(key string) (*database.DataEntity, bool) {

	raw, ok := db.data.Get(key)
	if !ok {
		return nil, false
	}
	entity, _ := raw.(*database.DataEntity)
	return entity, true
}

// PutEntity 把DataEntity 存入到 DB
func (db *DB) PutEntity(key string, entity *database.DataEntity) int {
	return db.data.Put(key, entity)
}

// PutIfExists  如果存在当前的Key, Put 现有的 DataEntity
func (db *DB) PutIfExists(key string, entity *database.DataEntity) int {
	return db.data.PutIfExists(key, entity)
}

// PutIfAbsent 仅在 key 不存在时插入 DataEntity
func (db *DB) PutIfAbsent(key string, entity *database.DataEntity) int {
	return db.data.PutIfAbsent(key, entity)
}

// Remove 从数据库中移除指定 key
func (db *DB) Remove(key string) {
	db.data.Remove(key)
}

// Removes 从数据库中移除指定的多个 key
func (db *DB) Removes(keys ...string) (deleted int) {
	deleted = 0
	for _, key := range keys {
		_, exists := db.data.Get(key)
		if exists {
			db.Remove(key)
			deleted++
		}
	}
	return deleted
}

// Flush 清空数据库
func (db *DB) Flush() {
	db.data.Clear()
}
