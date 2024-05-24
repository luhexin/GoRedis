package database

import (
	"GoRedis/interface/database"
	"GoRedis/interface/resp"
	"GoRedis/lib/utils"
	"GoRedis/resp/reply"
)

func (db *DB) getAsString(key string) ([]byte, reply.ErrorReply) {
	entity, ok := db.GetEntity(key)
	if !ok {
		return nil, nil
	}
	bytes, ok := entity.Data.([]byte)
	if !ok {
		return nil, &reply.WrongTypeErrReply{}
	}
	return bytes, nil
}

// execGet GET k1: 返回 key 对应的 value
func execGet(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	bytes, err := db.getAsString(key)
	if err != nil {
		return err
	}
	if bytes == nil {
		return &reply.NullBulkReply{}
	}
	return reply.MakeBulkReply(bytes)
}

// execSet 为给定key设置string value和ttl
func execSet(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	value := args[1]
	entity := &database.DataEntity{
		Data: value,
	}
	db.PutEntity(key, entity)
	db.addAof(utils.ToCmdLine2("set", args...)) //添加到aof文件
	return &reply.OkReply{}
}

// execSetNX 设置string if Not Exists
func execSetNX(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	value := args[1]
	entity := &database.DataEntity{
		Data: value,
	}
	result := db.PutIfAbsent(key, entity)
	db.addAof(utils.ToCmdLine2("setnx", args...))
	return reply.MakeIntReply(int64(result))
}

// execGetSet 先获取原先的值，然后设置新的值; 返回原来的值
func execGetSet(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	value := args[1]

	entity, exists := db.GetEntity(key)
	db.PutEntity(key, &database.DataEntity{Data: value})
	db.addAof(utils.ToCmdLine2("getset", args...))
	if !exists {
		return reply.MakeNullBulkReply()
	}
	old := entity.Data.([]byte)
	return reply.MakeBulkReply(old)
}

// execStrLen 返回与给定key绑定的string的长度
func execStrLen(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	entity, exists := db.GetEntity(key)
	if !exists {
		return reply.MakeNullBulkReply()
	}
	old := entity.Data.([]byte)
	return reply.MakeIntReply(int64(len(old)))
}

func init() {
	RegisterCommand("Get", execGet, 2)
	RegisterCommand("Set", execSet, -3)
	RegisterCommand("SetNx", execSetNX, 3)
	RegisterCommand("GetSet", execGetSet, 3)
	RegisterCommand("StrLen", execStrLen, 2)
}
