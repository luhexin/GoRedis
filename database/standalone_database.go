package database

/*调用底层业务的 db*/
import (
	"GoRedis/aof"
	"GoRedis/config"
	"GoRedis/interface/resp"
	"GoRedis/lib/logger"
	"GoRedis/resp/reply"
	"errors"
	"fmt"
	"runtime/debug"
	"strconv"
	"strings"
)

// StandaloneDatabase 一组分数据库
type StandaloneDatabase struct {
	dbSet      []*DB
	aofHandler *aof.AofHandler
}

// NewStandaloneDatabase 新建一个 redis 内核
func NewStandaloneDatabase() *StandaloneDatabase {
	mdb := &StandaloneDatabase{}
	if config.Properties.Databases == 0 { //读取配置文件
		config.Properties.Databases = 16
	}
	mdb.dbSet = make([]*DB, config.Properties.Databases)
	for i := range mdb.dbSet { // 填充 StandaloneDatabase 结构体中的DB数组，每一个DB的底层都是Sync.map
		singleDB := makeDB()
		singleDB.index = i
		mdb.dbSet[i] = singleDB
	}
	if config.Properties.AppendOnly {
		aofHandler, err := aof.NewAOFHandler(mdb)
		if err != nil {
			panic(err)
		}
		mdb.aofHandler = aofHandler
		for _, db := range mdb.dbSet {
			//dp是指针数组, 会发生内存逃逸; 防止传入mdb.aofHandler.AddAof的db因为后序遍历更改
			singleDB := db
			//初始化addAof方法
			singleDB.addAof = func(line CmdLine) {
				mdb.aofHandler.AddAof(singleDB.index, line)
			}
		}
	}
	return mdb
}

// Exec 执行命令
// 将用户指令转交给分DB执行
func (mdb *StandaloneDatabase) Exec(c resp.Connection, cmdLine [][]byte) (result resp.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
		}
	}()

	cmdName := strings.ToLower(string(cmdLine[0]))
	if cmdName == "select" { // 选择db的指令，select 1：选择第一个分db
		if len(cmdLine) != 2 {
			return reply.MakeArgNumErrReply("select")
		}
		return execSelect(c, mdb, cmdLine[1:])
	}
	// 操作db的指令：set k v; get k
	dbIndex := c.GetDBIndex()
	selectedDB := mdb.dbSet[dbIndex]
	return selectedDB.Exec(c, cmdLine)
}

// Close 关闭数据库
func (mdb *StandaloneDatabase) Close() {

}

func (mdb *StandaloneDatabase) AfterClientClose(c resp.Connection) {
}

// 提供用户选择DB的功能
// 通过用户发送的指令args, 修改resp.Connection字段
// select 1
func execSelect(c resp.Connection, mdb *StandaloneDatabase, args [][]byte) resp.Reply {
	dbIndex, err := strconv.Atoi(string(args[0]))
	if !errors.Is(err, nil) {
		return reply.MakeErrReply("ERR invalid DB index")
	}
	if dbIndex >= len(mdb.dbSet) {
		return reply.MakeErrReply("ERR DB index is out of range")
	}
	c.SelectDB(dbIndex)
	return reply.MakeOkReply()
}
