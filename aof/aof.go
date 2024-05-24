package aof

import (
	"GoRedis/config"
	databaseface "GoRedis/interface/database"
	"GoRedis/lib/logger"
	"GoRedis/lib/utils"
	"GoRedis/resp/connection"
	"GoRedis/resp/parser"
	"GoRedis/resp/reply"
	"io"
	"os"
	"strconv"
)

type CmdLine = [][]byte

const (
	aofQueueSize = 1 << 16
)

type payload struct {
	cmdLine CmdLine
	dbIndex int
}

// AofHandler receive msgs from channel and write to AOF file
type AofHandler struct {
	db          databaseface.Database
	aofChan     chan *payload //缓存区
	aofFile     *os.File
	aofFilename string
	currentDB   int //记录上一条指令写入的分数据库
}

func NewAOFHandler(db databaseface.Database) (*AofHandler, error) {
	handler := &AofHandler{}
	handler.aofFilename = config.Properties.AppendFilename
	handler.db = db
	handler.LoadAof()
	// 从头到尾到会用到，所以不需要关闭文件流
	aofFile, err := os.OpenFile(handler.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	handler.aofFile = aofFile
	handler.aofChan = make(chan *payload, aofQueueSize)
	go func() {
		handler.handleAof() // 从channel中取
	}()
	return handler, nil
}

// AddAof payload(set k v) -> aofchan
func (handler *AofHandler) AddAof(dbIndex int, cmdLine CmdLine) {
	if config.Properties.AppendOnly && handler.aofChan != nil {
		handler.aofChan <- &payload{
			cmdLine: cmdLine,
			dbIndex: dbIndex,
		}
	}
}

// handleAof payload(set k v) <- aofchan (落盘)
func (handler *AofHandler) handleAof() {
	handler.currentDB = 0
	for p := range handler.aofChan { // 落到.aof文件中
		if p.dbIndex != handler.currentDB {
			// select db
			data := reply.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(p.dbIndex))).ToBytes()
			_, err := handler.aofFile.Write(data)
			if err != nil {
				logger.Warn(err)
				continue
			}
			handler.currentDB = p.dbIndex
		}
		data := reply.MakeMultiBulkReply(p.cmdLine).ToBytes()
		_, err := handler.aofFile.Write(data)
		if err != nil {
			logger.Warn(err)
		}
	}
}

// LoadAof 读取 aof 文件, 重写aof文件中的内容
func (handler *AofHandler) LoadAof() {
	//1. 以只读的方式Open, 打开文件
	file, err := os.Open(handler.aofFilename)
	if err != nil {
		logger.Warn(err)
		return
	}
	defer file.Close()
	//2. 调用parser解析指令
	ch := parser.ParseStream(file)
	fakeConn := &connection.Connection{} // 为了记录selectDB
	//3. 读取channel
	for p := range ch {
		// 4.1 管道读取问题
		if p.Err != nil {
			if p.Err == io.EOF {
				break
			}
			logger.Error("parse error: " + p.Err.Error())
			continue
		}
		if p.Data == nil {
			logger.Error("empty payload")
			continue
		}

		r, ok := p.Data.(*reply.MultiBulkReply)
		if !ok {
			logger.Error("require multi bulk reply")
			continue
		}
		// 4.2 正常读取
		ret := handler.db.Exec(fakeConn, r.Args)
		if reply.IsErrorReply(ret) { // 记录执行的错误
			logger.Error("exec err", err)
		}
	}
}
