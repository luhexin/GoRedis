package parser

import (
	"GoRedis/interface/resp"
	"GoRedis/lib/logger"
	"GoRedis/resp/reply"
	"bufio"
	"errors"
	"io"
	"runtime/debug"
	"strconv"
	"strings"
)

// Payload stores redis.Reply or error
type Payload struct {
	//
	Data resp.Reply //客户端和服务端交互的数据都可以认为是reply
	Err  error
}

// ParseStream reads data from io.Reader and send payloads through channel
// 解析器调用入口
func ParseStream(reader io.Reader) <-chan *Payload {
	ch := make(chan *Payload)
	go parse0(reader, ch) // 异步解析
	return ch
}

type readState struct { //解析器状态
	readingMultiLine  bool //单行还是多行数据
	expectedArgsCount int
	msgType           byte
	args              [][]byte
	bulkLen           int64
}

func (s *readState) finished() bool {
	return s.expectedArgsCount > 0 && len(s.args) == s.expectedArgsCount
}

// 为了支持异步，解析的结果塞入管道
func parse0(reader io.Reader, ch chan<- *Payload) {
	//如果死循环中出现了panic, 会终止当前 goroutine 的执行；
	//防止带崩整个协程，recover 捕获 goroutine 中发生的 panic 并恢复正常执行流
	defer func() {
		if err := recover(); err != nil { //如果出现错误
			logger.Error(string(debug.Stack()))
		}
	}()
	bufReader := bufio.NewReader(reader)
	var state readState
	var err error
	var msg []byte
	for true { //用户连接之后，进入死循环，不断地读取解析用户发送的信息; 用户断开，跳出死循环
		// *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
		// 1. read line：取出一行指令, msg = *3
		var ioErr bool
		msg, ioErr, err = readLine(bufReader, &state)
		if err != nil {
			if ioErr { // 发生io错误，停止解析
				ch <- &Payload{
					Err: err,
				}
				close(ch)
				return
			}
			// 协议错误，清空状态，继续解析
			ch <- &Payload{
				Err: err,
			}
			state = readState{}
			continue
		}

		// 2. 判断是否是多行解析
		if !state.readingMultiLine {
			// 此时说明没有初始化，没有打开多行解析模式;
			if msg[0] == '*' { //没有开始解析
				err = parseMultiBulkHeader(msg, &state)
				if err != nil {
					ch <- &Payload{
						Err: errors.New("protocol error: " + string(msg)),
					}
					state = readState{}
					continue
				}
				if state.expectedArgsCount == 0 {
					ch <- &Payload{
						Data: &reply.EmptyMultiBulkReply{},
					}
					state = readState{}
					continue
				}
			} else if msg[0] == '$' { //$3\r\nSET\r\: 单行没有开始解析
				err = parseBulkHeader(msg, &state)
				if err != nil {
					ch <- &Payload{
						Err: errors.New("protocol error: " + string(msg)),
					}
					state = readState{} // reset state
					continue
				}
				if state.bulkLen == -1 { // null bulk reply
					ch <- &Payload{
						Data: &reply.NullBulkReply{},
					}
					state = readState{} // reset state
					continue
				}
			} else { // +OK\r\n: 就是一个单行的reply
				result, err := parseSingleLineReply(msg)
				ch <- &Payload{
					Data: result,
					Err:  err,
				}
				state = readState{} // reset state
				continue
			}
		} else {
			// *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
			// parseMultiBulkHeader、parseBulkHeader：state.readingMultiLine -> true
			// *3\r\n：已经在上面解析了; $3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n：未解析
			err = readBody(msg, &state) // 读取的数据放到state.args, 反复读取解析指令
			if err != nil {
				ch <- &Payload{
					Err: errors.New("protocol error: " + string(msg)),
				}
				state = readState{} // reset state
				continue
			}
			// 当前解析已完成
			if state.finished() {
				var result resp.Reply
				if state.msgType == '*' {
					result = reply.MakeMultiBulkReply(state.args)
				} else if state.msgType == '$' {
					result = reply.MakeBulkReply(state.args[0])
				}
				ch <- &Payload{
					Data: result,
					Err:  err,
				}
				state = readState{}
			}
		}
	}
}

// 切割指令($3\r\n; key\r\n), 字符串中的实际内容可能含有\r\n，所以不能简单的按照\r\n切分
// *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
func readLine(bufReader *bufio.Reader, state *readState) ([]byte, bool, error) {
	// 1. \r\n切分
	// 2. 之前读取了$, 严格读取字符个数
	var msg []byte
	var err error
	if state.bulkLen == 0 { // 之前没有读到$数字: \r\n切分
		msg, err = bufReader.ReadBytes('\n') //msg: $3\r\n
		if err != nil {
			return nil, true, err
		}
		if len(msg) == 0 || msg[len(msg)-2] != '\r' { //格式错误
			return nil, false, errors.New("protocol error: " + string(msg))
		}
	} else { // 2. 之前读取了$, 严格读取字符个数
		// 如果读取到$后的数字是3, state.bulkLen = 3
		msg = make([]byte, state.bulkLen+2)
		_, err = io.ReadFull(bufReader, msg) //塞满msg, msg : key\r\n
		if err != nil {
			return nil, true, err
		}
		if len(msg) == 0 ||
			msg[len(msg)-2] != '\r' ||
			msg[len(msg)-1] != '\n' { //格式错误
			return nil, false, errors.New("protocol error: " + string(msg))
		}
		state.bulkLen = 0
	}
	return msg, false, nil
}

// *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
// 解析字符串头*3\r\n, 并修改解析器readState 状态
func parseMultiBulkHeader(msg []byte, state *readState) error {
	var err error
	var expectedLine uint64
	expectedLine, err = strconv.ParseUint(string(msg[1:len(msg)-2]), 10, 32)
	if err != nil {
		return errors.New("protocol error: " + string(msg))
	}
	if expectedLine == 0 {
		state.expectedArgsCount = 0
		return nil
	} else if expectedLine > 0 {
		state.msgType = msg[0]                      //*
		state.readingMultiLine = true               //多行状态
		state.expectedArgsCount = int(expectedLine) //3
		state.args = make([][]byte, 0, expectedLine)
		return nil
	} else {
		return errors.New("protocol error: " + string(msg))
	}
}

// $3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
// 解析$3\r\n
func parseBulkHeader(msg []byte, state *readState) error {
	var err error
	state.bulkLen, err = strconv.ParseInt(string(msg[1:len(msg)-2]), 10, 64)
	if err != nil {
		return errors.New("protocol error: " + string(msg))
	}
	if state.bulkLen == -1 { // null bulk
		return nil
	} else if state.bulkLen > 0 {
		state.msgType = msg[0]
		state.readingMultiLine = true
		state.expectedArgsCount = 1
		state.args = make([][]byte, 0, 1)
		return nil
	} else {
		return errors.New("protocol error: " + string(msg))
	}
}

//+OK\r\n; -err\r\n; :5\r\n
func parseSingleLineReply(msg []byte) (resp.Reply, error) {
	str := strings.TrimSuffix(string(msg), "\r\n")
	var result resp.Reply
	switch msg[0] {
	case '+': // +OK\r\n
		result = reply.MakeStatusReply(str[1:])
	case '-': // -err\r\n
		result = reply.MakeErrReply(str[1:])
	case ':': // :5\r\n
		val, err := strconv.ParseInt(str[1:], 10, 64)
		if err != nil {
			return nil, errors.New("protocol error: " + string(msg))
		}
		result = reply.MakeIntReply(val)
	}
	return result, nil
}

// $3\r\n  SET\r\n  $3\r\n  key\r\n  $5\r\n  value\r\n
// 解析内容
func readBody(msg []byte, state *readState) error {
	line := msg[0 : len(msg)-2]
	var err error
	if line[0] == '$' {
		// $3\r\n 取出3，并塞入state.bulkLen
		state.bulkLen, err = strconv.ParseInt(string(line[1:]), 10, 64)
		if err != nil {
			return errors.New("protocol error: " + string(msg))
		}
		// $0\r\n
		if state.bulkLen <= 0 {
			state.args = append(state.args, []byte{})
			state.bulkLen = 0
		}
	} else { // SET\r\n
		state.args = append(state.args, line)
	}
	return nil
}
