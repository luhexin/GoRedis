package reply

/*回复常量*/

type PongReply struct {
}

//客户端发ping，回复pong
var pongBytes = []byte("+PONG\r\n")

func (r PongReply) ToBytes() []byte {
	return pongBytes
}
func MakePongReply() *PongReply { //对外提供接口
	return &PongReply{}
}

// OkReply ok回复
type OkReply struct {
}

var okBytes = []byte("+OK\r\n")

func (o OkReply) ToBytes() []byte {
	return okBytes
}

var theOkReply = new(OkReply)

func MakeOkReply() *OkReply {
	return theOkReply
}

// NullBulkReply 空回复
type NullBulkReply struct {
}

var nullBulkBytes = []byte("$-1\r\n")

func (n NullBulkReply) ToBytes() []byte {
	return nullBulkBytes
}

var theNullBulkBytes = new(NullBulkReply)

func MakeNullBulkBytes() *NullBulkReply {
	return theNullBulkBytes
}

// EmptyMultiBulkReply 空数组
type EmptyMultiBulkReply struct {
}

var emptyMultiBulkBytes = []byte("*0\r\n")

func (e EmptyMultiBulkReply) ToBytes() []byte {
	return emptyMultiBulkBytes
}

var theEmptyMultiBulkBytes = new(EmptyMultiBulkReply)

func MakeEmptyMultiBulkBytes() *EmptyMultiBulkReply {
	return theEmptyMultiBulkBytes
}

// NoReply 回复空
type NoReply struct {
}

var noBytes = []byte("")

func (n NoReply) ToBytes() []byte {
	return noBytes
}

var theNoBytes = new(NoReply)

func MakeNoBytes() *NoReply {
	return theNoBytes
}
