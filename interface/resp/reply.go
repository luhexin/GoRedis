package resp

//服务端对客户端的回复
type Reply interface {
	ToBytes() []byte //TCP是面向字节流的，所以要转化为字节
}
