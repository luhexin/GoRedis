package resp

// Connection 接口：Connection可能会有不同的实现，和持久化有关
type Connection interface {
	Write([]byte) error
	GetDBIndex() int
	SelectDB(int)
}
