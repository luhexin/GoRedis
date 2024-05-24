package database

import (
	"strings"
)

// 记录系统里所有指令与command之间的关系，比如每一个get、set指令对应一个command
var cmdTable = make(map[string]*command)

type command struct {
	executor ExecFunc
	arity    int // 参数数量
}

// RegisterCommand 注册指令(记录指令与command之间的关系)
func RegisterCommand(name string, executor ExecFunc, arity int) {
	name = strings.ToLower(name)
	cmdTable[name] = &command{
		executor: executor,
		arity:    arity,
	}
}
