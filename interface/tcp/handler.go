package tcp

import (
	"context"
	"net"
)

type Handler interface { //具体的业务由Handler完成
	Handle(ctx context.Context, conn net.Conn)
	Close() error
}
