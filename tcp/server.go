package tcp

import (
	"GoRedis/interface/tcp"
	"GoRedis/lib/logger"
	"context"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

type Config struct {
	Address string
}

func ListenAndServeWithSignal(cfg *Config, handler tcp.Handler) error {

	closeChan := make(chan struct{})
	sigChan := make(chan os.Signal)
	signal.Notify(sigChan, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigChan
		switch sig {
		case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			closeChan <- struct{}{}
		}
	}()

	listener, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		return err
	}
	logger.Info("start listen")
	ListenAndServe(listener, handler, closeChan)
	return nil
}

func ListenAndServe(listner net.Listener, handler tcp.Handler, closeChan <-chan struct{}) {

	// 当系统给进程发送关闭信号时，通过channel通知方法
	go func() {
		<-closeChan //没有信号则处于阻塞状态
		logger.Info("shutting down")
		_ = listner.Close()
		_ = handler.Close()
	}()

	defer func() {
		_ = listner.Close()
		_ = handler.Close()
	}()
	ctx := context.Background()

	var waitDone sync.WaitGroup //等待所有客户端退出
	for true {
		conn, err := listner.Accept()
		if err != nil {
			break
		}
		logger.Info("accepted link")
		waitDone.Add(1)
		go func() { //一个协程服务一个连接
			defer func() {
				waitDone.Done()
			}()
			handler.Handle(ctx, conn)
		}()
	}
	// 如果连接错误会跳出循环，防止还在运行的协程终止
	waitDone.Wait()
}
