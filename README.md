# GoRedis
使用Go重写Redis中间件
- [x] 实现了Redis协议解析器
    - [x] 在 TCP Server的基础上，实现Redis的通信协议(RESP协议)
- [x] 实现内存数据库与Redis持久化
    - [x] 实现STRING、KEYS命令集
    - [x] 实现Aof落盘功能、使用了GO的文件IO特性
- [x] 实现Redis集群
    - [x] 实现一致性哈希、连接工厂