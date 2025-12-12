# Redis Proxy

一个用Vibe coding，Rust编写的高性能 Redis 代理服务器，支持读写分离、连接池管理和命令执行统计。

## 功能特性

- **完整的 Redis 协议支持**: 实现了 RESP (Redis Serialization Protocol) 协议解析
- **读写分离**: 自动将读命令路由到从节点，写命令路由到主节点
- **连接池管理**: 为主从节点维护连接池，提高性能
- **负载均衡**: 使用轮询算法在多个从节点间分配读请求
- **故障转移**: 从节点不可用时自动回退到主节点
- **统计信息**: 详细的命令执行统计，包括执行次数、耗时、错误率等
- **异步处理**: 基于 Tokio 的异步 I/O，支持高并发

## 快速开始

### 构建

```bash
cargo build --release
```

### 配置

创建配置文件 `config.toml`：

```toml
[proxy]
bind_addr = "127.0.0.1:6380"
pool_size = 10
log_level = "info"

[redis]
master = "127.0.0.1:6379"
slaves = [
    "127.0.0.1:6380",
    "127.0.0.1:6381",
]
```

或者使用命令生成默认配置：

```bash
./target/release/redis-proxy --generate-config
```

### 运行

```bash
# 使用默认配置文件 config.toml
./target/release/redis-proxy

# 指定配置文件
./target/release/redis-proxy /path/to/config.toml
```

### 连接代理

使用任何 Redis 客户端连接到代理：

```bash
redis-cli -h 127.0.0.1 -p 6380
```

## 命令分类

代理会自动识别命令类型并路由到相应的节点：

### 读命令（路由到从节点）
- 字符串: GET, MGET, STRLEN, GETRANGE, GETBIT
- 哈希: HGET, HMGET, HGETALL, HKEYS, HVALS, HLEN
- 列表: LLEN, LINDEX, LRANGE, LPOS
- 集合: SCARD, SISMEMBER, SMEMBERS, SRANDMEMBER
- 有序集合: ZCARD, ZCOUNT, ZSCORE, ZRANK, ZRANGE
- 键操作: EXISTS, TYPE, TTL, KEYS, SCAN
- 信息命令: INFO, PING, ECHO

### 写命令（路由到主节点）
- 字符串: SET, SETEX, INCR, DECR, APPEND
- 哈希: HSET, HDEL, HINCRBY
- 列表: LPUSH, RPUSH, LPOP, RPOP, LSET
- 集合: SADD, SREM, SPOP
- 有序集合: ZADD, ZREM, ZINCRBY
- 键操作: DEL, EXPIRE, RENAME
- 事务: MULTI, EXEC, WATCH
- 发布订阅: PUBLISH, SUBSCRIBE

## 统计信息

代理提供了特殊的 `PROXY` 命令来查看统计信息：

```bash
# 查看详细统计
redis-cli -h 127.0.0.1 -p 6380 PROXY STATS

# 查看代理信息
redis-cli -h 127.0.0.1 -p 6380 PROXY INFO

# 重置统计信息
redis-cli -h 127.0.0.1 -p 6380 PROXY RESET
```

统计信息包括：
- 总命令数、读命令数、写命令数
- 错误总数
- 连接数统计
- 每个命令的执行次数、平均耗时、最小/最大耗时
- 运行时间

## 开发

### 运行测试

```bash
cargo test
```

### 运行单个测试

```bash
cargo test test_name
```

### 启用调试日志

```bash
RUST_LOG=debug ./target/release/redis-proxy
```

## 架构

项目结构：

- `src/protocol.rs`: RESP 协议解析器
- `src/pool.rs`: 连接池和集群管理
- `src/command.rs`: 命令解析和分类
- `src/stats.rs`: 统计信息收集
- `src/proxy.rs`: 代理请求处理
- `src/config.rs`: 配置文件管理
- `src/main.rs`: 主程序入口

## 性能优化

- 使用异步 I/O 处理并发连接
- 连接池复用减少连接开销
- 零拷贝的协议解析
- 无锁的统计信息收集（使用原子操作）

## 许可证

MIT
