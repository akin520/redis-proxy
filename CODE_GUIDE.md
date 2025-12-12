# Redis 代理代码详解

这个文档帮助你理解 Redis 代理的代码结构和工作原理。

## 📁 项目结构

```
src/
├── main.rs          # 程序入口，启动服务器
├── protocol.rs      # Redis 协议解析器（RESP）
├── pool.rs          # 连接池管理
├── command.rs       # 命令分类（读/写）
├── stats.rs         # 统计信息收集
├── proxy.rs         # 请求处理和路由
└── config.rs        # 配置文件管理
```

## 🔄 请求处理流程

```
客户端
  ↓
[1] main.rs: 接受 TCP 连接
  ↓
[2] proxy.rs: 读取客户端数据
  ↓
[3] protocol.rs: 解析 RESP 协议
  ↓
[4] command.rs: 判断命令类型（读/写）
  ↓
[5] pool.rs: 获取连接（主节点/从节点）
  ↓
[6] pool.rs: 发送命令到 Redis
  ↓
[7] protocol.rs: 解析 Redis 响应
  ↓
[8] proxy.rs: 返回响应给客户端
  ↓
[9] stats.rs: 记录统计信息
```

## 📝 核心模块详解

### 1. protocol.rs - Redis 协议解析器

**作用**：解析和编码 Redis 的 RESP 协议

**核心类型**：
- `RespValue`: 表示 Redis 的 5 种数据类型
  - SimpleString: `+OK\r\n`
  - Error: `-ERR message\r\n`
  - Integer: `:1000\r\n`
  - BulkString: `$5\r\nhello\r\n`
  - Array: `*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n`

**关键方法**：
- `parse()`: 从字节流解析 RESP 值
- `encode()`: 将 RESP 值编码为字节流
- `parse_inline_command()`: 支持 redis-benchmark 的内联命令格式

### 2. pool.rs - 连接池管理

**作用**：管理到 Redis 的 TCP 连接，实现连接复用

**核心类型**：
- `RedisConnection`: 单个 TCP 连接
- `ConnectionPool`: 连接池（使用 Semaphore 控制并发）
- `PooledConnection`: 智能指针，自动归还连接
- `RedisCluster`: 管理主从集群

**关键机制**：
```rust
// 获取连接的流程：
1. 获取 Semaphore 许可（限制最大连接数）
2. 从池中取出空闲连接（如果有）
3. 如果池为空，创建新连接
4. 返回 PooledConnection（包装了连接和许可）

// 归还连接的流程（自动）：
1. PooledConnection 被 drop
2. 连接放回池中
3. Semaphore 许可自动释放
4. 唤醒等待的请求
```

**性能优化**：
- TCP_NODELAY: 禁用 Nagle 算法，降低延迟
- 连接复用: 避免 TCP 三次握手开销
- Semaphore: 公平调度，防止连接池耗尽

### 3. command.rs - 命令分类

**作用**：判断 Redis 命令是读操作还是写操作

**核心类型**：
- `CommandType`: Read（读）或 Write（写）
- `Command`: 包含命令名、类型和参数

**分类规则**：
- **读命令** → 路由到从节点
  - GET, MGET, HGET, LRANGE, SMEMBERS, ZRANGE
  - INFO, PING, KEYS, SCAN

- **写命令** → 路由到主节点
  - SET, DEL, INCR, LPUSH, SADD, ZADD
  - MULTI, EXEC, PUBLISH

- **未知命令** → 默认为写命令（安全起见）

### 4. stats.rs - 统计信息

**作用**：收集和展示命令执行统计

**统计指标**：
- 每个命令的执行次数
- 平均/最小/最大执行时间
- 错误次数
- 总命令数（读/写分开统计）
- 连接数统计

**实现方式**：
- 使用 `AtomicU64` 实现无锁计数
- 使用 `DashMap` 存储每个命令的统计信息
- 使用 `RwLock` 保护单个命令的统计数据

### 5. proxy.rs - 请求处理

**作用**：处理客户端连接，路由请求到 Redis

**核心流程**：
```rust
1. 接受客户端连接
2. 循环读取客户端数据
3. 解析 RESP 命令
4. 判断命令类型（读/写）
5. 获取相应的连接（主/从）
6. 发送命令到 Redis
7. 返回响应给客户端
8. 记录统计信息
```

**错误处理**：
- 区分正常断开和异常错误
- ConnectionReset → debug 级别（正常）
- 其他错误 → error 级别（需要关注）

**特殊命令**：
- `PROXY STATS`: 查看统计信息
- `PROXY INFO`: 查看代理配置
- `PROXY RESET`: 重置统计信息

### 6. config.rs - 配置管理

**作用**：加载和验证配置文件

**配置项**：
```toml
[proxy]
bind_addr = "127.0.0.1:6380"  # 代理监听地址
pool_size = 50                 # 每个节点的连接池大小
log_level = "info"             # 日志级别

[redis]
master = "127.0.0.1:6379"      # 主节点地址
slaves = [                     # 从节点地址列表
    "127.0.0.1:6380",
]
```

### 7. main.rs - 程序入口

**作用**：启动代理服务器

**启动流程**：
```rust
1. 加载配置文件
2. 初始化日志系统
3. 创建 RedisCluster（连接池）
4. 创建 Statistics（统计）
5. 创建 ProxyHandler（请求处理器）
6. 监听 TCP 端口
7. 为每个连接创建异步任务
8. 等待 Ctrl+C 信号退出
```

## 🎯 关键设计模式

### 1. 连接池模式
```rust
// 使用 RAII 自动管理连接生命周期
let mut conn = pool.get_connection().await?;
conn.send_command(&cmd).await?;
// conn 被 drop，自动归还到池中
```

### 2. 读写分离
```rust
if command.is_read() {
    // 从节点（负载均衡）
    cluster.get_slave_connection().await?
} else {
    // 主节点
    cluster.get_master_connection().await?
}
```

### 3. 异步并发
```rust
// 每个客户端连接都是一个独立的异步任务
tokio::spawn(async move {
    handler.handle_client(stream, addr).await;
});
```

## 🔧 性能优化技巧

### 1. 连接复用
- **问题**：每次请求创建新连接，TCP 三次握手开销大
- **解决**：连接池复用，性能提升 8-10 倍

### 2. TCP_NODELAY
- **问题**：Nagle 算法合并小包，增加延迟
- **解决**：禁用 Nagle，立即发送数据

### 3. 信号量控制
- **问题**：无限创建连接导致资源耗尽
- **解决**：Semaphore 限制最大连接数，公平排队

### 4. 无锁统计
- **问题**：锁竞争影响性能
- **解决**：AtomicU64 + DashMap 实现无锁统计

## 🐛 常见问题

### Q1: 为什么会出现 "connection pool exhausted"？
**A**: 并发请求超过 pool_size，所有连接都在使用中
**解决**: 增加 pool_size 或优化 Redis 查询性能

### Q2: 为什么性能比原生 Redis 慢？
**A**: 可能原因：
1. 连接池未启用（每次创建新连接）
2. pool_size 太小
3. 网络延迟（代理增加一跳）

### Q3: 如何调试连接问题？
**A**: 设置 `log_level = "debug"` 查看详细日志

## 📚 学习建议

1. **先理解流程**：从 main.rs 开始，跟踪一个请求的完整流程
2. **再看细节**：深入每个模块，理解实现原理
3. **动手实验**：修改代码，观察行为变化
4. **性能测试**：使用 redis-benchmark 测试性能

## 🎓 Rust 知识点

这个项目涉及的 Rust 特性：
- **异步编程**: async/await, tokio
- **所有权**: Arc, Mutex, 生命周期
- **错误处理**: Result, anyhow
- **并发**: Semaphore, 原子操作
- **模式匹配**: match, if let
- **trait**: Drop, Clone, Debug

希望这个文档能帮助你理解代码！🎉
