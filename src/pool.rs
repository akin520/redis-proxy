// ============================================================================
// Redis 连接池管理模块
// ============================================================================
// 这个模块负责管理到 Redis 服务器的 TCP 连接
// 主要功能：
// 1. 连接复用：避免每次请求都创建新连接
// 2. 并发控制：使用信号量限制最大连接数
// 3. 集群支持：管理主从节点的连接池

use anyhow::{anyhow, Result};
use bytes::BytesMut;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, Semaphore};
use tracing::{debug, error, warn};

use crate::protocol::{RespParser, RespValue};

// ============================================================================
// RedisConnection 结构体：单个 Redis 连接
// ============================================================================
// 封装了一个 TCP 连接和读取缓冲区
pub struct RedisConnection {
    stream: TcpStream,      // TCP 连接流
    buffer: BytesMut,       // 读取缓冲区，用于存储从 Redis 接收的数据
}

impl RedisConnection {
    // ========================================================================
    // new: 创建一个新的 Redis 连接
    // ========================================================================
    // 参数：
    //   - addr: Redis 服务器地址，格式为 "host:port"
    // 返回：
    //   - Ok(RedisConnection): 成功创建的连接
    //   - Err: 连接失败的错误
    pub async fn new(addr: &str) -> Result<Self> {
        // 建立 TCP 连接
        let stream = TcpStream::connect(addr).await?;

        // 禁用 Nagle 算法以降低延迟
        // Nagle 算法会将小数据包合并后再发送，这会增加延迟
        // 对于 Redis 这种请求-响应模式，我们希望立即发送数据
        stream.set_nodelay(true)?;

        Ok(Self {
            stream,
            // 预分配 8KB 的缓冲区，减少内存分配次数
            buffer: BytesMut::with_capacity(8192),
        })
    }

    // ========================================================================
    // send_command: 发送命令并接收响应
    // ========================================================================
    // 这是连接的核心方法，负责：
    // 1. 将命令编码为 RESP 格式
    // 2. 通过 TCP 发送到 Redis
    // 3. 读取并解析 Redis 的响应
    pub async fn send_command(&mut self, cmd: &RespValue) -> Result<RespValue> {
        // 将命令编码为 RESP 协议格式的字节数组
        let encoded = cmd.encode();

        // 将数据写入 TCP 流
        // write_all 会确保所有数据都被发送
        self.stream.write_all(&encoded).await?;

        // 循环读取响应，直到解析出完整的 RESP 值
        loop {
            // 尝试从缓冲区解析响应
            if let Some(response) = RespParser::parse(&mut self.buffer)? {
                return Ok(response);
            }

            // 如果缓冲区数据不完整，继续从 TCP 流读取更多数据
            // read_buf 会自动扩展缓冲区
            if self.stream.read_buf(&mut self.buffer).await? == 0 {
                // 读取到 0 字节表示连接已关闭
                return Err(anyhow!("connection closed by server"));
            }
        }
    }
}

// ============================================================================
// ConnectionPool 结构体：连接池
// ============================================================================
// 管理到单个 Redis 节点的连接池
// 核心机制：
// 1. 使用 Semaphore 控制最大连接数
// 2. 使用 Mutex 保护连接池（Vec）
// 3. 连接使用完毕后自动归还到池中
pub struct ConnectionPool {
    addr: String,                               // Redis 服务器地址
    pool: Arc<Mutex<Vec<RedisConnection>>>,     // 连接池（空闲连接列表）
    semaphore: Arc<Semaphore>,                  // 信号量，控制最大连接数
}

impl ConnectionPool {
    // ========================================================================
    // new: 创建一个新的连接池
    // ========================================================================
    // 参数：
    //   - addr: Redis 服务器地址
    //   - max_size: 最大连接数（连接池容量）
    pub fn new(addr: String, max_size: usize) -> Self {
        Self {
            addr,
            // 创建空的连接池，预分配容量
            pool: Arc::new(Mutex::new(Vec::with_capacity(max_size))),
            // 创建信号量，初始值为 max_size
            // 每获取一个连接就消耗一个许可，归还连接时释放许可
            semaphore: Arc::new(Semaphore::new(max_size)),
        }
    }

    // ========================================================================
    // get_connection: 从连接池获取一个连接
    // ========================================================================
    // 工作流程：
    // 1. 获取信号量许可（如果池满了会阻塞等待）
    // 2. 尝试从池中取出一个空闲连接
    // 3. 如果池为空，创建新连接
    // 4. 返回包装后的连接（PooledConnection）
    pub async fn get_connection(&self) -> Result<PooledConnection> {
        // 获取信号量许可
        // 这会阻塞直到有可用的许可（即连接数未达到上限）
        // acquire_owned 返回一个拥有所有权的许可，可以跨 await 边界传递
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|e| anyhow!("failed to acquire semaphore permit: {}", e))?;

        // 尝试从池中获取一个空闲连接
        {
            let mut pool = self.pool.lock().await;
            if let Some(conn) = pool.pop() {
                debug!("reusing connection to {}", self.addr);
                // 返回复用的连接
                return Ok(PooledConnection {
                    conn: Some(conn),
                    pool: self.pool.clone(),
                    _permit: permit,  // 许可会在 PooledConnection drop 时自动释放
                });
            }
        }

        // 池中没有空闲连接，创建新连接
        match RedisConnection::new(&self.addr).await {
            Ok(conn) => {
                debug!("created new connection to {}", self.addr);
                Ok(PooledConnection {
                    conn: Some(conn),
                    pool: self.pool.clone(),
                    _permit: permit,
                })
            }
            Err(e) => {
                error!("failed to connect to {}: {}", self.addr, e);
                // 连接失败，许可会自动释放（permit drop）
                Err(e)
            }
        }
    }

    pub fn addr(&self) -> &str {
        &self.addr
    }
}

// ============================================================================
// PooledConnection 结构体：池化的连接
// ============================================================================
// 这是一个智能指针，包装了 RedisConnection
// 当它被 drop 时，会自动将连接归还到池中
pub struct PooledConnection {
    conn: Option<RedisConnection>,              // 实际的连接（Option 用于 drop 时取出）
    pool: Arc<Mutex<Vec<RedisConnection>>>,     // 连接池的引用
    _permit: tokio::sync::OwnedSemaphorePermit, // 信号量许可（下划线表示不直接使用）
}

impl PooledConnection {
    // ========================================================================
    // send_command: 发送命令（代理到内部的 RedisConnection）
    // ========================================================================
    pub async fn send_command(&mut self, cmd: &RespValue) -> Result<RespValue> {
        self.conn
            .as_mut()
            .ok_or_else(|| anyhow!("connection is closed"))?
            .send_command(cmd)
            .await
    }
}

// ============================================================================
// Drop trait 实现：自动归还连接
// ============================================================================
// 当 PooledConnection 被销毁时，这个方法会自动调用
// 它负责将连接归还到池中，并释放信号量许可
impl Drop for PooledConnection {
    fn drop(&mut self) {
        // 取出连接（Option::take 会将 self.conn 设为 None）
        if let Some(conn) = self.conn.take() {
            let pool = self.pool.clone();

            // 在后台任务中归还连接
            // 使用 spawn 避免阻塞当前任务
            tokio::task::spawn(async move {
                let mut pool = pool.lock().await;
                pool.push(conn);  // 将连接放回池中
            });
        }
        // _permit 会在这里自动 drop，释放信号量许可
        // 这会唤醒一个正在等待连接的请求
    }
}

// ============================================================================
// RedisCluster 结构体：Redis 主从集群
// ============================================================================
// 管理一个 Redis 主从集群的连接
// 功能：
// 1. 维护主节点和多个从节点的连接池
// 2. 实现读写分离（写操作到主节点，读操作到从节点）
// 3. 从节点负载均衡（轮询）
// 4. 故障转移（从节点不可用时回退到主节点）
pub struct RedisCluster {
    master: ConnectionPool,                     // 主节点连接池
    slaves: Vec<ConnectionPool>,                // 从节点连接池列表
    next_slave: parking_lot::Mutex<usize>,      // 下一个要使用的从节点索引（轮询）
}

impl RedisCluster {
    // ========================================================================
    // new: 创建一个新的 Redis 集群
    // ========================================================================
    // 参数：
    //   - master_addr: 主节点地址
    //   - slave_addrs: 从节点地址列表
    //   - pool_size: 每个节点的连接池大小
    pub fn new(master_addr: String, slave_addrs: Vec<String>, pool_size: usize) -> Self {
        // 为主节点创建连接池
        let master = ConnectionPool::new(master_addr, pool_size);

        // 为每个从节点创建连接池
        let slaves = slave_addrs
            .into_iter()
            .map(|addr| ConnectionPool::new(addr, pool_size))
            .collect();

        Self {
            master,
            slaves,
            // 初始化轮询索引为 0
            next_slave: parking_lot::Mutex::new(0),
        }
    }

    // ========================================================================
    // get_master_connection: 获取主节点连接
    // ========================================================================
    // 用于写操作（SET, DEL, INCR 等）
    pub async fn get_master_connection(&self) -> Result<PooledConnection> {
        self.master.get_connection().await
    }

    // ========================================================================
    // get_slave_connection: 获取从节点连接
    // ========================================================================
    // 用于读操作（GET, MGET, KEYS 等）
    // 实现了：
    // 1. 轮询负载均衡：依次使用不同的从节点
    // 2. 故障转移：如果所有从节点都失败，回退到主节点
    pub async fn get_slave_connection(&self) -> Result<PooledConnection> {
        // 如果没有配置从节点，直接使用主节点
        if self.slaves.is_empty() {
            return self.master.get_connection().await;
        }

        // 获取下一个要使用的从节点索引（轮询算法）
        let mut idx = {
            let mut next = self.next_slave.lock();
            let current = *next;
            // 更新索引，循环到下一个从节点
            *next = (*next + 1) % self.slaves.len();
            current
        };

        // 尝试连接从节点，如果失败则尝试下一个
        for attempt in 0..self.slaves.len() {
            match self.slaves[idx].get_connection().await {
                Ok(conn) => return Ok(conn),
                Err(e) => {
                    warn!(
                        "failed to connect to slave {} (attempt {}): {}",
                        self.slaves[idx].addr(),
                        attempt + 1,
                        e
                    );
                    // 尝试下一个从节点
                    idx = (idx + 1) % self.slaves.len();
                }
            }
        }

        // 所有从节点都失败了，回退到主节点
        warn!("all slaves failed, falling back to master");
        self.master.get_connection().await
    }

    pub fn master_addr(&self) -> &str {
        self.master.addr()
    }

    pub fn slave_addrs(&self) -> Vec<&str> {
        self.slaves.iter().map(|s| s.addr()).collect()
    }
}
