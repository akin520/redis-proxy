# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a high-performance Redis proxy server written in Rust that implements read/write splitting, connection pooling, and command execution statistics. The proxy sits between Redis clients and a Redis master-slave cluster, automatically routing read commands to slave nodes and write commands to the master node.

## Build and Test Commands

```bash
# Build the project
cargo build --release

# Run tests
cargo test

# Run a specific test
cargo test test_name

# Run with debug logging
RUST_LOG=debug cargo run

# Generate default config file
cargo run -- --generate-config

# Run with custom config
cargo run -- /path/to/config.toml
```

## Architecture

### Core Components

**Protocol Layer (`src/protocol.rs`)**
- Implements RESP (Redis Serialization Protocol) parser
- `RespValue` enum represents all Redis data types: SimpleString, Error, Integer, BulkString, Array
- `RespParser::parse()` handles incremental parsing from BytesMut buffers
- Zero-copy parsing where possible for performance

**Connection Management (`src/pool.rs`)**
- `RedisConnection`: Low-level TCP connection wrapper with RESP protocol support
- `ConnectionPool`: Manages connection pool for a single Redis node using semaphore-based limiting
- `RedisCluster`: Manages master and multiple slave pools with round-robin load balancing
- Automatic failover: if all slaves fail, falls back to master for read operations

**Command Classification (`src/command.rs`)**
- `Command::parse()` extracts command name and arguments from RESP values
- `classify_command()` determines if command is read or write based on comprehensive command list
- Covers all major Redis commands: strings, hashes, lists, sets, sorted sets, streams, geo, hyperloglog, etc.
- Unknown commands default to write for safety

**Statistics Tracking (`src/stats.rs`)**
- Lock-free statistics using atomic operations and DashMap
- Tracks per-command metrics: count, avg/min/max duration, error count
- Global metrics: total commands, read/write split, connections, uptime
- `Statistics::record_command()` called after each command execution

**Request Handling (`src/proxy.rs`)**
- `ProxyHandler::handle_client()` manages client connection lifecycle
- Parses incoming RESP commands and routes based on command type
- Special `PROXY` command for introspection: STATS, INFO, RESET
- Error handling with proper RESP error responses

**Configuration (`src/config.rs`)**
- TOML-based configuration with serde
- Validates master address, pool size, bind address
- Supports default values for all settings

### Request Flow

1. Client connects to proxy on configured bind_addr (default: 127.0.0.1:6380)
2. Proxy parses RESP command from client
3. Command classifier determines read vs write
4. Read commands → get connection from slave pool (round-robin)
5. Write commands → get connection from master pool
6. Forward command to Redis node and return response
7. Record statistics (duration, success/error)

### Read/Write Splitting Logic

The proxy implements intelligent command routing in `src/command.rs:classify_command()`:
- Read commands (GET, HGET, LRANGE, etc.) go to slaves
- Write commands (SET, HSET, LPUSH, etc.) go to master
- Info/monitoring commands (PING, INFO) treated as reads
- Transaction commands (MULTI, EXEC) go to master
- Unknown commands default to master for safety

## Key Design Decisions

**Async I/O with Tokio**: All network operations are async for high concurrency without thread-per-connection overhead.

**Connection Pooling**: Reuses connections to Redis nodes to avoid connection setup overhead. Pool size configurable per node.

**Round-Robin Load Balancing**: Simple but effective distribution of read load across slaves. Index protected by parking_lot::Mutex for minimal contention.

**Graceful Degradation**: If slaves are unavailable, reads automatically fall back to master to maintain availability.

**Lock-Free Statistics**: Uses AtomicU64 for counters and DashMap for per-command stats to minimize contention in high-throughput scenarios.

## Testing Strategy

Unit tests focus on:
- Protocol parsing (complete and incomplete messages)
- Command classification (read vs write)
- Configuration validation

Integration testing requires running Redis instances and is not included in the test suite.

## Common Development Patterns

When adding support for new Redis commands:
1. Add command name to appropriate match arm in `src/command.rs:classify_command()`
2. Ensure command name is uppercase in the match pattern
3. Default to `CommandType::Write` if unsure for safety

When modifying statistics:
1. Use atomic operations for simple counters
2. Use DashMap for per-key statistics to avoid lock contention
3. Always record both success and error cases

When changing protocol handling:
1. Test with incomplete buffers (partial RESP messages)
2. Ensure proper CRLF handling
3. Handle null bulk strings and null arrays correctly
