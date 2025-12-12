use crate::protocol::RespValue;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandType {
    Read,
    Write,
}

pub struct Command {
    pub name: String,
    pub cmd_type: CommandType,
    pub args: Vec<Vec<u8>>,
}

impl Command {
    pub fn parse(value: &RespValue) -> Option<Self> {
        let parts = value.as_command()?;
        if parts.is_empty() {
            return None;
        }

        let name = String::from_utf8_lossy(&parts[0]).to_uppercase();
        let cmd_type = Self::classify_command(&name);
        let args = parts[1..].to_vec();

        Some(Self {
            name,
            cmd_type,
            args,
        })
    }

    fn classify_command(name: &str) -> CommandType {
        match name {
            // Read commands
            "GET" | "MGET" | "STRLEN" | "GETRANGE" | "GETBIT" | "GETEX" | "GETDEL" => {
                CommandType::Read
            }
            "HGET" | "HMGET" | "HGETALL" | "HKEYS" | "HVALS" | "HLEN" | "HEXISTS"
            | "HSCAN" => CommandType::Read,
            "LLEN" | "LINDEX" | "LRANGE" | "LPOS" => CommandType::Read,
            "SCARD" | "SISMEMBER" | "SMISMEMBER" | "SMEMBERS" | "SRANDMEMBER" | "SSCAN" => {
                CommandType::Read
            }
            "ZCARD" | "ZCOUNT" | "ZLEXCOUNT" | "ZSCORE" | "ZMSCORE" | "ZRANK" | "ZREVRANK"
            | "ZRANGE" | "ZREVRANGE" | "ZRANGEBYSCORE" | "ZREVRANGEBYSCORE"
            | "ZRANGEBYLEX" | "ZREVRANGEBYLEX" | "ZSCAN" => CommandType::Read,
            "EXISTS" | "TYPE" | "TTL" | "PTTL" | "KEYS" | "SCAN" | "RANDOMKEY" | "DUMP" => {
                CommandType::Read
            }
            "BITCOUNT" | "BITPOS" => CommandType::Read,
            "PFCOUNT" => CommandType::Read,
            "GEOHASH" | "GEOPOS" | "GEODIST" | "GEORADIUS" | "GEORADIUSBYMEMBER"
            | "GEOSEARCH" => CommandType::Read,
            "XLEN" | "XRANGE" | "XREVRANGE" | "XREAD" | "XPENDING" => CommandType::Read,
            "LOLWUT" => CommandType::Read,

            // Info and monitoring commands (read-only)
            "INFO" | "DBSIZE" | "LASTSAVE" | "TIME" | "PING" | "ECHO" | "CLIENT"
            | "COMMAND" | "CONFIG" | "MEMORY" | "SLOWLOG" | "LATENCY" | "MODULE" => {
                CommandType::Read
            }

            // Write commands
            "SET" | "SETNX" | "SETEX" | "PSETEX" | "SETRANGE" | "SETBIT" | "MSET"
            | "MSETNX" | "APPEND" | "INCR" | "INCRBY" | "INCRBYFLOAT" | "DECR"
            | "DECRBY" => CommandType::Write,
            "HSET" | "HSETNX" | "HMSET" | "HINCRBY" | "HINCRBYFLOAT" | "HDEL" => {
                CommandType::Write
            }
            "LPUSH" | "LPUSHX" | "RPUSH" | "RPUSHX" | "LPOP" | "RPOP" | "BLPOP" | "BRPOP"
            | "BRPOPLPUSH" | "LINSERT" | "LSET" | "LREM" | "LTRIM" | "RPOPLPUSH"
            | "LMOVE" | "BLMOVE" | "LMPOP" | "BLMPOP" => CommandType::Write,
            "SADD" | "SREM" | "SPOP" | "SMOVE" | "SINTERSTORE" | "SUNIONSTORE"
            | "SDIFFSTORE" => CommandType::Write,
            "ZADD" | "ZINCRBY" | "ZREM" | "ZREMRANGEBYRANK" | "ZREMRANGEBYSCORE"
            | "ZREMRANGEBYLEX" | "ZPOPMIN" | "ZPOPMAX" | "BZPOPMIN" | "BZPOPMAX"
            | "ZINTERSTORE" | "ZUNIONSTORE" | "ZDIFFSTORE" | "ZMPOP" | "BZMPOP"
            | "ZRANGESTORE" => CommandType::Write,
            "DEL" | "UNLINK" | "EXPIRE" | "EXPIREAT" | "PEXPIRE" | "PEXPIREAT" | "PERSIST"
            | "RENAME" | "RENAMENX" | "MOVE" | "COPY" | "RESTORE" | "MIGRATE"
            | "TOUCH" => CommandType::Write,
            "GETSET" => CommandType::Write,
            "BITOP" | "BITFIELD" | "BITFIELD_RO" => CommandType::Write,
            "PFADD" | "PFMERGE" => CommandType::Write,
            "GEOADD" | "GEORADIUSSTORE" | "GEORADIUSBYMEMBERSTORE" | "GEOSEARCHSTORE" => {
                CommandType::Write
            }
            "XADD" | "XTRIM" | "XDEL" | "XACK" | "XGROUP" | "XCLAIM" | "XAUTOCLAIM"
            | "XREADGROUP" | "XSETID" => CommandType::Write,

            // Transaction commands (write)
            "MULTI" | "EXEC" | "DISCARD" | "WATCH" | "UNWATCH" => CommandType::Write,

            // Pub/Sub commands (write)
            "PUBLISH" | "SUBSCRIBE" | "UNSUBSCRIBE" | "PSUBSCRIBE" | "PUNSUBSCRIBE"
            | "PUBSUB" => CommandType::Write,

            // Script commands (write by default for safety)
            "EVAL" | "EVALSHA" | "SCRIPT" => CommandType::Write,

            // Database commands (write)
            "FLUSHDB" | "FLUSHALL" | "SELECT" | "SWAPDB" => CommandType::Write,

            // Server commands (write)
            "SAVE" | "BGSAVE" | "BGREWRITEAOF" | "SHUTDOWN" | "REPLICAOF" | "SLAVEOF"
            | "ROLE" | "DEBUG" | "MONITOR" | "SYNC" | "PSYNC" => CommandType::Write,

            // Default to write for safety
            _ => CommandType::Write,
        }
    }

    pub fn is_read(&self) -> bool {
        self.cmd_type == CommandType::Read
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_classify_read_commands() {
        let get_cmd = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"GET".to_vec())),
            RespValue::BulkString(Some(b"key".to_vec())),
        ]));
        let cmd = Command::parse(&get_cmd).unwrap();
        assert_eq!(cmd.cmd_type, CommandType::Read);
        assert_eq!(cmd.name, "GET");
    }

    #[test]
    fn test_classify_write_commands() {
        let set_cmd = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"SET".to_vec())),
            RespValue::BulkString(Some(b"key".to_vec())),
            RespValue::BulkString(Some(b"value".to_vec())),
        ]));
        let cmd = Command::parse(&set_cmd).unwrap();
        assert_eq!(cmd.cmd_type, CommandType::Write);
        assert_eq!(cmd.name, "SET");
    }
}
