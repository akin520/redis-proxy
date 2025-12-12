// ============================================================================
// Redis 协议解析器 (RESP - Redis Serialization Protocol)
// ============================================================================
// 这个模块负责解析和编码 Redis 协议格式的数据
// Redis 使用 RESP 协议进行客户端和服务器之间的通信

use anyhow::{anyhow, Result};
use bytes::{Buf, BytesMut};
use std::io::Cursor;

// ============================================================================
// RespValue 枚举：表示 Redis 协议中的所有数据类型
// ============================================================================
#[derive(Debug, Clone, PartialEq)]
pub enum RespValue {
    // 简单字符串：以 + 开头，例如 "+OK\r\n"
    SimpleString(String),

    // 错误信息：以 - 开头，例如 "-ERR unknown command\r\n"
    Error(String),

    // 整数：以 : 开头，例如 ":1000\r\n"
    Integer(i64),

    // 批量字符串：以 $ 开头，可以为 None（表示 null）
    // 例如 "$5\r\nhello\r\n" 或 "$-1\r\n"（null）
    BulkString(Option<Vec<u8>>),

    // 数组：以 * 开头，可以为 None（表示 null）
    // 例如 "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n"
    Array(Option<Vec<RespValue>>),
}

impl RespValue {
    // ========================================================================
    // encode: 将 RespValue 编码为 Redis 协议格式的字节数组
    // ========================================================================
    // 这个方法将内存中的数据结构转换为可以通过网络发送的字节流
    pub fn encode(&self) -> Vec<u8> {
        match self {
            // 简单字符串：格式为 "+内容\r\n"
            RespValue::SimpleString(s) => format!("+{}\r\n", s).into_bytes(),

            // 错误信息：格式为 "-内容\r\n"
            RespValue::Error(e) => format!("-{}\r\n", e).into_bytes(),

            // 整数：格式为 ":数字\r\n"
            RespValue::Integer(i) => format!(":{}\r\n", i).into_bytes(),

            // 空批量字符串：格式为 "$-1\r\n"
            RespValue::BulkString(None) => b"$-1\r\n".to_vec(),

            // 批量字符串：格式为 "$长度\r\n数据\r\n"
            RespValue::BulkString(Some(data)) => {
                let mut result = format!("${}\r\n", data.len()).into_bytes();
                result.extend_from_slice(data);
                result.extend_from_slice(b"\r\n");
                result
            }

            // 空数组：格式为 "*-1\r\n"
            RespValue::Array(None) => b"*-1\r\n".to_vec(),

            // 数组：格式为 "*元素个数\r\n" + 每个元素的编码
            RespValue::Array(Some(arr)) => {
                let mut result = format!("*{}\r\n", arr.len()).into_bytes();
                for item in arr {
                    result.extend_from_slice(&item.encode());
                }
                result
            }
        }
    }

    // ========================================================================
    // as_command: 将 RespValue 转换为命令格式
    // ========================================================================
    // Redis 命令通常是一个数组，其中每个元素都是批量字符串
    // 例如：["GET", "key"] 表示 GET key 命令
    pub fn as_command(&self) -> Option<Vec<Vec<u8>>> {
        match self {
            RespValue::Array(Some(arr)) => {
                let mut cmd = Vec::new();
                // 遍历数组中的每个元素
                for item in arr {
                    match item {
                        // 只接受批量字符串作为命令参数
                        RespValue::BulkString(Some(data)) => cmd.push(data.clone()),
                        // 如果遇到其他类型，返回 None
                        _ => return None,
                    }
                }
                Some(cmd)
            }
            _ => None,
        }
    }
}

// ============================================================================
// RespParser 结构体：Redis 协议解析器
// ============================================================================
pub struct RespParser;

impl RespParser {
    // ========================================================================
    // parse: 从字节缓冲区中解析一个完整的 RESP 值
    // ========================================================================
    // 参数：
    //   - buf: 可变的字节缓冲区，解析成功后会移除已解析的数据
    // 返回：
    //   - Ok(Some(value)): 成功解析出一个值
    //   - Ok(None): 数据不完整，需要更多数据
    //   - Err: 解析错误
    pub fn parse(buf: &mut BytesMut) -> Result<Option<RespValue>> {
        // 如果缓冲区为空，返回 None
        if buf.is_empty() {
            return Ok(None);
        }

        // 创建一个游标用于读取数据
        let mut cursor = Cursor::new(&buf[..]);

        // 检查第一个字节，判断是标准 RESP 格式还是内联命令格式
        // 标准 RESP 格式以特殊字符开头：+ - : $ *
        // 内联命令格式直接是文本，例如 "PING\r\n"（redis-benchmark 使用）
        let first_byte = buf[0];
        let result = if matches!(first_byte, b'+' | b'-' | b':' | b'$' | b'*') {
            // 标准 RESP 格式
            Self::parse_value(&mut cursor)
        } else {
            // 内联命令格式（用于 redis-benchmark 等工具）
            Self::parse_inline_command(&mut cursor)
        };

        match result {
            Ok(value) => {
                // 解析成功，移除已解析的数据
                let pos = cursor.position() as usize;
                buf.advance(pos);
                Ok(Some(value))
            }
            Err(e) => {
                // 如果是数据不完整的错误，返回 None 等待更多数据
                if e.to_string().contains("incomplete") {
                    Ok(None)
                } else {
                    // 其他错误直接返回
                    Err(e)
                }
            }
        }
    }

    // ========================================================================
    // parse_inline_command: 解析内联命令格式
    // ========================================================================
    // 内联命令格式：直接的文本命令，用空格分隔参数
    // 例如："PING\r\n" 或 "SET key value\r\n"
    // 这种格式主要用于 redis-benchmark 和 telnet 等工具
    fn parse_inline_command(cursor: &mut Cursor<&[u8]>) -> Result<RespValue> {
        // 读取一行（直到 \r\n）
        let line = Self::read_line(cursor)?;
        let line_str = String::from_utf8(line).map_err(|e| anyhow!("invalid UTF-8: {}", e))?;

        // 按空格分割命令和参数，转换为批量字符串数组
        let parts: Vec<RespValue> = line_str
            .split_whitespace()
            .map(|s| RespValue::BulkString(Some(s.as_bytes().to_vec())))
            .collect();

        // 空命令是无效的
        if parts.is_empty() {
            return Err(anyhow!("empty inline command"));
        }

        // 返回数组格式（与标准 RESP 命令格式一致）
        Ok(RespValue::Array(Some(parts)))
    }

    // ========================================================================
    // parse_value: 解析标准 RESP 格式的值
    // ========================================================================
    // 根据第一个字节判断类型，然后调用相应的解析函数
    fn parse_value(cursor: &mut Cursor<&[u8]>) -> Result<RespValue> {
        // 检查是否还有数据
        if !cursor.has_remaining() {
            return Err(anyhow!("incomplete data"));
        }

        // 读取类型标记字节
        let type_byte = cursor.get_u8();
        match type_byte {
            b'+' => Self::parse_simple_string(cursor),  // 简单字符串
            b'-' => Self::parse_error(cursor),          // 错误
            b':' => Self::parse_integer(cursor),        // 整数
            b'$' => Self::parse_bulk_string(cursor),    // 批量字符串
            b'*' => Self::parse_array(cursor),          // 数组
            _ => Err(anyhow!("invalid RESP type: {}", type_byte as char)),
        }
    }

    // ========================================================================
    // parse_simple_string: 解析简单字符串
    // ========================================================================
    // 格式：+内容\r\n
    // 例如：+OK\r\n
    fn parse_simple_string(cursor: &mut Cursor<&[u8]>) -> Result<RespValue> {
        let line = Self::read_line(cursor)?;
        Ok(RespValue::SimpleString(
            String::from_utf8(line).map_err(|e| anyhow!("invalid UTF-8: {}", e))?,
        ))
    }

    // ========================================================================
    // parse_error: 解析错误信息
    // ========================================================================
    // 格式：-内容\r\n
    // 例如：-ERR unknown command\r\n
    fn parse_error(cursor: &mut Cursor<&[u8]>) -> Result<RespValue> {
        let line = Self::read_line(cursor)?;
        Ok(RespValue::Error(
            String::from_utf8(line).map_err(|e| anyhow!("invalid UTF-8: {}", e))?,
        ))
    }

    // ========================================================================
    // parse_integer: 解析整数
    // ========================================================================
    // 格式：:数字\r\n
    // 例如：:1000\r\n
    fn parse_integer(cursor: &mut Cursor<&[u8]>) -> Result<RespValue> {
        let line = Self::read_line(cursor)?;
        let s = String::from_utf8(line).map_err(|e| anyhow!("invalid UTF-8: {}", e))?;
        let num = s.parse::<i64>().map_err(|e| anyhow!("invalid integer: {}", e))?;
        Ok(RespValue::Integer(num))
    }

    // ========================================================================
    // parse_bulk_string: 解析批量字符串
    // ========================================================================
    // 格式：$长度\r\n数据\r\n
    // 例如：$5\r\nhello\r\n
    // 特殊情况：$-1\r\n 表示 null
    fn parse_bulk_string(cursor: &mut Cursor<&[u8]>) -> Result<RespValue> {
        // 读取长度行
        let line = Self::read_line(cursor)?;
        let s = String::from_utf8(line).map_err(|e| anyhow!("invalid UTF-8: {}", e))?;
        let len = s.parse::<i64>().map_err(|e| anyhow!("invalid length: {}", e))?;

        // -1 表示 null
        if len == -1 {
            return Ok(RespValue::BulkString(None));
        }

        let len = len as usize;
        // 检查剩余数据是否足够（数据 + \r\n）
        if cursor.remaining() < len + 2 {
            return Err(anyhow!("incomplete bulk string"));
        }

        // 读取指定长度的数据
        let mut data = vec![0u8; len];
        cursor.copy_to_slice(&mut data);

        // 验证结束符 \r\n
        if cursor.get_u8() != b'\r' || cursor.get_u8() != b'\n' {
            return Err(anyhow!("invalid bulk string terminator"));
        }

        Ok(RespValue::BulkString(Some(data)))
    }

    // ========================================================================
    // parse_array: 解析数组
    // ========================================================================
    // 格式：*元素个数\r\n + 每个元素
    // 例如：*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n
    // 特殊情况：*-1\r\n 表示 null
    fn parse_array(cursor: &mut Cursor<&[u8]>) -> Result<RespValue> {
        // 读取元素个数
        let line = Self::read_line(cursor)?;
        let s = String::from_utf8(line).map_err(|e| anyhow!("invalid UTF-8: {}", e))?;
        let len = s.parse::<i64>().map_err(|e| anyhow!("invalid length: {}", e))?;

        // -1 表示 null
        if len == -1 {
            return Ok(RespValue::Array(None));
        }

        // 递归解析每个元素
        let len = len as usize;
        let mut arr = Vec::with_capacity(len);
        for _ in 0..len {
            arr.push(Self::parse_value(cursor)?);
        }

        Ok(RespValue::Array(Some(arr)))
    }

    // ========================================================================
    // read_line: 读取一行数据（直到 \r\n）
    // ========================================================================
    // 这是一个辅助函数，用于读取以 \r\n 结尾的一行数据
    fn read_line(cursor: &mut Cursor<&[u8]>) -> Result<Vec<u8>> {
        let start = cursor.position() as usize;
        let slice = &cursor.get_ref()[start..];

        // 使用滑动窗口查找 \r\n
        for (i, window) in slice.windows(2).enumerate() {
            if window == b"\r\n" {
                let end = start + i;
                // 移动游标到 \r\n 之后
                cursor.set_position((end + 2) as u64);
                // 返回不包含 \r\n 的数据
                return Ok(cursor.get_ref()[start..end].to_vec());
            }
        }

        // 没有找到 \r\n，数据不完整
        Err(anyhow!("incomplete line"))
    }
}

// ============================================================================
// 单元测试
// ============================================================================
#[cfg(test)]
mod tests {
    use super::*;

    // 测试解析简单字符串
    #[test]
    fn test_parse_simple_string() {
        let mut buf = BytesMut::from("+OK\r\n");
        let result = RespParser::parse(&mut buf).unwrap();
        assert_eq!(result, Some(RespValue::SimpleString("OK".to_string())));
    }

    // 测试解析批量字符串
    #[test]
    fn test_parse_bulk_string() {
        let mut buf = BytesMut::from("$5\r\nhello\r\n");
        let result = RespParser::parse(&mut buf).unwrap();
        assert_eq!(
            result,
            Some(RespValue::BulkString(Some(b"hello".to_vec())))
        );
    }

    // 测试解析数组（Redis 命令格式）
    #[test]
    fn test_parse_array() {
        let mut buf = BytesMut::from("*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n");
        let result = RespParser::parse(&mut buf).unwrap();
        assert_eq!(
            result,
            Some(RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b"GET".to_vec())),
                RespValue::BulkString(Some(b"key".to_vec())),
            ])))
        );
    }

    // 测试解析内联命令（redis-benchmark 格式）
    #[test]
    fn test_parse_inline_command() {
        let mut buf = BytesMut::from("PING\r\n");
        let result = RespParser::parse(&mut buf).unwrap();
        assert_eq!(
            result,
            Some(RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b"PING".to_vec())),
            ])))
        );
    }

    // 测试解析带参数的内联命令
    #[test]
    fn test_parse_inline_command_with_args() {
        let mut buf = BytesMut::from("SET key value\r\n");
        let result = RespParser::parse(&mut buf).unwrap();
        assert_eq!(
            result,
            Some(RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b"SET".to_vec())),
                RespValue::BulkString(Some(b"key".to_vec())),
                RespValue::BulkString(Some(b"value".to_vec())),
            ])))
        );
    }
}
