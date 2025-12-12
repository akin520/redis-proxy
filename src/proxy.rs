use anyhow::{anyhow, Result};
use bytes::BytesMut;
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{debug, error, info};

use crate::command::Command;
use crate::pool::RedisCluster;
use crate::protocol::{RespParser, RespValue};
use crate::stats::Statistics;

pub struct ProxyHandler {
    cluster: Arc<RedisCluster>,
    stats: Arc<Statistics>,
}

impl ProxyHandler {
    pub fn new(cluster: Arc<RedisCluster>, stats: Arc<Statistics>) -> Self {
        Self { cluster, stats }
    }

    pub async fn handle_client(&self, mut client_stream: TcpStream, client_addr: String) {
        info!("new client connection from {}", client_addr);
        self.stats.increment_connections();

        let mut buffer = BytesMut::with_capacity(8192);

        loop {
            match client_stream.read_buf(&mut buffer).await {
                Ok(0) => {
                    debug!("client {} disconnected", client_addr);
                    break;
                }
                Ok(n) => {
                    debug!("received {} bytes from {}", n, client_addr);

                    while let Some(request) = self.parse_request(&mut buffer) {
                        match self.process_request(request).await {
                            Ok(response) => {
                                let encoded = response.encode();
                                if let Err(e) = client_stream.write_all(&encoded).await {
                                    if Self::is_connection_error(&e) {
                                        debug!("client {} disconnected while writing: {}", client_addr, e);
                                    } else {
                                        error!("failed to write response to {}: {}", client_addr, e);
                                    }
                                    break;
                                }
                            }
                            Err(e) => {
                                error!("error processing request from {}: {}", client_addr, e);
                                let error_response =
                                    RespValue::Error(format!("ERR {}", e));
                                let encoded = error_response.encode();
                                if let Err(e) = client_stream.write_all(&encoded).await {
                                    if Self::is_connection_error(&e) {
                                        debug!("client {} disconnected while writing error: {}", client_addr, e);
                                    } else {
                                        error!("failed to write error response to {}: {}", client_addr, e);
                                    }
                                    break;
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    // Connection reset by peer is normal when client disconnects
                    if Self::is_connection_error(&e) {
                        debug!("client {} disconnected: {}", client_addr, e);
                    } else {
                        error!("error reading from {}: {}", client_addr, e);
                    }
                    break;
                }
            }
        }

        self.stats.decrement_connections();
        info!("client {} connection closed", client_addr);
    }

    fn is_connection_error(e: &std::io::Error) -> bool {
        use std::io::ErrorKind;
        matches!(
            e.kind(),
            ErrorKind::ConnectionReset
                | ErrorKind::ConnectionAborted
                | ErrorKind::BrokenPipe
                | ErrorKind::UnexpectedEof
        )
    }

    fn parse_request(&self, buffer: &mut BytesMut) -> Option<RespValue> {
        match RespParser::parse(buffer) {
            Ok(Some(value)) => Some(value),
            Ok(None) => None,
            Err(e) => {
                error!("failed to parse request: {}", e);
                None
            }
        }
    }

    async fn process_request(&self, request: RespValue) -> Result<RespValue> {
        let command = Command::parse(&request)
            .ok_or_else(|| anyhow!("invalid command format"))?;

        debug!("processing command: {}", command.name);

        let start = Instant::now();
        let result = self.execute_command(&command, &request).await;
        let duration = start.elapsed();

        let is_error = result.is_err();
        self.stats.record_command(
            &command.name,
            command.is_read(),
            duration,
            is_error,
        );

        result
    }

    async fn execute_command(
        &self,
        command: &Command,
        request: &RespValue,
    ) -> Result<RespValue> {
        if command.name == "PROXY" {
            return self.handle_proxy_command(command).await;
        }

        if command.is_read() {
            debug!("routing {} to slave", command.name);
            let mut conn = self.cluster.get_slave_connection().await?;
            conn.send_command(request).await
        } else {
            debug!("routing {} to master", command.name);
            let mut conn = self.cluster.get_master_connection().await?;
            conn.send_command(request).await
        }
    }

    async fn handle_proxy_command(&self, command: &Command) -> Result<RespValue> {
        if command.args.is_empty() {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'proxy' command".to_string(),
            ));
        }

        let subcommand = String::from_utf8_lossy(&command.args[0]).to_uppercase();

        match subcommand.as_str() {
            "STATS" => self.get_stats_response(),
            "RESET" => {
                self.stats.reset();
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            "INFO" => self.get_info_response(),
            _ => Ok(RespValue::Error(format!(
                "ERR unknown PROXY subcommand '{}'",
                subcommand
            ))),
        }
    }

    fn get_stats_response(&self) -> Result<RespValue> {
        let global = self.stats.get_global_stats();
        let mut stats = self.stats.get_all_command_stats();
        stats.sort_by(|a, b| b.1.count.cmp(&a.1.count));

        let mut lines = vec![
            format!("# Global Statistics"),
            format!("uptime_seconds:{}", global.uptime_seconds),
            format!("total_commands:{}", global.total_commands),
            format!("total_read_commands:{}", global.total_read_commands),
            format!("total_write_commands:{}", global.total_write_commands),
            format!("total_errors:{}", global.total_errors),
            format!("connections_received:{}", global.connections_received),
            format!("connections_active:{}", global.connections_active),
            format!(""),
            format!("# Command Statistics"),
        ];

        for (cmd, stat) in stats.iter().take(50) {
            lines.push(format!(
                "{}:count={},avg={:.2}ms,min={}ms,max={}ms,errors={}",
                cmd,
                stat.count,
                stat.avg_duration_ms,
                stat.min_duration_ms,
                stat.max_duration_ms,
                stat.errors
            ));
        }

        Ok(RespValue::BulkString(Some(lines.join("\r\n").into_bytes())))
    }

    fn get_info_response(&self) -> Result<RespValue> {
        let info = format!(
            "# Redis Proxy\r\n\
             master:{}\r\n\
             slaves:{}\r\n\
             version:0.1.0\r\n",
            self.cluster.master_addr(),
            self.cluster.slave_addrs().join(",")
        );

        Ok(RespValue::BulkString(Some(info.into_bytes())))
    }
}
