use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandStats {
    pub count: u64,
    pub total_duration_ms: u64,
    pub avg_duration_ms: f64,
    pub min_duration_ms: u64,
    pub max_duration_ms: u64,
    pub errors: u64,
}

impl CommandStats {
    fn new() -> Self {
        Self {
            count: 0,
            total_duration_ms: 0,
            avg_duration_ms: 0.0,
            min_duration_ms: u64::MAX,
            max_duration_ms: 0,
            errors: 0,
        }
    }

    fn update(&mut self, duration_ms: u64, is_error: bool) {
        self.count += 1;
        self.total_duration_ms += duration_ms;
        self.avg_duration_ms = self.total_duration_ms as f64 / self.count as f64;
        self.min_duration_ms = self.min_duration_ms.min(duration_ms);
        self.max_duration_ms = self.max_duration_ms.max(duration_ms);
        if is_error {
            self.errors += 1;
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalStats {
    pub total_commands: u64,
    pub total_read_commands: u64,
    pub total_write_commands: u64,
    pub total_errors: u64,
    pub uptime_seconds: u64,
    pub connections_received: u64,
    pub connections_active: u64,
}

pub struct Statistics {
    command_stats: DashMap<String, RwLock<CommandStats>>,
    total_commands: AtomicU64,
    total_read_commands: AtomicU64,
    total_write_commands: AtomicU64,
    total_errors: AtomicU64,
    connections_received: AtomicU64,
    connections_active: AtomicU64,
    start_time: Instant,
}

impl Statistics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            command_stats: DashMap::new(),
            total_commands: AtomicU64::new(0),
            total_read_commands: AtomicU64::new(0),
            total_write_commands: AtomicU64::new(0),
            total_errors: AtomicU64::new(0),
            connections_received: AtomicU64::new(0),
            connections_active: AtomicU64::new(0),
            start_time: Instant::now(),
        })
    }

    pub fn record_command(
        &self,
        command_name: &str,
        is_read: bool,
        duration: Duration,
        is_error: bool,
    ) {
        let duration_ms = duration.as_millis() as u64;

        self.total_commands.fetch_add(1, Ordering::Relaxed);
        if is_read {
            self.total_read_commands.fetch_add(1, Ordering::Relaxed);
        } else {
            self.total_write_commands.fetch_add(1, Ordering::Relaxed);
        }

        if is_error {
            self.total_errors.fetch_add(1, Ordering::Relaxed);
        }

        let entry = self
            .command_stats
            .entry(command_name.to_uppercase())
            .or_insert_with(|| RwLock::new(CommandStats::new()));

        entry.write().update(duration_ms, is_error);
    }

    pub fn increment_connections(&self) {
        self.connections_received.fetch_add(1, Ordering::Relaxed);
        self.connections_active.fetch_add(1, Ordering::Relaxed);
    }

    pub fn decrement_connections(&self) {
        self.connections_active.fetch_sub(1, Ordering::Relaxed);
    }


    pub fn get_all_command_stats(&self) -> Vec<(String, CommandStats)> {
        self.command_stats
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().read().clone()))
            .collect()
    }

    pub fn get_global_stats(&self) -> GlobalStats {
        GlobalStats {
            total_commands: self.total_commands.load(Ordering::Relaxed),
            total_read_commands: self.total_read_commands.load(Ordering::Relaxed),
            total_write_commands: self.total_write_commands.load(Ordering::Relaxed),
            total_errors: self.total_errors.load(Ordering::Relaxed),
            uptime_seconds: self.start_time.elapsed().as_secs(),
            connections_received: self.connections_received.load(Ordering::Relaxed),
            connections_active: self.connections_active.load(Ordering::Relaxed),
        }
    }

    pub fn reset(&self) {
        self.command_stats.clear();
        self.total_commands.store(0, Ordering::Relaxed);
        self.total_read_commands.store(0, Ordering::Relaxed);
        self.total_write_commands.store(0, Ordering::Relaxed);
        self.total_errors.store(0, Ordering::Relaxed);
        self.connections_received.store(0, Ordering::Relaxed);
    }

    pub fn print_stats(&self) {
        let global = self.get_global_stats();
        println!("\n=== Redis Proxy Statistics ===");
        println!("Uptime: {} seconds", global.uptime_seconds);
        println!("Total Commands: {}", global.total_commands);
        println!("  Read Commands: {}", global.total_read_commands);
        println!("  Write Commands: {}", global.total_write_commands);
        println!("Total Errors: {}", global.total_errors);
        println!("Connections Received: {}", global.connections_received);
        println!("Active Connections: {}", global.connections_active);

        println!("\n=== Command Statistics ===");
        let mut stats: Vec<_> = self.get_all_command_stats();
        stats.sort_by(|a, b| b.1.count.cmp(&a.1.count));

        for (cmd, stat) in stats.iter().take(20) {
            println!(
                "{:<15} count: {:>8}  avg: {:>6.2}ms  min: {:>6}ms  max: {:>6}ms  errors: {:>6}",
                cmd, stat.count, stat.avg_duration_ms, stat.min_duration_ms, stat.max_duration_ms, stat.errors
            );
        }
        println!();
    }
}

impl Default for Statistics {
    fn default() -> Self {
        Self {
            command_stats: DashMap::new(),
            total_commands: AtomicU64::new(0),
            total_read_commands: AtomicU64::new(0),
            total_write_commands: AtomicU64::new(0),
            total_errors: AtomicU64::new(0),
            connections_received: AtomicU64::new(0),
            connections_active: AtomicU64::new(0),
            start_time: Instant::now(),
        }
    }
}
