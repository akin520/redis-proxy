use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub proxy: ProxyConfig,
    pub redis: RedisConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProxyConfig {
    #[serde(default = "default_bind_addr")]
    pub bind_addr: String,
    #[serde(default = "default_pool_size")]
    pub pool_size: usize,
    #[serde(default = "default_log_level")]
    pub log_level: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedisConfig {
    pub master: String,
    #[serde(default)]
    pub slaves: Vec<String>,
}

fn default_bind_addr() -> String {
    "127.0.0.1:6380".to_string()
}

fn default_pool_size() -> usize {
    50
}

fn default_log_level() -> String {
    "info".to_string()
}

impl Default for Config {
    fn default() -> Self {
        Self {
            proxy: ProxyConfig {
                bind_addr: default_bind_addr(),
                pool_size: default_pool_size(),
                log_level: default_log_level(),
            },
            redis: RedisConfig {
                master: "127.0.0.1:6379".to_string(),
                slaves: vec![],
            },
        }
    }
}

impl Config {
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = fs::read_to_string(path.as_ref())
            .context("failed to read config file")?;

        let config: Config = toml::from_str(&content)
            .context("failed to parse config file")?;

        Ok(config)
    }

    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let content = toml::to_string_pretty(self)
            .context("failed to serialize config")?;

        fs::write(path.as_ref(), content)
            .context("failed to write config file")?;

        Ok(())
    }

    pub fn validate(&self) -> Result<()> {
        if self.proxy.bind_addr.is_empty() {
            anyhow::bail!("proxy bind_addr cannot be empty");
        }

        if self.proxy.pool_size == 0 {
            anyhow::bail!("proxy pool_size must be greater than 0");
        }

        if self.redis.master.is_empty() {
            anyhow::bail!("redis master address cannot be empty");
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.proxy.bind_addr, "127.0.0.1:6380");
        assert_eq!(config.proxy.pool_size, 50);
        assert_eq!(config.redis.master, "127.0.0.1:6379");
    }

    #[test]
    fn test_config_validation() {
        let config = Config::default();
        assert!(config.validate().is_ok());

        let mut invalid_config = Config::default();
        invalid_config.redis.master = String::new();
        assert!(invalid_config.validate().is_err());
    }
}
