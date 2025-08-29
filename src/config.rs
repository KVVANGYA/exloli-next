use std::time::Duration;

use anyhow::Result;
use duration_str::deserialize_duration;
use once_cell::sync::OnceCell;
use serde::Deserialize;
use teloxide::types::{ChatId, Recipient};

pub static CHANNEL_ID: OnceCell<String> = OnceCell::new();

fn default_allow_public_commands() -> bool {
    true
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    /// 日志等级
    pub log_level: String,
    /// 同时下载线程数量
    pub threads_num: usize,
    /// 定时爬取间隔
    #[serde(deserialize_with = "deserialize_duration")]
    pub interval: Duration,
    /// Sqlite 数据库位置
    pub database_url: String,
    pub exhentai: ExHentai,
    pub telegraph: Telegraph,
    pub telegram: Telegram,
    pub s3: S3,
    pub ipfs: Ipfs,
    pub backup: Backup,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExHentai {
    /// 登陆 cookie
    pub cookie: String,
    /// 搜索参数
    pub search_params: Vec<(String, String)>,
    /// 最大遍历画廊数量
    pub search_count: usize,
    /// 翻译文件的位置
    pub trans_file: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Telegraph {
    /// Telegraph token
    pub access_token: String,
    /// 文章作者名称
    pub author_name: String,
    /// 文章作者连接
    pub author_url: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Telegram {
    /// 频道 id
    pub channel_id: Recipient,
    /// bot 名称
    pub bot_id: String,
    /// bot token
    pub token: String,
    /// 讨论组 ID
    pub group_id: ChatId,
    /// 入口讨论组 ID
    pub auth_group_id: ChatId,
    /// 是否允许非管理员使用公共命令
    #[serde(default = "default_allow_public_commands")]
    pub allow_public_commands: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct S3 {
    /// region
    pub region: String,
    /// S3 endpoint
    pub endpoint: String,
    /// bucket 名称
    pub bucket: String,
    /// access-key
    pub access_key: String,
    /// secret-key
    pub secret_key: String,
    /// 公开访问连接
    pub host: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Ipfs {
    pub gateway_host: String,
    pub gateway_date: String,
    /// teletype.in 授权令牌
    pub teletype_token: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Backup {
    /// 是否启用定时备份
    pub enabled: bool,
    /// 备份间隔（小时）
    pub interval_hours: u64,
    /// 备份目标频道/群组ID
    pub target_chat_id: ChatId,
    /// 备份文件保留天数
    pub retention_days: u32,
    /// 是否压缩备份文件
    pub compress: bool,
    /// 备份文件前缀
    pub file_prefix: String,
}

impl Config {
    pub fn new(path: &str) -> Result<Self> {
        let s = std::fs::read_to_string(path)?;
        Ok(toml::from_str(&s)?)
    }
}
