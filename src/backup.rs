use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use teloxide::prelude::*;
use teloxide::types::{InputFile, ParseMode};
use tokio::fs;
use tokio::time::sleep;
use tracing::{error, info, warn};

use crate::config::{Backup as BackupConfig, Config};
use crate::bot::Bot;

pub struct BackupService {
    config: BackupConfig,
    bot: Bot,
    database_path: String,
}

impl BackupService {
    pub fn new(config: BackupConfig, bot: Bot, database_path: String) -> Self {
        Self {
            config,
            bot,
            database_path,
        }
    }

    /// 启动定时备份服务
    pub async fn start(&self) -> Result<()> {
        if !self.config.enabled {
            info!("备份服务已禁用");
            return Ok(());
        }

        info!("启动定时备份服务，间隔: {} 小时", self.config.interval_hours);
        
        let interval = Duration::from_secs(self.config.interval_hours * 3600);
        
        loop {
            if let Err(e) = self.perform_backup().await {
                error!("备份失败: {}", e);
            }
            
            // 清理过期备份文件（如果启用）
            if self.config.enable_retention {
                if let Err(e) = self.cleanup_old_backups().await {
                    error!("清理过期备份失败: {}", e);
                }
            }
            
            sleep(interval).await;
        }
    }

    /// 执行备份操作
    pub async fn perform_backup(&self) -> Result<()> {
        info!("开始执行数据库备份");

        // 检查数据库文件是否存在
        if !Path::new(&self.database_path).exists() {
            warn!("数据库文件不存在: {}", self.database_path);
            return Ok(());
        }

        // 生成备份文件名
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)?
            .as_secs();
        let datetime: DateTime<Utc> = DateTime::from_timestamp(timestamp as i64, 0)
            .context("无效的时间戳")?;
        
        let backup_filename = if self.config.compress {
            format!("{}_{}.db.gz", self.config.file_prefix, datetime.format("%Y%m%d_%H%M%S"))
        } else {
            format!("{}_{}.db", self.config.file_prefix, datetime.format("%Y%m%d_%H%M%S"))
        };

        let backup_path = PathBuf::from(&backup_filename);

        // 创建备份文件
        if self.config.compress {
            self.create_compressed_backup(&backup_path).await?;
        } else {
            fs::copy(&self.database_path, &backup_path).await
                .context("复制数据库文件失败")?;
        }

        // 获取文件大小
        let file_size = fs::metadata(&backup_path).await?.len();
        let size_mb = file_size as f64 / 1024.0 / 1024.0;

        // 发送备份文件到指定频道
        let caption = format!(
            "🗄️ **数据库备份**\n\n📅 备份时间: {}\n📦 文件大小: {:.2} MB\n🔧 压缩: {}",
            datetime.format("%Y-%m-%d %H:%M:%S UTC"),
            size_mb,
            if self.config.compress { "是" } else { "否" }
        );

        let input_file = InputFile::file(&backup_path);
        
        match self.bot.send_document(self.config.target_chat_id, input_file)
            .caption(&caption)
            .parse_mode(ParseMode::Markdown)
            .await
        {
            Ok(_) => {
                info!("备份文件已成功发送到频道: {}", backup_filename);
            }
            Err(e) => {
                error!("发送备份文件失败: {}", e);
                return Err(e.into());
            }
        }

        // 删除本地备份文件
        if let Err(e) = fs::remove_file(&backup_path).await {
            warn!("删除本地备份文件失败: {}", e);
        }

        info!("备份操作完成");
        Ok(())
    }

    /// 创建压缩备份
    async fn create_compressed_backup(&self, backup_path: &Path) -> Result<()> {
        use std::io::prelude::*;
        use flate2::Compression;
        use flate2::write::GzEncoder;

        let input_data = fs::read(&self.database_path).await
            .context("读取数据库文件失败")?;

        let output_file = std::fs::File::create(backup_path)
            .context("创建备份文件失败")?;

        let mut encoder = GzEncoder::new(output_file, Compression::default());
        encoder.write_all(&input_data)
            .context("压缩数据失败")?;
        encoder.finish()
            .context("完成压缩失败")?;

        Ok(())
    }

    /// 清理过期的备份文件（从频道中删除）
    async fn cleanup_old_backups(&self) -> Result<()> {
        if !self.config.enable_retention {
            return Ok(());
        }

        // 注意：Telegram Bot API 不支持直接列出频道中的文件
        // 这里只是一个占位符实现，实际中可能需要维护一个备份记录数据库
        // 或者通过其他方式跟踪已发送的备份文件
        
        info!(
            "清理过期备份功能已启用，保留天数: {} 天。需要额外的实现来跟踪已发送的消息",
            self.config.retention_days
        );
        
        // TODO: 实现实际的清理逻辑
        // 1. 查询数据库中超过 retention_days 天的备份记录
        // 2. 删除对应的 Telegram 消息
        // 3. 从数据库中删除记录
        
        Ok(())
    }

    /// 手动触发备份
    pub async fn manual_backup(&self) -> Result<String> {
        self.perform_backup().await?;
        Ok("手动备份已完成".to_string())
    }
}

/// 启动备份服务的辅助函数
pub async fn start_backup_service(config: &Config, bot: Bot) -> Result<()> {
    if !config.backup.enabled {
        return Ok(());
    }

    let backup_service = BackupService::new(
        config.backup.clone(),
        bot,
        config.database_url.clone(),
    );

    // 在后台启动备份服务
    tokio::spawn(async move {
        if let Err(e) = backup_service.start().await {
            error!("备份服务错误: {}", e);
        }
    });

    Ok(())
}