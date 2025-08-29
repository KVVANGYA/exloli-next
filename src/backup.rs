use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use teloxide::prelude::*;
use teloxide::types::{InputFile, ParseMode};
use tokio::fs;
use tokio::time::sleep;
use tracing::{error, info, warn};

use crate::config::Backup as BackupConfig;
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
                self.send_error_notification(&format!("备份失败: {}", e)).await;
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
        info!("开始执行应用程序完整备份");

        // 检查 /app 目录是否存在
        let app_dir = Path::new("/app");
        if !app_dir.exists() {
            warn!("/app 目录不存在，尝试备份当前工作目录");
            // 如果 /app 不存在，使用当前工作目录
            let current_dir = std::env::current_dir()?;
            return self.backup_directory(&current_dir).await;
        }

        self.backup_directory(app_dir).await
    }

    /// 备份指定目录
    async fn backup_directory(&self, dir_path: &Path) -> Result<()> {
        info!("备份目录: {}", dir_path.display());

        // 如果目录包含SQLite数据库，先尝试创建数据库快照
        self.prepare_sqlite_backup(dir_path).await?;

        // 生成备份文件名
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)?
            .as_secs();
        let datetime: DateTime<Utc> = DateTime::from_timestamp(timestamp as i64, 0)
            .context("无效的时间戳")?;
        
        let backup_filename = if self.config.compress {
            format!(
                "{}_{}.tar.gz", 
                self.config.file_prefix, 
                datetime.format("%Y%m%d_%H%M%S")
            )
        } else {
            format!(
                "{}_{}.tar", 
                self.config.file_prefix, 
                datetime.format("%Y%m%d_%H%M%S")
            )
        };

        let backup_path = PathBuf::from(&backup_filename);

        // 创建目录备份
        self.create_directory_backup(dir_path, &backup_path, self.config.compress).await?;

        // 获取文件大小
        let file_size = fs::metadata(&backup_path).await?.len();
        let size_mb = file_size as f64 / 1024.0 / 1024.0;

        // 发送备份文件到指定频道
        let format_info = if self.config.compress {
            "tar.gz 压缩包"
        } else {
            "tar 未压缩包"
        };
        
        let caption = format!(
            "🗄️ *应用程序完整备份*

📅 备份时间: {}
📦 文件大小: {:.2} MB
📁 备份内容: {} 目录完整备份
🔧 格式: {}",
            datetime.format("%Y\\--%m\\--%d %H:%M:%S UTC").to_string().replace("-", "\\-"),
            size_mb,
            dir_path.display().to_string().replace("-", "\\-").replace(".", "\\."),
            format_info.replace(".", "\\.")
        );

        let input_file = InputFile::file(&backup_path);
        
        match self.bot.send_document(self.config.target_chat_id, input_file)
            .caption(&caption)
            .parse_mode(ParseMode::MarkdownV2)
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

    /// 创建目录备份
    async fn create_directory_backup(&self, source_dir: &Path, backup_path: &Path, compress: bool) -> Result<()> {
        use tokio::process::Command;
        
        let format_desc = if compress { "tar.gz 压缩" } else { "tar 未压缩" };
        info!("创建目录备份 ({}): {} -> {}", format_desc, source_dir.display(), backup_path.display());

        // 简化 tar 命令参数，直接打包目录
        let tar_command = if compress {
            if cfg!(target_os = "windows") {
                // Windows 上的 tar 命令使用简化参数
                vec![
                    "-czf",
                    backup_path.to_str().context("备份路径转换失败")?,
                    "--exclude=*.log",
                    "--exclude=*.tmp",
                    "--exclude=target",
                    "--exclude=exloli_backup_*.tar.gz",
                    "--exclude=exloli_backup_*.tar",
                    source_dir.to_str().context("源目录路径转换失败")?
                ]
            } else {
                // Unix 系统使用完整的 GNU tar 选项
                vec![
                    "-czf",
                    backup_path.to_str().context("备份路径转换失败")?,
                    "--warning=no-file-changed",
                    "--warning=no-file-removed",
                    "--ignore-failed-read",
                    "--exclude=*.log",
                    "--exclude=*.tmp",
                    "--exclude=target",
                    "--exclude=exloli_backup_*.tar.gz",
                    "--exclude=exloli_backup_*.tar",
                    source_dir.to_str().context("源目录路径转换失败")?
                ]
            }
        } else {
            if cfg!(target_os = "windows") {
                // Windows 上的 tar 命令使用简化参数
                vec![
                    "-cf",
                    backup_path.to_str().context("备份路径转换失败")?,
                    "--exclude=*.log",
                    "--exclude=*.tmp", 
                    "--exclude=target",
                    "--exclude=exloli_backup_*.tar.gz",
                    "--exclude=exloli_backup_*.tar",
                    source_dir.to_str().context("源目录路径转换失败")?
                ]
            } else {
                // Unix 系统使用完整的 GNU tar 选项
                vec![
                    "-cf",
                    backup_path.to_str().context("备份路径转换失败")?,
                    "--warning=no-file-changed",
                    "--warning=no-file-removed",
                    "--ignore-failed-read",
                    "--exclude=*.log",
                    "--exclude=*.tmp", 
                    "--exclude=target",
                    "--exclude=exloli_backup_*.tar.gz",
                    "--exclude=exloli_backup_*.tar",
                    source_dir.to_str().context("源目录路径转换失败")?
                ]
            }
        };

        // 首先检查 tar 命令是否可用
        let tar_available = Command::new("tar")
            .arg("--version")
            .output()
            .await
            .map(|output| output.status.success())
            .unwrap_or(false);

        if !tar_available {
            error!("tar 命令不可用，请安装 tar 工具或在 Docker 容器中运行");
            return Err(anyhow::anyhow!("tar 命令不可用，无法创建备份"));
        }

        // 使用 tar 命令创建备份
        info!("执行 tar 命令，参数: {:?}", tar_command);
        let tar_command_clone = tar_command.clone();
        let output = Command::new("tar")
            .args(tar_command)
            .output()
            .await
            .context("执行 tar 命令失败")?;

        // 检查命令执行结果，但允许某些警告
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        let exit_code = output.status.code().unwrap_or(-1);
        
        info!("tar 命令执行结果: 退出码={}, stderr长度={}, stdout长度={}", 
              exit_code, stderr.len(), stdout.len());
        
        if !output.status.success() {
            // 如果 stderr 和 stdout 都为空，可能是命令没有正确执行
            if stderr.is_empty() && stdout.is_empty() {
                error!("tar 命令执行失败，无输出信息。退出码: {}", exit_code);
                error!("这可能是因为:");
                error!("1. tar 命令不存在或无法执行");
                error!("2. 权限不足");
                error!("3. 路径参数有误");
                error!("命令参数: {:?}", tar_command_clone);
                return Err(anyhow::anyhow!("tar 命令执行失败，退出码: {}", exit_code));
            }
            
            // 如果只是文件变更相关的警告或tar的常见警告，不视为错误
            if stderr.contains("file changed as we read it") || 
               stderr.contains("file removed before we read it") ||
               stderr.contains("db.sqlite") ||
               stderr.contains("Removing leading") {
                warn!("备份过程中检测到警告，但备份已完成: {}", stderr);
                if stderr.contains("Removing leading") {
                    info!("tar 正在移除路径前缀以创建相对路径，这是正常行为");
                } else {
                    info!("这通常是由于SQLite数据库正在使用中导致的，备份文件仍然有效");
                }
            } else {
                error!("tar 命令执行失败: stderr={}, stdout={}", stderr, stdout);
                return Err(anyhow::anyhow!("tar 命令执行失败: {}", stderr));
            }
        } else {
            // 即使成功也要检查是否有警告信息
            if !stderr.is_empty() && stderr.contains("Removing leading") {
                info!("tar 成功完成，警告信息: {}", stderr);
            }
        }

        // 验证备份文件是否创建成功
        if !backup_path.exists() {
            return Err(anyhow::anyhow!("备份文件未成功创建: {}", backup_path.display()));
        }

        info!("目录备份创建成功");
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

    /// 准备SQLite数据库备份
    async fn prepare_sqlite_backup(&self, dir_path: &Path) -> Result<()> {
        let db_path = dir_path.join("db.sqlite");
        
        if !db_path.exists() {
            return Ok(()); // 数据库不存在，跳过
        }

        info!("检测到SQLite数据库，尝试创建一致性快照");

        // 尝试使用sqlite3命令创建数据库备份
        let backup_path = dir_path.join("db.sqlite.backup");
        
        let output = tokio::process::Command::new("sqlite3")
            .arg(&db_path)
            .arg(&format!(".backup {}", backup_path.display()))
            .output()
            .await;

        match output {
            Ok(result) if result.status.success() => {
                info!("SQLite数据库快照创建成功");
                // 备份完成后，快照文件会被包含在tar备份中
            }
            Ok(result) => {
                let stderr = String::from_utf8_lossy(&result.stderr);
                warn!("SQLite快照创建失败，将使用常规备份: {}", stderr);
            }
            Err(e) => {
                warn!("无法执行sqlite3命令，将使用常规备份: {}", e);
            }
        }

        Ok(())
    }

    /// 手动触发备份
    pub async fn manual_backup(&self) -> Result<String> {
        if let Err(e) = self.perform_backup().await {
            let error_msg = format!("手动备份失败: {}", e);
            self.send_error_notification(&error_msg).await;
            return Err(e);
        }
        Ok("手动备份已完成".to_string())
    }

    /// 发送错误通知到 Telegram
    async fn send_error_notification(&self, error_message: &str) {
        // 转义特殊字符以避免 MarkdownV2 解析错误
        let escaped_error = error_message
            .replace("\\", "\\\\")
            .replace("*", "\\*")
            .replace("_", "\\_")
            .replace("[", "\\[")
            .replace("]", "\\]")
            .replace("(", "\\(")
            .replace(")", "\\)")
            .replace("`", "\\`")
            .replace(".", "\\.")
            .replace("-", "\\-")
            .replace("!", "\\!")
            .replace("+", "\\+")
            .replace("=", "\\=")
            .replace("{", "\\{")
            .replace("}", "\\}");
            
        let notification = format!(
            "❌ *备份错误通知*

🕒 时间: {}
📋 错误信息: {}

🔧 请检查系统日志获取详细信息",
            chrono::Utc::now().format("%Y\\--%m\\--%d %H:%M:%S UTC").to_string().replace("-", "\\-"),
            escaped_error
        );

        if let Err(e) = self.bot
            .send_message(self.config.target_chat_id, &notification)
            .parse_mode(ParseMode::MarkdownV2)
            .await
        {
            error!("发送错误通知失败: {}", e);
            // 如果 Markdown 失败，尝试发送纯文本
            let plain_notification = format!(
                "备份错误通知\n\n时间: {}\n错误信息: {}\n\n请检查系统日志获取详细信息",
                chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC"),
                error_message
            );
            if let Err(e) = self.bot
                .send_message(self.config.target_chat_id, &plain_notification)
                .await
            {
                error!("发送纯文本错误通知也失败: {}", e);
            }
        }
    }
}

/// 启动备份服务的辅助函数  
pub async fn start_backup_service(config: &crate::config::Config, bot: Bot) -> anyhow::Result<()> {
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