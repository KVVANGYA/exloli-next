use std::env;

use anyhow::Result;
use exloli_next::bot::start_dispatcher;
use exloli_next::config::{Config, CHANNEL_ID};
use exloli_next::ehentai::EhClient;
use exloli_next::tags::EhTagTransDB;
use exloli_next::uploader::ExloliUploader;
use exloli_next::backup::start_backup_service;
use futures::FutureExt;
use teloxide::prelude::*;
use teloxide::types::ParseMode;

#[tokio::main]
async fn main() -> Result<()> {
    if env::var("IS_DAEMON_CHILD").is_ok() {
        // 作为守护进程的子进程运行
        run_app().await
    } else {
        // 作为守护进程本身运行
        exloli_next::daemon::start_daemon(env::current_exe()?.to_str().unwrap()).await
    }
}

pub async fn run_app() -> Result<()> {
    let config = Config::new("./config.toml")?;
    CHANNEL_ID.set(config.telegram.channel_id.to_string()).unwrap();

    // NOTE: 全局数据库连接需要用这个变量初始化
    env::set_var("DATABASE_URL", &config.database_url);
    env::set_var("RUST_LOG", &config.log_level);

    tracing_subscriber::FmtSubscriber::builder()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init()
        .unwrap();

    let trans = EhTagTransDB::new(&config.exhentai.trans_file);
    let ehentai = EhClient::new(&config.exhentai.cookie).await?;
    let bot = Bot::new(&config.telegram.token)
        .throttle(Default::default())
        .parse_mode(ParseMode::Html)
        .cache_me();
    let uploader =
        ExloliUploader::new(config.clone(), ehentai.clone(), bot.clone(), trans.clone()).await?;

    // 启动备份服务（独立任务，不阻塞主程序）
    let backup_config = config.backup.clone();
    let backup_bot = bot.clone();
    tokio::spawn(async move {
        loop {
            match start_backup_service(&backup_config, backup_bot.clone()).await {
                Ok(_) => {
                    tracing::error!("备份服务意外退出，5秒后重新启动");
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                }
                Err(e) => {
                    tracing::error!("启动备份服务失败: {}, 30秒后重试", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                }
            }
        }
    });

    let t1 = {
        let uploader = uploader.clone();
        tokio::spawn(async move { 
            // 定时扫描任务永不退出，即使发生错误也要继续运行
            loop {
                if let Err(e) = std::panic::AssertUnwindSafe(uploader.start()).catch_unwind().await {
                    tracing::error!("定时扫描任务发生严重错误（panic），重新启动: {:?}", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
                    continue;
                }
                // 如果start()正常退出（这不应该发生），重新启动
                tracing::error!("定时扫描任务意外退出，重新启动");
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            }
        })
    };
    let t2 = {
        let trans = trans.clone();
        tokio::spawn(async move { 
            // Bot调度器任务，如果失败尝试重启
            loop {
                // start_dispatcher 永不返回，如果返回则说明出错了
                start_dispatcher(config.clone(), uploader.clone(), bot.clone(), trans.clone()).await;
                tracing::error!("Bot调度器意外退出，5秒后重新启动");
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            }
        })
    };
    let t3 = tokio::spawn(async move { 
        // 标签更新任务，如果失败尝试重启
        loop {
            if let Err(e) = std::panic::AssertUnwindSafe(trans.start()).catch_unwind().await {
                tracing::error!("标签更新任务发生严重错误（panic），重新启动: {:?}", e);
                tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                continue;
            }
            // 如果start()正常退出，重新启动
            tracing::error!("标签更新任务意外退出，重新启动");
            tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
        }
    });

    // 使用 join! 而不是 try_join!，这样即使某个任务失败也不会导致程序退出
    // 由于每个任务都有自己的重启机制，这些任务应该永不退出
    let (r1, r2, r3) = tokio::join!(t1, t2, t3);
    
    // 记录任务意外退出（这应该永远不会发生）
    if let Err(e) = r1 {
        tracing::error!("定时扫描任务线程异常退出: {}", e);
    }
    if let Err(e) = r2 {
        tracing::error!("Bot调度器任务线程异常退出: {}", e);
    }
    if let Err(e) = r3 {
        tracing::error!("标签更新任务线程异常退出: {}", e);
    }

    Ok(())
}
