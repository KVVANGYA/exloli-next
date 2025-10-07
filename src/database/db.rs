use std::env;

use futures::executor::block_on;
use once_cell::sync::Lazy;
use sqlx::sqlite::*;
use sqlx::{Connection, Row};
use tracing::{info, warn, error};

pub static DB: Lazy<SqlitePool> = Lazy::new(|| {
    let url = env::var("DATABASE_URL").expect("数据库连接字符串未设置");
    block_on(get_connection_pool(&url))
});

pub async fn get_connection_pool(url: &str) -> SqlitePool {
    info!("初始化数据库连接：{}", url);
    
    // 尝试修复数据库
    if let Err(e) = try_repair_database(url).await {
        warn!("数据库修复失败: {}", e);
    }
    
    let options = SqliteConnectOptions::new()
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Normal)
        .foreign_keys(false)
        .filename(url)
        .create_if_missing(true)
        // 添加数据库恢复选项
        .pragma("integrity_check", "1")
        .pragma("quick_check", "1");
        
    let pool = SqlitePoolOptions::new()
        .max_connections(10)
        .connect_with(options).await.expect("数据库连接失败");
        
    info!("检查数据库迁移");
    sqlx::migrate!("./migrations").run(&pool).await.expect("数据库迁移失败");
    pool
}

async fn try_repair_database(url: &str) -> Result<(), sqlx::Error> {
    info!("检查数据库完整性：{}", url);
    
    let repair_options = SqliteConnectOptions::new()
        .filename(url)
        .journal_mode(SqliteJournalMode::Delete) // 临时使用DELETE模式进行修复
        .synchronous(SqliteSynchronous::Full);
    
    match SqliteConnection::connect_with(&repair_options).await {
        Ok(mut conn) => {
            // 尝试运行完整性检查
            match sqlx::query("PRAGMA integrity_check;")
                .fetch_one(&mut conn)
                .await {
                Ok(row) => {
                    let result: String = row.get(0);
                    if result != "ok" {
                        warn!("数据库完整性检查失败: {}", result);
                        
                        // 尝试修复
                        if let Err(e) = sqlx::query("VACUUM;").execute(&mut conn).await {
                            error!("VACUUM 失败: {}", e);
                        } else {
                            info!("数据库 VACUUM 完成");
                        }
                        
                        // 重建索引
                        if let Err(e) = sqlx::query("REINDEX;").execute(&mut conn).await {
                            error!("REINDEX 失败: {}", e);
                        } else {
                            info!("数据库索引重建完成");
                        }
                    } else {
                        info!("数据库完整性检查通过");
                    }
                }
                Err(e) => {
                    error!("数据库完整性检查失败: {}", e);
                    return Err(e);
                }
            }
            
            conn.close().await?;
            Ok(())
        }
        Err(e) => {
            error!("无法连接到数据库进行修复: {}", e);
            Err(e)
        }
    }
}
