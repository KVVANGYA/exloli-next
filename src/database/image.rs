use sqlx::sqlite::SqliteQueryResult;
use sqlx::Result;
use tracing::Level;

use super::db::DB;

#[derive(sqlx::FromRow, Debug)]
pub struct PageEntity {
    /// 画廊 ID
    pub gallery_id: i32,
    /// 页面编号
    pub page: i32,
    /// 图片 id
    pub image_id: i32,
}

#[derive(sqlx::FromRow, Debug)]
pub struct ImageEntity {
    /// 图片的自增 ID
    pub id: i32,
    /// 图片哈希
    pub hash: String,
    /// 相对 https://telegra.ph 的图片 URL
    pub url: String,
}

impl ImageEntity {
    /// 创建一条记录
    #[tracing::instrument(level = Level::DEBUG)]
    pub async fn create(hash: &str, url: &str) -> Result<Self> {
        sqlx::query_as("INSERT INTO image (hash, url) VALUES (?, ?) RETURNING *")
            .bind(hash)
            .bind(url)
            .fetch_one(&*DB)
            .await
    }

    /// 根据图片 hash 获取一张图片
    #[tracing::instrument(level = Level::DEBUG)]
    pub async fn get_by_hash(hash: &str) -> Result<Option<ImageEntity>> {
        sqlx::query_as("SELECT * FROM image WHERE hash = ?").bind(hash).fetch_optional(&*DB).await
    }

    /// 从指定画廊里随机获取一张图片
    ///
    /// 画廊要求：
    /// - 分数大于 80
    /// - 作者只有 1 位
    #[tracing::instrument(level = Level::DEBUG)]
    pub async fn get_challenge() -> Result<Option<Self>> {
        sqlx::query_as(
            r#"SELECT * FROM image
            JOIN page ON page.image_id = image.id
            JOIN gallery ON gallery.id = image.gallery_id
            JOIN poll ON poll.gallery_id = gallery.id
            WHERE poll.score > 0.8
              AND JSON_ARRAY_LENGTH(JSON_EXTRACT(gallery.tags, '$.artist')) = 1
            ORDER BY RANDOM()
            LIMIT 1"#,
        )
        .fetch_optional(&*DB)
        .await
    }

    /// 获取指定画廊的所有图片，并且按页码排列
    #[tracing::instrument(level = Level::DEBUG)]
    pub async fn get_by_gallery_id(gallery_id: i32) -> Result<Vec<Self>> {
        sqlx::query_as("SELECT * FROM image JOIN page ON page.image_id = image.id WHERE page.gallery_id = ? ORDER BY page.page")
            .bind(gallery_id)
            .fetch_all(&*DB)
            .await
    }
}

impl PageEntity {
    /// 创建一条记录，有冲突时则忽略
    #[tracing::instrument(level = Level::DEBUG)]
    pub async fn create(gallery_id: i32, page: i32, image_id: i32) -> Result<SqliteQueryResult> {
        sqlx::query("INSERT OR IGNORE INTO page (gallery_id, page, image_id) VALUES (?, ?, ?)")
            .bind(gallery_id)
            .bind(page)
            .bind(image_id)
            .execute(&*DB)
            .await
    }
}
