use std::backtrace::Backtrace;
use std::time::Duration;

use anyhow::{anyhow, bail, Result};
use chrono::{Datelike, Utc};
use futures::{StreamExt, FutureExt};
use regex::Regex;
use reqwest::{Client, StatusCode};
use scraper::{Html, Selector};
use telegraph_rs::{html_to_node, Telegraph};
use teloxide::prelude::*;
use teloxide::types::MessageId;
use teloxide::utils::html::{code_inline, link};
use tokio::task::JoinHandle;
use tokio::time;
use tracing::{debug, error, info, warn, Instrument};

use crate::bot::Bot;
use crate::config::Config;
use crate::database::{
    GalleryEntity, ImageEntity, MessageEntity, PageEntity, PollEntity, TelegraphEntity,
};
use crate::ehentai::{EhClient, EhGallery, EhGalleryUrl, GalleryInfo};
use crate::s3::S3Uploader;
use crate::tags::EhTagTransDB;
use crate::utils::pad_left;

/// 带指数退避的重试机制
async fn retry_with_backoff<T, E, F, Fut>(max_retries: usize, mut func: F) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    let mut attempts = 0;
    loop {
        match func().await {
            Ok(result) => return Ok(result),
            Err(err) => {
                attempts += 1;
                if attempts >= max_retries {
                    error!("重试 {} 次后仍失败，放弃操作: {}", max_retries, err);
                    return Err(err);
                }
                
                // 根据重试次数调整延迟：第1次1s，第2次2s，第3次4s，第4次8s，第5次16s
                let delay = Duration::from_millis(1000 * (1 << (attempts - 1))); 
                warn!("操作失败 (尝试 {}/{}): {}, {}秒后重试", attempts, max_retries, err, delay.as_secs());
                time::sleep(delay).await;
            }
        }
    }
}

/// 改进的重试机制，针对网络错误提供更多重试次数
async fn retry_network_operation<T, E, F, Fut>(operation_name: &str, mut func: F) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    const MAX_RETRIES: usize = 7; // 网络操作给更多重试机会
    
    for attempt in 1..=MAX_RETRIES {
        match func().await {
            Ok(result) => {
                if attempt > 1 {
                    info!("{} 在第 {} 次尝试后成功", operation_name, attempt);
                }
                return Ok(result);
            }
            Err(err) => {
                if attempt >= MAX_RETRIES {
                    error!("{} 重试 {} 次后仍失败: {}", operation_name, MAX_RETRIES, err);
                    return Err(err);
                }
                
                // 更保守的退避策略：前几次快速重试，后面延迟增加
                let delay = if attempt <= 3 {
                    Duration::from_secs(attempt as u64) // 1s, 2s, 3s
                } else {
                    Duration::from_secs(5 + ((attempt - 3) * 5) as u64) // 10s, 15s, 20s, 25s
                };
                
                warn!("{} 失败 (尝试 {}/{}): {}, {}秒后重试", 
                      operation_name, attempt, MAX_RETRIES, err, delay.as_secs());
                time::sleep(delay).await;
            }
        }
    }
    
    unreachable!()
}

#[derive(Debug, Clone)]
pub struct UploadProgress {
    pub gallery_id: i32,
    pub total_pages: usize,
    pub downloaded_pages: usize,
    pub uploaded_pages: usize, 
    pub parsed_pages: usize,
}

#[derive(Debug, Clone)]
pub struct ExloliUploader {
    ehentai: EhClient,
    telegraph: Telegraph,
    bot: Bot,
    config: Config,
    trans: EhTagTransDB,
}

impl ExloliUploader {
    pub async fn new(
        config: Config,
        ehentai: EhClient,
        bot: Bot,
        trans: EhTagTransDB,
    ) -> Result<Self> {
        let telegraph = Telegraph::new(&config.telegraph.author_name)
            .author_url(&config.telegraph.author_url)
            .access_token(&config.telegraph.access_token)
            .create()
            .await?;
        Ok(Self { ehentai, config, telegraph, bot, trans })
    }

    /// 每隔 interval 分钟检查一次
    pub async fn start(&self) {
        info!("定时扫描任务已启动，扫描间隔: {:?}", self.config.interval);
        info!("搜索参数: {:?}", self.config.exhentai.search_params);
        info!("搜索数量: {}", self.config.exhentai.search_count);
        
        loop {
            let scan_start_time = std::time::Instant::now();
            info!("🔄 开始扫描 E 站本子 ({})", chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC"));
            
            // 添加 panic 捕获确保单次扫描失败不会终止循环
            let scan_result = std::panic::AssertUnwindSafe(async {
                self.check().await
            }).catch_unwind().await;
            
            match scan_result {
                Ok(()) => {
                    let scan_duration = scan_start_time.elapsed();
                    info!("✅ 扫描完毕，耗时 {:?}，等待 {:?} 后继续下次扫描", scan_duration, self.config.interval);
                }
                Err(panic_err) => {
                    let scan_duration = scan_start_time.elapsed();
                    error!("❌ 扫描过程中发生严重错误（panic），耗时 {:?}: {:?}", scan_duration, panic_err);
                    error!("将在 {:?} 后重试下次扫描", self.config.interval);
                }
            }
            
            info!("⏰ 下次扫描将在 {} 开始", 
                  (chrono::Utc::now() + chrono::Duration::from_std(self.config.interval).unwrap())
                  .format("%Y-%m-%d %H:%M:%S UTC"));
            time::sleep(self.config.interval).await;
        }
    }

    /// 根据配置文件，扫描前 N 个本子，并进行上传或者更新
    #[tracing::instrument(skip(self))]
    async fn check(&self) {
        // 添加整体错误捕获，确保扫描循环不会因为任何错误而中断
        let result = std::panic::AssertUnwindSafe(async {
            let stream = self
                .ehentai
                .search_iter(&self.config.exhentai.search_params)
                .take(self.config.exhentai.search_count);
            tokio::pin!(stream);
            
            let mut processed_count = 0;
            let mut error_count = 0;
            
            while let Some(next) = stream.next().await {
                processed_count += 1;
                info!("处理画廊 {}/{}: {}", processed_count, self.config.exhentai.search_count, next.url());
                
                // 错误不要上抛，避免影响后续画廊
                if let Err(err) = self.try_update(&next, true).await {
                    error_count += 1;
                    error!("check_and_update: {:?}\n{}", err, Backtrace::force_capture());
                }
                if let Err(err) = self.try_upload(&next, true).await {
                    error_count += 1;
                    error!("check_and_upload: {:?}\n{}", err, Backtrace::force_capture());
                    // 通知管理员上传失败 (但不让通知失败影响主流程)
                    if let Err(notify_err) = std::panic::AssertUnwindSafe(
                        self.notify_admins(&format!("画廊上传失败\n\nURL: {}\n错误: {}", next.url(), err))
                    ).catch_unwind().await {
                        error!("通知管理员失败: {:?}", notify_err);
                    }
                }
                time::sleep(Duration::from_secs(1)).await;
            }
            
            info!("扫描完成：处理了 {} 个画廊，遇到 {} 个错误", processed_count, error_count);
            Result::<()>::Ok(())
        });
        
        if let Err(panic_err) = std::panic::AssertUnwindSafe(result).catch_unwind().await {
            error!("扫描过程中发生严重错误（panic）: {:?}", panic_err);
            // 即使发生panic，也要让程序继续运行
        }
    }

    /// 检查指定画廊是否已经上传，如果没有则进行上传
    ///
    /// 为了避免绕晕自己，这次不考虑父子画廊，只要 id 不同就视为新画廊，只要是新画廊就进行上传
    #[tracing::instrument(skip(self))]
    pub async fn try_upload(&self, gallery: &EhGalleryUrl, check: bool) -> Result<()> {
        self.try_upload_with_progress(gallery, check, None::<fn(UploadProgress) -> std::future::Ready<()>>).await
    }

    /// 带进度回调的上传方法
    #[tracing::instrument(skip(self, progress_callback))]
    pub async fn try_upload_with_progress<F, Fut>(
        &self,
        gallery: &EhGalleryUrl,
        check: bool,
        progress_callback: Option<F>,
    ) -> Result<()>
    where
        F: Fn(UploadProgress) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        if check
            && GalleryEntity::check(gallery.id()).await?
            && MessageEntity::get_by_gallery(gallery.id()).await?.is_some()
        {
            return Ok(());
        }

        let gallery = self.ehentai.get_gallery(gallery).await?;
        // 上传图片、发布文章
        self.upload_gallery_image_with_progress(&gallery, check, progress_callback).await?;
        let article = self.publish_telegraph_article(&gallery).await?;
        // 发送消息
        let text = self.create_message_text(&gallery, &article.url).await?;
        // FIXME: 此处没有考虑到父画廊没有上传，但是父父画廊上传过的情况
        // 不过一般情况下画廊应该不会那么短时间内更新多次
        let msg = if let Some(parent) = &gallery.parent {
            if let Some(pmsg) = MessageEntity::get_by_gallery(parent.id()).await? {
                self.bot
                    .send_message(self.config.telegram.channel_id.clone(), text)
                    .reply_to_message_id(MessageId(pmsg.id))
                    .await?
            } else {
                self.bot.send_message(self.config.telegram.channel_id.clone(), text).await?
            }
        } else {
            self.bot.send_message(self.config.telegram.channel_id.clone(), text).await?
        };
        // 数据入库
        MessageEntity::create(msg.id.0, gallery.url.id()).await?;
        TelegraphEntity::create(gallery.url.id(), &article.url).await?;
        GalleryEntity::create(&gallery).await?;

        Ok(())
    }

    /// 检查指定画廊是否有更新，比如标题、标签
    #[tracing::instrument(skip(self))]
    pub async fn try_update(&self, gallery: &EhGalleryUrl, check: bool) -> Result<()> {
        let entity = match GalleryEntity::get(gallery.id()).await? {
            Some(v) => v,
            _ => return Ok(()),
        };
        let message = match MessageEntity::get_by_gallery(gallery.id()).await? {
            Some(v) => v,
            _ => return Ok(()),
        };

        // 2 天内创建的画廊，每天都尝试更新
        // 7 天内创建的画廊，每 3 天尝试更新
        // 14 天内创建的画廊，每 7 天尝试更新
        // 其余的，每 14 天尝试更新
        let now = Utc::now().date_naive();
        let seed = match now - message.publish_date {
            d if d < chrono::Duration::days(2) => 1,
            d if d < chrono::Duration::days(7) => 3,
            d if d < chrono::Duration::days(14) => 7,
            _ => 14,
        };
        if check && now.day() % seed != 0 {
            return Ok(());
        }

        // 检查 tag 和标题是否有变化
        let gallery = self.ehentai.get_gallery(gallery).await?;

        if gallery.tags != entity.tags.0 || gallery.title != entity.title {
            let telegraph = TelegraphEntity::get(gallery.url.id()).await?.unwrap();
            let text = self.create_message_text(&gallery, &telegraph.url).await?;
            self.bot
                .edit_message_text(
                    self.config.telegram.channel_id.clone(),
                    MessageId(message.id),
                    text,
                )
                .await?;
        }

        GalleryEntity::create(&gallery).await?;

        Ok(())
    }

    /// 重新发布指定画廊的文章，并更新消息
    pub async fn republish(&self, gallery: &GalleryEntity, msg: &MessageEntity) -> Result<()> {
        info!("重新发布：{}", msg.id);
        let article = self.publish_telegraph_article(gallery).await?;
        let text = self.create_message_text(gallery, &article.url).await?;
        self.bot
            .edit_message_text(self.config.telegram.channel_id.clone(), MessageId(msg.id), text)
            .await?;
        TelegraphEntity::update(gallery.id, &article.url).await?;
        Ok(())
    }

    /// 检查 telegraph 文章是否正常
    pub async fn check_telegraph(&self, url: &str) -> Result<bool> {
        Ok(Client::new().head(url).send().await?.status() != StatusCode::NOT_FOUND)
    }
}

impl ExloliUploader {
    /// 获取某个画廊里的所有图片，并且上传到 telegrpah，如果已经上传过的，会跳过上传
    async fn upload_gallery_image(&self, gallery: &EhGallery) -> Result<()> {
        self.upload_gallery_image_with_progress(gallery, true, None::<fn(UploadProgress) -> std::future::Ready<()>>).await
    }

    /// 带进度回调的图片上传方法
    async fn upload_gallery_image_with_progress<F, Fut>(
        &self,
        gallery: &EhGallery,
        check: bool,
        progress_callback: Option<F>,
    ) -> Result<()>
    where
        F: Fn(UploadProgress) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        // 扫描所有图片
        // 对于已经上传过的图片，不需要重复上传，只需要插入 PageEntity 记录即可
        let mut pages = vec![];
        let mut already_uploaded = 0;
        for page in &gallery.pages {
            if check {
                // 只有在check=true时才进行hash检查来去重
                match ImageEntity::get_by_hash(page.hash()).await? {
                    Some(img) => {
                        // NOTE: 此处存在重复插入的可能，但是由于 PageEntity::create 使用 OR IGNORE，所以不影响
                        PageEntity::create(page.gallery_id(), page.page(), img.id).await?;
                        already_uploaded += 1;
                    }
                    None => pages.push(page.clone()),
                }
            } else {
                // check=false时，强制重新下载所有图片，跳过hash检查
                pages.push(page.clone());
            }
        }
        info!("需要下载&上传 {} 张图片，已存在 {} 张", pages.len(), already_uploaded);

        if pages.is_empty() {
            // 如果所有图片都已经上传过，触发一次完成回调
            if let Some(callback) = progress_callback {
                let final_progress = UploadProgress {
                    gallery_id: gallery.url.id(),
                    total_pages: gallery.pages.len(),
                    downloaded_pages: already_uploaded,
                    uploaded_pages: already_uploaded,
                    parsed_pages: already_uploaded,
                };
                callback(final_progress).await;
            }
            return Ok(());
        }

        let concurrent = self.config.threads_num;
        // 使用更合理的通道容量：并发数的2倍，避免内存占用过高
        let (parse_tx, parse_rx) = tokio::sync::mpsc::channel(concurrent * 2);
        let client = self.ehentai.clone();
        
        // 进度跟踪变量
        use std::sync::Arc;
        use tokio::sync::Mutex;
        let progress = Arc::new(Mutex::new(UploadProgress {
            gallery_id: gallery.url.id(),
            total_pages: gallery.pages.len(),
            downloaded_pages: already_uploaded,
            uploaded_pages: already_uploaded,
            parsed_pages: already_uploaded,
        }));
        
        let callback_arc = progress_callback.map(Arc::new);
        
        // 获取图片链接时不要并行，避免触发反爬限制
        let progress_clone_parser = progress.clone();
        let callback_clone_parser = callback_arc.clone();
        let getter = tokio::spawn(
            async move {
                for page in pages {
                    let rst = client.get_image_url(&page).await?;
                    info!("已解析：{}", page.page());
                    
                    // 更新解析进度
                    if let Some(ref callback) = callback_clone_parser {
                        let mut prog = progress_clone_parser.lock().await;
                        prog.parsed_pages += 1;
                        callback(prog.clone()).await;
                    }
                    
                    // 如果发送失败（通道关闭），提前退出
                    if parse_tx.send((page, rst)).await.is_err() {
                        break;
                    }
                }
                drop(parse_tx); // 关闭发送端，让接收端知道没有更多数据
                Result::<()>::Ok(())
            }
            .in_current_span(),
        );

        // 创建下载上传的并发任务
        let s3 = S3Uploader::new(
            self.config.ipfs.gateway_host.clone(),
            self.config.ipfs.gateway_date.clone(),
            self.config.ipfs.teletype_token.clone(),
        )?;
        
        // 使用 Semaphore 来控制并发数量
        let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrent));
        let mut upload_handles = vec![];
        let parse_rx = Arc::new(tokio::sync::Mutex::new(parse_rx));
        
        // 启动并发的下载上传任务
        for _ in 0..concurrent {
            let sem = semaphore.clone();
            let rx = parse_rx.clone();
            let progress_clone = progress.clone();
            let callback_clone = callback_arc.clone();
            let s3_clone = s3.clone();
            
            let client = Client::builder()
                .timeout(Duration::from_secs(30))
                .connect_timeout(Duration::from_secs(30))
                .build()?;
            
            let handle = tokio::spawn(
                async move {
                    loop {
                        // 获取下一个任务
                        let task = {
                            let mut rx_guard = rx.lock().await;
                            rx_guard.recv().await
                        };
                        
                        let (page, (fileindex, original_url)) = match task {
                            Some(data) => data,
                            None => break, // 没有更多任务
                        };
                        
                        // 获取信号量许可，控制并发
                        let _permit = sem.acquire().await.unwrap();

                        // 首先尝试获取预览图URL作为备选方案
                        let preview_url = match client.get(&page.url()).send().await {
                            Ok(response) => {
                                let html_text = response.text().await.unwrap_or_default();
                                let html = Html::parse_document(&html_text);
                                let selector = Selector::parse("img#img").unwrap();
                                html.select(&selector)
                                    .next()
                                    .and_then(|ele| ele.value().attr("src"))
                                    .map(|s| s.to_string())
                            }
                            Err(_) => None,
                        };

                        let suffix = original_url.split('.').last().unwrap_or("jpg");

                        // 先获取 Content-Length 检查文件大小
                        let should_compress = match client.head(&original_url).send().await {
                            Ok(response) => {
                                if let Some(content_length) = response.headers().get("content-length") {
                                    if let Ok(size_str) = content_length.to_str() {
                                        if let Ok(size) = size_str.parse::<usize>() {
                                            let should_compress = size > 2_000_000; // 2MB
                                            if should_compress {
                                                debug!("图片 {} 大小 {} bytes，使用 WebP 压缩", page.page(), size);
                                            }
                                            should_compress
                                        } else { false }
                                    } else { false }
                                } else { false }
                            }
                            Err(_) => false, // HEAD 失败则不压缩
                        };

                        // 根据文件大小决定是否使用 WebP 压缩
                        // 使用 ll 参数启用无损压缩
                        let (download_url, filename, use_compressed) = if should_compress {
                            let webp_url = format!("https://images.weserv.nl/?url={}&output=webp&n=-1&w=2560&h=2560",
                                urlencoding::encode(&original_url));
                            (webp_url, format!("{}.webp", page.hash()), true)
                        } else {
                            (original_url.clone(), format!("{}.{}", page.hash(), suffix), false)
                        };

                        // 下载图片（带网络重试机制和内容验证）
                        let bytes = retry_network_operation(
                            &format!("下载图片 {}", page.page()),
                            || async {
                                let response = client.get(&download_url).send().await?;
                                
                                // 检查Content-Type
                                if let Some(content_type) = response.headers().get("content-type") {
                                    if let Ok(ct_str) = content_type.to_str() {
                                        if !ct_str.starts_with("image/") && !ct_str.contains("octet-stream") {
                                            return Err(anyhow!("响应不是图片类型，Content-Type: {}", ct_str));
                                        }
                                    }
                                }
                                
                                let bytes = response.bytes().await?;
                                
                                // 检查内容是否为HTML页面或JSON错误响应
                                if bytes.len() > 10 {
                                    let content_start = String::from_utf8_lossy(&bytes[..std::cmp::min(200, bytes.len())]);
                                    let content_lower = content_start.trim_start().to_lowercase();
                                    if content_lower.starts_with("<!doctype html") || 
                                       content_lower.starts_with("<html") ||
                                       content_lower.contains("<title>") ||
                                       content_lower.contains("<form") {
                                        return Err(anyhow!("下载到的是HTML页面而不是图片"));
                                    }
                                    // 检查是否为images.weserv.nl的JSON错误响应
                                    if (content_lower.starts_with("{") && content_lower.contains("\"status\"") && content_lower.contains("\"error\"")) ||
                                       (content_lower.starts_with("{") && content_lower.contains("\"code\"") && content_lower.contains("\"message\"")) {
                                        return Err(anyhow!("images.weserv.nl返回错误响应: {}", content_start));
                                    }
                                }
                                
                                Ok(bytes)
                            }
                        ).await.map_err(|e| {
                            error!("下载图片失败 {}: {}", page.page(), e);
                            e
                        }).ok();

                        // 如果压缩图片下载失败，尝试使用预览图
                        let (final_bytes, final_filename, used_preview) = if bytes.is_none() && preview_url.is_some() {
                            let preview = preview_url.unwrap();
                            info!("原图下载失败，使用预览图作为备用方案: {}", preview);
                            match retry_network_operation(
                                &format!("下载预览图 {}", page.page()),
                                || async {
                                    let response = client.get(&preview).send().await?;
                                    
                                    // 检查Content-Type
                                    if let Some(content_type) = response.headers().get("content-type") {
                                        if let Ok(ct_str) = content_type.to_str() {
                                            if !ct_str.starts_with("image/") && !ct_str.contains("octet-stream") {
                                                return Err(anyhow!("预览图响应不是图片类型，Content-Type: {}", ct_str));
                                            }
                                        }
                                    }
                                    
                                    let bytes = response.bytes().await?;
                                    
                                    // 检查内容是否为HTML页面或JSON错误响应
                                    if bytes.len() > 10 {
                                        let content_start = String::from_utf8_lossy(&bytes[..std::cmp::min(200, bytes.len())]);
                                        let content_lower = content_start.trim_start().to_lowercase();
                                        if content_lower.starts_with("<!doctype html") || 
                                           content_lower.starts_with("<html") ||
                                           content_lower.contains("<title>") ||
                                           content_lower.contains("<form") {
                                            return Err(anyhow!("下载到的是HTML页面而不是预览图"));
                                        }
                                        // 检查是否为JSON错误响应
                                        if (content_lower.starts_with("{") && content_lower.contains("\"status\"") && content_lower.contains("\"error\"")) ||
                                           (content_lower.starts_with("{") && content_lower.contains("\"code\"") && content_lower.contains("\"message\"")) {
                                            return Err(anyhow!("预览图服务返回错误响应: {}", content_start));
                                        }
                                    }
                                    
                                    Ok(bytes)
                                }
                            ).await {
                                Ok(preview_bytes) => {
                                    let preview_suffix = preview.split('.').last().unwrap_or("jpg");
                                    (Some(preview_bytes), format!("{}_preview.{}", page.hash(), preview_suffix), true)
                                },
                                Err(e) => {
                                    error!("下载预览图也失败 {}: {}", page.page(), e);
                                    // 即使预览图失败，也不要让整个画廊失败，而是跳过这张图片
                                    warn!("跳过图片 {} (原图和预览图都下载失败)", page.page());
                                    continue;
                                }
                            }
                        } else if let Some(b) = bytes {
                            (Some(b), filename, false)
                        } else if preview_url.is_some() {
                            // 如果原图失败但有预览图，尝试预览图
                            let preview = preview_url.unwrap();
                            info!("原图下载失败，尝试预览图: {}", preview);
                            match retry_network_operation(
                                &format!("下载预览图 {}", page.page()),
                                || async {
                                    let response = client.get(&preview).send().await?;
                                    
                                    // 检查Content-Type
                                    if let Some(content_type) = response.headers().get("content-type") {
                                        if let Ok(ct_str) = content_type.to_str() {
                                            if !ct_str.starts_with("image/") && !ct_str.contains("octet-stream") {
                                                return Err(anyhow!("预览图响应不是图片类型，Content-Type: {}", ct_str));
                                            }
                                        }
                                    }
                                    
                                    let bytes = response.bytes().await?;
                                    
                                    // 检查内容是否为HTML页面或JSON错误响应
                                    if bytes.len() > 10 {
                                        let content_start = String::from_utf8_lossy(&bytes[..std::cmp::min(200, bytes.len())]);
                                        let content_lower = content_start.trim_start().to_lowercase();
                                        if content_lower.starts_with("<!doctype html") || 
                                           content_lower.starts_with("<html") ||
                                           content_lower.contains("<title>") ||
                                           content_lower.contains("<form") {
                                            return Err(anyhow!("下载到的是HTML页面而不是预览图"));
                                        }
                                        // 检查是否为JSON错误响应
                                        if (content_lower.starts_with("{") && content_lower.contains("\"status\"") && content_lower.contains("\"error\"")) ||
                                           (content_lower.starts_with("{") && content_lower.contains("\"code\"") && content_lower.contains("\"message\"")) {
                                            return Err(anyhow!("预览图服务返回错误响应: {}", content_start));
                                        }
                                    }
                                    
                                    Ok(bytes)
                                }
                            ).await {
                                Ok(preview_bytes) => {
                                    let preview_suffix = preview.split('.').last().unwrap_or("jpg");
                                    (Some(preview_bytes), format!("{}_preview.{}", page.hash(), preview_suffix), true)
                                },
                                Err(e) => {
                                    error!("下载预览图失败 {}: {}", page.page(), e);
                                    warn!("跳过图片 {} (原图和预览图都下载失败)", page.page());
                                    continue;
                                }
                            }
                        } else {
                            error!("图片 {} 没有任何可用的下载源", page.page());
                            warn!("跳过图片 {} (无可用下载源)", page.page());
                            continue;
                        };

                        let bytes = final_bytes.unwrap();
                        debug!("已下载: {} ({}, {} bytes) {}", page.page(),
                            if used_preview { "预览图" } else if use_compressed { "WebP" } else { suffix },
                            bytes.len(),
                            if used_preview { "（备选方案）" } else { "" });

                        // 更新下载进度
                        if let Some(ref callback) = callback_clone {
                            let mut prog = progress_clone.lock().await;
                            prog.downloaded_pages += 1;
                            callback(prog.clone()).await;
                        }

                        // 上传到 S3（带网络重试机制）
                        let upload_url = match retry_network_operation(
                            &format!("上传图片 {}", page.page()), 
                            || async {
                                s3_clone.upload(&final_filename, &mut bytes.as_ref()).await
                            }
                        ).await {
                            Ok(url) => url,
                            Err(e) => {
                                error!("上传图片失败 {} (多次重试后仍失败): {}", page.page(), e);
                                return Err(anyhow!("上传图片失败 {} (多次重试后仍失败): {}", page.page(), e));
                            }
                        };
                        debug!("已上传: {} {}", page.page(), if used_preview { "（预览图）" } else { "" });

                        // 更新上传进度
                        if let Some(ref callback) = callback_clone {
                            let mut prog = progress_clone.lock().await;
                            prog.uploaded_pages += 1;
                            callback(prog.clone()).await;
                        }

                        // 保存到数据库
                        if let Err(e) = ImageEntity::create(fileindex, page.hash(), &upload_url).await {
                            error!("保存图片记录失败 {}: {}", page.page(), e);
                            return Err(anyhow!("保存图片记录失败 {}: {}", page.page(), e));
                        }
                        if let Err(e) = PageEntity::create(page.gallery_id(), page.page(), fileindex).await {
                            error!("保存页面记录失败 {}: {}", page.page(), e);
                            return Err(anyhow!("保存页面记录失败 {}: {}", page.page(), e));
                        }
                    }
                    Result::<()>::Ok(())
                }
                .in_current_span(),
            );
            upload_handles.push(handle);
        }

        // 等待解析任务完成
        let parse_result = flatten(getter).await;
        
        // 等待所有上传任务完成
        let mut upload_results = vec![];
        for handle in upload_handles {
            upload_results.push(flatten(handle).await);
        }
        
        // 检查是否有任务失败
        parse_result?;
        for result in upload_results {
            result?;
        }

        Ok(())
    }

    /// 从数据库中读取某个画廊的所有图片，生成一篇 telegraph 文章
    /// 为了防止画廊被删除后无法更新，此处不应该依赖 EhGallery
    /// 如果图片数量过多，会创建多个分页文章并返回第一页
    async fn publish_telegraph_article<T: GalleryInfo>(
        &self,
        gallery: &T,
    ) -> Result<telegraph_rs::Page> {
        let images = ImageEntity::get_by_gallery_id(gallery.url().id()).await?;
        
        // Telegraph 单页最大图片数量限制 (约50KB内容限制，每个img标签约100-200字节)
        const MAX_IMAGES_PER_PAGE: usize = 200;
        
        if images.len() <= MAX_IMAGES_PER_PAGE {
            // 图片数量不多，创建单页文章
            return self.create_single_telegraph_page(gallery, &images).await;
        }
        
        // 图片数量过多，需要分页处理
        info!("画廊 {} 有 {} 张图片，需要分页处理", gallery.url().id(), images.len());
        
        let total_pages = (images.len() + MAX_IMAGES_PER_PAGE - 1) / MAX_IMAGES_PER_PAGE;
        let mut created_pages: Vec<telegraph_rs::Page> = Vec::new();
        
        for (page_idx, image_chunk) in images.chunks(MAX_IMAGES_PER_PAGE).enumerate() {
            let page_num = page_idx + 1;
            let page_title = if total_pages > 1 {
                format!("{} (第{}页/共{}页)", gallery.title_jp(), page_num, total_pages)
            } else {
                gallery.title_jp()
            };
            
            let mut html = String::new();
            
            // 只在第一页显示封面
            if page_idx == 0 && gallery.cover() != 0 && gallery.cover() < images.len() {
                html.push_str(&format!(r#"<img src="{}">"#, images[gallery.cover()].url()));
            }
            
            // 添加分页导航（除第一页外）
            if page_idx > 0 && !created_pages.is_empty() {
                html.push_str(&format!(r#"<p><a href="{}">← 返回第1页</a></p>"#, created_pages[0].url));
            }
            
            // 添加当前页图片
            for img in image_chunk {
                html.push_str(&format!(r#"<img src="{}">"#, img.url()));
            }
            
            // 添加页面信息和导航
            if total_pages > 1 {
                html.push_str(&format!("<p>第{}页/共{}页 (图片总数：{})</p>", page_num, total_pages, images.len()));
                
                // 添加下一页链接占位符（将在创建下一页后更新）
                if page_idx < total_pages - 1 {
                    html.push_str("<p>下一页链接将在创建后添加</p>");
                }
            } else {
                html.push_str(&format!("<p>图片总数：{}</p>", gallery.pages()));
            }
            
            let node = html_to_node(&html);
            let page = self.telegraph.create_page(&page_title, &node, false).await?;
            
            debug!("创建Telegraph分页 {}/{}：{}", page_num, total_pages, page.url);
            created_pages.push(page);
        }
        
        // 更新页面间的导航链接
        if created_pages.len() > 1 {
            for (idx, page) in created_pages.iter().enumerate() {
                let page_num = idx + 1;
                let mut html = String::new();
                
                // 重新构建HTML内容，添加正确的导航链接
                if idx == 0 && gallery.cover() != 0 && gallery.cover() < images.len() {
                    html.push_str(&format!(r#"<img src="{}">"#, images[gallery.cover()].url()));
                }
                
                // 导航链接
                let mut nav_links = Vec::new();
                if idx > 0 {
                    nav_links.push(format!(r#"<a href="{}">← 上一页</a>"#, created_pages[idx - 1].url));
                }
                if idx == 0 && created_pages.len() > 1 {
                    nav_links.push(format!(r#"<a href="{}">下一页 →</a>"#, created_pages[1].url));
                }
                if idx < created_pages.len() - 1 && idx > 0 {
                    nav_links.push(format!(r#"<a href="{}">下一页 →</a>"#, created_pages[idx + 1].url));
                }
                
                if !nav_links.is_empty() {
                    html.push_str(&format!("<p>{}</p>", nav_links.join(" | ")));
                }
                
                // 添加当前页图片
                let start_idx = idx * MAX_IMAGES_PER_PAGE;
                let end_idx = std::cmp::min(start_idx + MAX_IMAGES_PER_PAGE, images.len());
                for img in &images[start_idx..end_idx] {
                    html.push_str(&format!(r#"<img src="{}">"#, img.url()));
                }
                
                // 页面信息
                html.push_str(&format!("<p>第{}页/共{}页 (图片总数：{})</p>", page_num, created_pages.len(), images.len()));
                
                // 底部导航
                if !nav_links.is_empty() {
                    html.push_str(&format!("<p>{}</p>", nav_links.join(" | ")));
                }
                
                let node = html_to_node(&html);
                let page_title = if created_pages.len() > 1 {
                    format!("{} (第{}页/共{}页)", gallery.title_jp(), page_num, created_pages.len())
                } else {
                    gallery.title_jp()
                };
                
                // 更新页面内容
                if let Err(e) = self.telegraph.edit_page(&page.path, &page_title, &node, false).await {
                    error!("更新Telegraph分页 {} 导航失败: {}", page_num, e);
                }
            }
        }
        
        // 返回第一页
        Ok(created_pages.into_iter().next().unwrap())
    }
    
    /// 创建单页Telegraph文章
    async fn create_single_telegraph_page<T: GalleryInfo>(
        &self,
        gallery: &T,
        images: &[ImageEntity],
    ) -> Result<telegraph_rs::Page> {
        let mut html = String::new();
        if gallery.cover() != 0 && gallery.cover() < images.len() {
            html.push_str(&format!(r#"<img src="{}">"#, images[gallery.cover()].url()))
        }
        for img in images {
            html.push_str(&format!(r#"<img src="{}">"#, img.url()));
        }
        html.push_str(&format!("<p>图片总数：{}</p>", gallery.pages()));

        let node = html_to_node(&html);
        let title = gallery.title_jp();
        Ok(self.telegraph.create_page(&title, &node, false).await?)
    }

    /// 为画廊生成一条可供发送的 telegram 消息正文
    async fn create_message_text<T: GalleryInfo>(
        &self,
        gallery: &T,
        article: &str,
    ) -> Result<String> {
        // 首先，将 tag 翻译
        // 并整理成 namespace: #tag1 #tag2 #tag3 的格式
        let re = Regex::new("[-/· ]").unwrap();
        let tags = self.trans.trans_tags(gallery.tags());
        let mut text = String::new();
        for (ns, tag) in tags {
            let tag = tag
                .iter()
                .map(|s| format!("#{}", re.replace_all(s, "_")))
                .collect::<Vec<_>>()
                .join(" ");
            text.push_str(&format!("{}: {}\n", code_inline(&pad_left(&ns, 6)), tag))
        }

        text.push_str(
            &format!("{}: {}\n", code_inline("  预览"), link(article, &gallery.title()),),
        );
        text.push_str(&format!("{}: {}", code_inline("原始地址"), gallery.url().url()));

        Ok(text)
    }

    /// 通知所有管理员
    async fn notify_admins(&self, message: &str) {
        for user_id in &self.config.telegram.trusted_users {
            if let Ok(chat_id) = user_id.parse::<i64>() {
                let result = self.bot.send_message(ChatId(chat_id), message).await;
                if let Err(e) = result {
                    error!("向管理员 {} 发送通知失败: {}", user_id, e);
                }
            }
        }
    }
}

async fn flatten<T>(handle: JoinHandle<Result<T>>) -> Result<T> {
    match handle.await {
        Ok(Ok(result)) => Ok(result),
        Ok(Err(err)) => Err(err),
        Err(err) => bail!(err),
    }
}

impl ExloliUploader {
    /// 重新扫描并上传没有上传过但存在记录的画廊
    pub async fn reupload(&self, mut galleries: Vec<GalleryEntity>) -> Result<()> {
        if galleries.is_empty() {
            galleries = GalleryEntity::list_scans().await?;
        }
        for gallery in galleries.iter().rev() {
            if let Some(score) = PollEntity::get_by_gallery(gallery.id).await? {
                if score.score > 0.8 {
                    info!("尝试上传画廊：{}", gallery.url());
                    if let Err(err) = self.try_upload(&gallery.url(), true).await {
                        error!("上传失败：{}", err);
                    }
                    time::sleep(Duration::from_secs(60)).await;
                }
            }
        }
        Ok(())
    }

    /// 重新检测已上传过的画廊预览是否有效，并重新上传
    pub async fn recheck(&self, mut galleries: Vec<GalleryEntity>) -> Result<()> {
        if galleries.is_empty() {
            galleries = GalleryEntity::list_scans().await?;
        }
        for gallery in galleries.iter().rev() {
            let telegraph =
                TelegraphEntity::get(gallery.id).await?.ok_or(anyhow!("找不到 telegraph"))?;
            if let Some(msg) = MessageEntity::get_by_gallery(gallery.id).await? {
                info!("检测画廊：{}", gallery.url());
                if !self.check_telegraph(&telegraph.url).await? {
                    info!("重新上传预览：{}", gallery.url());
                    if let Err(err) = self.republish(gallery, &msg).await {
                        error!("上传失败：{}", err);
                    }
                    time::sleep(Duration::from_secs(60)).await;
                }
            }
            time::sleep(Duration::from_secs(1)).await;
        }
        Ok(())
    }
}
