use std::backtrace::Backtrace;
use std::time::Duration;

use anyhow::{anyhow, bail, Result};
use chrono::{Datelike, Utc};
use futures::StreamExt;
use image::ImageFormat;
use regex::Regex;
use reqwest::{Client, StatusCode};
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
use crate::ehentai::{EhClient, EhGallery, EhGalleryUrl, EhPageUrl, GalleryInfo};
use crate::s3::S3Uploader;
use crate::tags::EhTagTransDB;
use crate::utils::pad_left;

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
        loop {
            info!("开始扫描 E 站 本子");
            self.check().await;
            info!("扫描完毕，等待 {:?} 后继续", self.config.interval);
            time::sleep(self.config.interval).await;
        }
    }

    /// 根据配置文件，扫描前 N 个本子，并进行上传或者更新
    #[tracing::instrument(skip(self))]
    async fn check(&self) {
        let stream = self
            .ehentai
            .search_iter(&self.config.exhentai.search_params)
            .take(self.config.exhentai.search_count);
        tokio::pin!(stream);
        while let Some(next) = stream.next().await {
            // 错误不要上抛，避免影响后续画廊
            if let Err(err) = self.try_update(&next, true).await {
                error!("check_and_update: {:?}\n{}", err, Backtrace::force_capture());
            }
            if let Err(err) = self.try_upload(&next, true).await {
                error!("check_and_upload: {:?}\n{}", err, Backtrace::force_capture());
                // 通知管理员上传失败
                self.notify_admins(&format!("画廊上传失败\n\nURL: {}\n错误: {}", next.url(), err)).await;
            }
            time::sleep(Duration::from_secs(1)).await;
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
        self.upload_gallery_image_with_progress(&gallery, progress_callback).await?;
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
        self.upload_gallery_image_with_progress(gallery, None::<fn(UploadProgress) -> std::future::Ready<()>>).await
    }

    /// 带进度回调的图片上传方法
    async fn upload_gallery_image_with_progress<F, Fut>(
        &self,
        gallery: &EhGallery,
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
            match ImageEntity::get_by_hash(page.hash()).await? {
                Some(img) => {
                    // NOTE: 此处存在重复插入的可能，但是由于 PageEntity::create 使用 OR IGNORE，所以不影响
                    PageEntity::create(page.gallery_id(), page.page(), img.id).await?;
                    already_uploaded += 1;
                }
                None => pages.push(page.clone()),
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
                .timeout(self.config.api_timeout)
                .connect_timeout(self.config.api_timeout)
                .build()?;
            
            let handle = tokio::spawn(
                async move {
                    loop {
                        // 获取下一个任务
                        let task = {
                            let mut rx_guard = rx.lock().await;
                            rx_guard.recv().await
                        };
                        
                        let (page, (fileindex, url)) = match task {
                            Some(data) => data,
                            None => break, // 没有更多任务
                        };
                        
                        // 获取信号量许可，控制并发
                        let _permit = sem.acquire().await.unwrap();

                        let suffix = url.split('.').last().unwrap_or("jpg");

                        // 先获取 Content-Length 检查文件大小
                        let (should_compress, original_file_size) = match client.head(&url).send().await {
                            Ok(response) => {
                                if let Some(content_length) = response.headers().get("content-length") {
                                    if let Ok(size_str) = content_length.to_str() {
                                        if let Ok(size) = size_str.parse::<usize>() {
                                            let should_compress = size > 2_000_000; // 超过 2MB
                                            debug!("图片 {} HEAD 请求成功，大小 {} bytes", page.page(), size);
                                            if should_compress {
                                                debug!("图片 {} 大小 {} bytes，使用 WebP 压缩", page.page(), size);
                                            }
                                            (should_compress, size)
                                        } else { 
                                            debug!("图片 {} HEAD 请求 Content-Length 解析失败: {}", page.page(), size_str);
                                            (false, 0)
                                        }
                                    } else { 
                                        debug!("图片 {} HEAD 请求 Content-Length 转换字符串失败", page.page());
                                        (false, 0)
                                    }
                                } else { 
                                    debug!("图片 {} HEAD 请求没有 Content-Length 头", page.page());
                                    (false, 0)
                                }
                            }
                            Err(e) => {
                                debug!("图片 {} HEAD 请求失败: {}", page.page(), e);
                                (false, 0) // HEAD 失败则不压缩
                            }
                        };

                        // 根据文件大小决定是否使用 WebP 压缩
                        let (download_url, mut filename) = if should_compress {
                            // 使用images.weserv.nl作为主要API，因为它对JPG支持无损压缩不会导致504
                            let webp_url = format!("https://images.weserv.nl/?url={}&output=webp&ll&n=-1",
                                urlencoding::encode(&url));
                            debug!("使用images.weserv.nl WebP无损压缩服务下载图片: {}", webp_url);
                            (webp_url, format!("{}.webp", page.hash()))
                        } else {
                            (url.clone(), format!("{}.{}", page.hash(), suffix))
                        };

                        // 下载图片
                        debug!("正在请求下载: {}", download_url);
                        let mut request_failed = false; // 标记网络请求是否失败
                        let mut bytes = match client.get(&download_url).send().await {
                            Ok(response) => {
                                let status = response.status();
                                debug!("图片 {} 响应状态: {}", page.page(), status);
                                
                                // 检查响应状态码
                                if !status.is_success() {
                                    if download_url.contains("hath.network") {
                                        error!("H@H 节点返回错误状态 {}: 所属的H@H节点异常，请稍后重试 (URL: {})", status, download_url);
                                        return Err(anyhow!("所属的H@H节点异常，请稍后重试 (状态码: {}, URL: {})", status, download_url));
                                    } else if status.as_u16() == 504 && should_compress {
                                        // 504 Gateway Timeout，可能是无损压缩导致的超时，尝试有损压缩质量100
                                        warn!("压缩服务返回504超时，尝试有损压缩质量100重试: {}", download_url);
                                        
                                        let fallback_url = if download_url.contains("images.weserv.nl") {
                                            format!("https://wsrv.nl/?url={}&output=webp&q=100&n=-1",
                                                urlencoding::encode(&url))
                                        } else {
                                            format!("https://images.weserv.nl/?url={}&output=webp&q=100&n=-1",
                                                urlencoding::encode(&url))
                                        };
                                        
                                        debug!("504超时重试URL: {}", fallback_url);
                                        // 立即重试质量100的压缩
                                        match client.get(&fallback_url).send().await {
                                            Ok(retry_resp) => {
                                                if retry_resp.status().is_success() {
                                                    match retry_resp.bytes().await {
                                                        Ok(retry_bytes) => {
                                                            debug!("504重试成功: {} bytes", retry_bytes.len());
                                                            retry_bytes
                                                        }
                                                        Err(e) => {
                                                            warn!("504重试读取响应失败: {}，进入后续处理", e);
                                                            vec![].into() // 让后续逻辑处理
                                                        }
                                                    }
                                                } else {
                                                    warn!("504重试仍然失败，状态码: {}，进入后续处理", retry_resp.status());
                                                    vec![].into() // 让后续逻辑处理
                                                }
                                            }
                                            Err(e) => {
                                                warn!("504重试请求失败: {}，进入后续处理", e);
                                                vec![].into() // 让后续逻辑处理
                                            }
                                        }
                                    } else {
                                        error!("请求失败，状态码: {} (URL: {})", status, download_url);
                                        return Err(anyhow!("请求失败，状态码: {} (URL: {})", status, download_url));
                                    }
                                } else {
                                    match response.bytes().await {
                                        Ok(bytes) => {
                                            debug!("图片 {} 下载完成: {} bytes", page.page(), bytes.len());
                                            
                                            // 检查是否是 H@H 错误响应（通常很小且包含错误信息）
                                            if download_url.contains("hath.network") && bytes.len() < 100 {
                                                let content = String::from_utf8_lossy(&bytes);
                                                warn!("H@H 节点可能返回错误响应: {} bytes, 内容: {:?}", bytes.len(), content);
                                                if content.contains("error") || content.contains("Error") || bytes.len() < 50 {
                                                    error!("所属的H@H节点异常，请稍后重试 (状态码: {}, URL: {})", status, download_url);
                                                    return Err(anyhow!("所属的H@H节点异常，请稍后重试 (状态码: {}, URL: {})", status, download_url));
                                                }
                                            }
                                            
                                            bytes
                                        },
                                        Err(e) => {
                                            error!("下载图片失败 {}: {}", page.page(), e);
                                            return Err(anyhow!("下载图片失败 {}: {}", page.page(), e));
                                        }
                                    }
                                }
                            },
                            Err(e) => {
                                warn!("主压缩API请求失败 {}: {}，尝试备用方案", page.page(), e);
                                // 请求失败时，设置空bytes并标记失败
                                request_failed = true;
                                vec![].into()
                            }
                        };

                        // 检查 WebP 压缩结果或网络请求失败
                        // 如果压缩成功但文件变大，检查是否应该使用原图
                        let mut use_original_instead = false;
                        if should_compress && !request_failed && bytes.len() >= 1000 && bytes.len() <= 4_900_000 {
                            // 压缩成功且大小合理，但需要检查是否比原图更大
                            if original_file_size > 0 && bytes.len() > original_file_size && original_file_size <= 4_900_000 {
                                warn!("WebP压缩后文件变大 ({} bytes -> {} bytes)，且原图 < 4.9MB，使用原图", original_file_size, bytes.len());
                                use_original_instead = true;
                            }
                        }
                        
                        if use_original_instead {
                            // 下载原图
                            debug!("下载原图替代压缩版本: {}", url);
                            bytes = match client.get(&url).send().await {
                                Ok(response) => {
                                    if response.status().is_success() {
                                        match response.bytes().await {
                                            Ok(original_bytes) => {
                                                debug!("原图下载成功: {} bytes", original_bytes.len());
                                                filename = format!("{}.{}", page.hash(), suffix);
                                                original_bytes
                                            },
                                            Err(e) => {
                                                warn!("原图下载失败: {}，继续使用压缩版本", e);
                                                bytes // 保持压缩版本
                                            }
                                        }
                                    } else {
                                        warn!("原图请求失败: {}，继续使用压缩版本", response.status());
                                        bytes // 保持压缩版本
                                    }
                                },
                                Err(e) => {
                                    warn!("原图请求失败: {}，继续使用压缩版本", e);
                                    bytes // 保持压缩版本
                                }
                            };
                        } else if should_compress && (request_failed || bytes.len() < 1000 || bytes.len() > 4_900_000) {
                            if request_failed {
                                warn!("images.weserv.nl 网络请求失败，尝试备用API");
                            } else if bytes.len() < 1000 {
                                warn!("images.weserv.nl 无损压缩失败（文件太小: {} bytes），尝试备用API", bytes.len());
                            } else {
                                warn!("images.weserv.nl 无损压缩过大（{} bytes > 4.9MB），尝试有损压缩", bytes.len());
                            }
                            
                            // 根据失败原因选择处理方式
                            let (service_url, service_name) = if request_failed {
                                // 网络请求失败，直接使用备用API wsrv.nl
                                let backup_url = format!("https://wsrv.nl/?url={}&output=webp&ll&n=-1",
                                    urlencoding::encode(&url));
                                debug!("网络请求失败，尝试备用 API wsrv.nl 无损: {}", backup_url);
                                (backup_url, "wsrv.nl 无损")
                            } else if bytes.len() > 4_900_000 {
                                // 文件过大，先尝试images.weserv.nl有损压缩
                                let images_lossy_url = format!("https://images.weserv.nl/?url={}&output=webp&q=95&n=-1",
                                    urlencoding::encode(&url));
                                debug!("尝试 images.weserv.nl 有损压缩: {}", images_lossy_url);
                                (images_lossy_url, "images.weserv.nl 有损")
                            } else {
                                // 文件太小（可能是错误），尝试备用API wsrv.nl
                                let backup_url = format!("https://wsrv.nl/?url={}&output=webp&ll&n=-1",
                                    urlencoding::encode(&url));
                                debug!("尝试备用 API wsrv.nl 无损: {}", backup_url);
                                (backup_url, "wsrv.nl 无损")
                            };
                            debug!("正在请求: {}", service_url);
                            
                            bytes = match client.get(&service_url).send().await {
                                Ok(response) => match response.bytes().await {
                                    Ok(b) if b.len() >= 1000 && b.len() <= 4_900_000 => {
                                        debug!("{} 成功: {} bytes", service_name, b.len());
                                        b
                                    },
                                    Ok(b) => {
                                        if b.len() < 1000 {
                                            warn!("{} 失败（文件太小: {} bytes）", service_name, b.len());
                                        } else {
                                            warn!("{} 仍然过大（{} bytes > 4.9MB）", service_name, b.len());
                                        }
                                        
                                        // 根据服务名称决定下一步处理
                                        if service_name == "images.weserv.nl 有损" {
                                            warn!("images.weserv.nl 有损压缩失败，尝试备用API wsrv.nl");
                                            // images.weserv.nl 有损都失败了，备用API也用有损
                                            let backup_url = format!("https://wsrv.nl/?url={}&output=webp&q=95&n=-1",
                                                urlencoding::encode(&url));
                                            debug!("尝试备用 API wsrv.nl 有损: {}", backup_url);
                                            
                                            match client.get(&backup_url).send().await {
                                                Ok(backup_resp) => match backup_resp.bytes().await {
                                                    Ok(backup_bytes) if backup_bytes.len() >= 1000 && backup_bytes.len() <= 4_900_000 => {
                                                        debug!("wsrv.nl 有损压缩成功: {} bytes", backup_bytes.len());
                                                        backup_bytes
                                                    },
                                                    Ok(backup_bytes) => {
                                                        warn!("wsrv.nl 也失败（{} bytes），尝试本地转码", backup_bytes.len());
                                                        Self::try_local_transcode(&client, &url, &page, &mut filename, suffix).await?.into()
                                                    },
                                                    Err(e) => {
                                                        warn!("wsrv.nl 请求失败: {}，尝试本地转码", e);
                                                        Self::try_local_transcode(&client, &url, &page, &mut filename, suffix).await?.into()
                                                    }
                                                },
                                                Err(e) => {
                                                    warn!("wsrv.nl 连接失败: {}，尝试本地转码", e);
                                                    Self::try_local_transcode(&client, &url, &page, &mut filename, suffix).await?.into()
                                                }
                                            }
                                        } else if service_name == "wsrv.nl 无损" {
                                            warn!("wsrv.nl 无损压缩失败，尝试 wsrv.nl 有损压缩");
                                            // wsrv.nl 无损失败，尝试有损
                                            let backup_url = format!("https://wsrv.nl/?url={}&output=webp&q=95&n=-1",
                                                urlencoding::encode(&url));
                                            debug!("尝试 wsrv.nl 有损: {}", backup_url);
                                            
                                            match client.get(&backup_url).send().await {
                                                Ok(backup_resp) => match backup_resp.bytes().await {
                                                    Ok(backup_bytes) if backup_bytes.len() >= 1000 && backup_bytes.len() <= 4_900_000 => {
                                                        debug!("wsrv.nl 有损压缩成功: {} bytes", backup_bytes.len());
                                                        backup_bytes
                                                    },
                                                    Ok(backup_bytes) => {
                                                        warn!("wsrv.nl 也失败（{} bytes），尝试本地转码", backup_bytes.len());
                                                        // 进入本地转码逻辑
                                                        Self::try_local_transcode(&client, &url, &page, &mut filename, suffix).await?.into()
                                                    },
                                                    Err(e) => {
                                                        warn!("wsrv.nl 请求失败: {}，尝试本地转码", e);
                                                        Self::try_local_transcode(&client, &url, &page, &mut filename, suffix).await?.into()
                                                    }
                                                },
                                                Err(e) => {
                                                    warn!("wsrv.nl 连接失败: {}，尝试本地转码", e);
                                                    Self::try_local_transcode(&client, &url, &page, &mut filename, suffix).await?.into()
                                                }
                                            }
                                        } else if service_name == "images.weserv.nl 质量100" {
                                            warn!("images.weserv.nl JPG质量100压缩失败，尝试质量95压缩");
                                            // JPG质量100失败，尝试质量95
                                            let backup_url = format!("https://images.weserv.nl/?url={}&output=webp&q=95&n=-1",
                                                urlencoding::encode(&url));
                                            debug!("尝试 images.weserv.nl JPG质量95: {}", backup_url);
                                            
                                            match client.get(&backup_url).send().await {
                                                Ok(backup_resp) => match backup_resp.bytes().await {
                                                    Ok(backup_bytes) if backup_bytes.len() >= 1000 && backup_bytes.len() <= 4_900_000 => {
                                                        debug!("wsrv.nl 有损压缩成功: {} bytes", backup_bytes.len());
                                                        backup_bytes
                                                    },
                                                    Ok(backup_bytes) => {
                                                        warn!("wsrv.nl 也失败（{} bytes），尝试本地转码", backup_bytes.len());
                                                        // 进入本地转码逻辑
                                                        Self::try_local_transcode(&client, &url, &page, &mut filename, suffix).await?.into()
                                                    },
                                                    Err(e) => {
                                                        warn!("wsrv.nl 请求失败: {}，尝试本地转码", e);
                                                        Self::try_local_transcode(&client, &url, &page, &mut filename, suffix).await?.into()
                                                    }
                                                },
                                                Err(e) => {
                                                    warn!("wsrv.nl 连接失败: {}，尝试本地转码", e);
                                                    Self::try_local_transcode(&client, &url, &page, &mut filename, suffix).await?.into()
                                                }
                                            }
                                        } else {
                                            // 不是 wsrv.nl 有损压缩失败，直接进入本地转码
                                            Self::try_local_transcode(&client, &url, &page, &mut filename, suffix).await?.into()
                                        }
                                    },
                                    Err(e) => {
                                        warn!("{} 请求失败: {}，尝试本地转码", service_name, e);
                                        Self::try_local_transcode(&client, &url, &page, &mut filename, suffix).await?.into()
                                    }
                                },
                                Err(e) => {
                                    warn!("{} 连接失败: {}，尝试本地转码", service_name, e);
                                    Self::try_local_transcode(&client, &url, &page, &mut filename, suffix).await?.into()
                                }
                            };
                        } else if should_compress {
                            debug!("已下载: {} (WebP无损, {} bytes)", page.page(), bytes.len());
                        } else {
                            debug!("已下载: {} ({}, {} bytes)", page.page(), suffix, bytes.len());
                        }

                        // 更新下载进度
                        if let Some(ref callback) = callback_clone {
                            let mut prog = progress_clone.lock().await;
                            prog.downloaded_pages += 1;
                            callback(prog.clone()).await;
                        }

                        // 上传到 S3
                        let upload_url = match s3_clone.upload(&filename, &mut bytes.as_ref()).await {
                            Ok(url) => url,
                            Err(e) => {
                                warn!("上传图片失败 {}: {}，尝试备用上传方案", page.page(), e);
                                
                                // 如果原本没有进行WebP压缩，现在尝试本地转码后上传
                                if !should_compress && suffix != "webp" {
                                    debug!("尝试本地WebP转码后重新上传，原图URL: {}", url);
                                    match Self::try_local_transcode(&client, &url, &page, &mut filename, suffix).await {
                                        Ok(transcoded_bytes) => {
                                            match s3_clone.upload(&filename, &mut transcoded_bytes.as_ref()).await {
                                                Ok(final_url) => {
                                                    debug!("备用上传成功: {}", page.page());
                                                    final_url
                                                },
                                                Err(backup_e) => {
                                                    error!("备用上传也失败 {}: {}", page.page(), backup_e);
                                                    return Err(anyhow!("上传图片失败 {}: {}", page.page(), e));
                                                }
                                            }
                                        },
                                        Err(transcode_e) => {
                                            error!("本地转码失败 {}: {}，无法进行备用上传", page.page(), transcode_e);
                                            return Err(anyhow!("上传图片失败 {}: {}", page.page(), e));
                                        }
                                    }
                                } else {
                                    error!("上传图片失败 {}: {}", page.page(), e);
                                    return Err(anyhow!("上传图片失败 {}: {}", page.page(), e));
                                }
                            }
                        };
                        debug!("已上传: {}", page.page());

                        // 立即写入数据库，防止后续错误导致重复处理
                        // 优先创建 ImageEntity（按 hash 去重的关键记录）
                        match ImageEntity::create(fileindex, page.hash(), &upload_url).await {
                            Ok(_) => {
                                debug!("图片记录保存成功: {}", page.page());
                                // 成功创建图片记录后，再创建页面记录
                                if let Err(e) = PageEntity::create(page.gallery_id(), page.page(), fileindex).await {
                                    error!("保存页面记录失败 {}: {}，但图片记录已保存", page.page(), e);
                                }
                            },
                            Err(e) => {
                                error!("保存图片记录失败 {}: {}，这可能导致重复下载", page.page(), e);
                                // 图片记录保存失败，仍然尝试保存页面记录
                                if let Err(pe) = PageEntity::create(page.gallery_id(), page.page(), fileindex).await {
                                    error!("保存页面记录也失败 {}: {}", page.page(), pe);
                                }
                            }
                        }

                        // 更新上传进度
                        if let Some(ref callback) = callback_clone {
                            let mut prog = progress_clone.lock().await;
                            prog.uploaded_pages += 1;
                            callback(prog.clone()).await;
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
    async fn publish_telegraph_article<T: GalleryInfo>(
        &self,
        gallery: &T,
    ) -> Result<telegraph_rs::Page> {
        let images = ImageEntity::get_by_gallery_id(gallery.url().id()).await?;

        let mut html = String::new();
        if gallery.cover() != 0 && gallery.cover() < images.len() {
            html.push_str(&format!(r#"<img src="{}">"#, images[gallery.cover()].url()))
        }
        for img in images {
            html.push_str(&format!(r#"<img src="{}">"#, img.url()));
        }
        html.push_str(&format!("<p>图片总数：{}</p>", gallery.pages()));

        let node = html_to_node(&html);
        // 文章标题优先使用日文
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
    /// 检查下载的文件是否超过大小限制
    /// 如果超过限制，尝试用本地转码来压缩
    async fn check_and_compress_if_needed(
        _client: &Client, 
        _original_url: &str, 
        downloaded_bytes: Vec<u8>, 
        _page: &EhPageUrl, 
        max_size: usize
    ) -> Result<(Vec<u8>, bool)> {
        if downloaded_bytes.len() <= max_size {
            debug!("文件大小合适: {} bytes", downloaded_bytes.len());
            return Ok((downloaded_bytes, false)); // 不是本地转码的WebP
        }
        
        warn!("文件过大: {} bytes > {} bytes，尝试本地压缩", downloaded_bytes.len(), max_size);
        
        // 使用智能压缩
        match Self::smart_webp_compression(&downloaded_bytes, max_size, 85.0, true).await {
            Ok(compressed_bytes) => {
                if compressed_bytes.len() <= max_size {
                    debug!("本地压缩成功: {} bytes -> {} bytes", downloaded_bytes.len(), compressed_bytes.len());
                    Ok((compressed_bytes, true)) // 是本地转码的WebP
                } else {
                    warn!("本地压缩后仍超限制，使用原文件");
                    Ok((downloaded_bytes, false))
                }
            },
            Err(e) => {
                warn!("本地压缩失败: {}，使用原文件", e);
                Ok((downloaded_bytes, false))
            }
        }
    }

    /// 尝试本地转码的辅助方法
    async fn try_local_transcode(
        client: &Client,
        url: &str,
        page: &EhPageUrl,
        filename: &mut String,
        suffix: &str,
    ) -> Result<Vec<u8>> {
        debug!("开始本地转码，原图URL: {}", url);
        match Self::convert_to_webp_locally_static(client, url, page).await {
            Ok((converted_bytes, is_webp)) => {
                debug!("本地转码成功: {} bytes ({})", converted_bytes.len(), 
                       if is_webp { "WebP" } else { "原图" });
                *filename = if is_webp {
                    format!("{}.webp", page.hash())
                } else {
                    format!("{}.{}", page.hash(), suffix)
                };
                Ok(converted_bytes)
            },
            Err(e) => {
                warn!("本地转码也失败: {}，最终降级使用原图", e);
                // 最终降级使用原图
                *filename = format!("{}.{}", page.hash(), suffix);
                match client.get(url).send().await {
                    Ok(resp) => match resp.bytes().await {
                        Ok(orig_bytes) => Ok(orig_bytes.to_vec()),
                        Err(e) => Err(anyhow!("下载原图失败: {}", e))
                    },
                    Err(e) => Err(anyhow!("请求原图失败: {}", e))
                }
            }
        }
    }

    /// 检查文件大小并进行智能 WebP 压缩
    /// lossy: true=有损压缩, false=无损压缩
    async fn smart_webp_compression(original_bytes: &[u8], _max_size: usize, quality: f32, lossy: bool) -> Result<Vec<u8>> {
        let compression_type = if lossy { "有损" } else { "无损" };
        debug!("开始 WebP {} 压缩: {} bytes", compression_type, original_bytes.len());
        
        // 检测图片格式
        let format = image::guess_format(original_bytes)
            .map_err(|e| anyhow!("无法识别图片格式: {}", e))?;
            
        // 如果已经是 WebP，直接返回
        if format == ImageFormat::WebP {
            debug!("图片已经是 WebP 格式，直接返回");
            return Ok(original_bytes.to_vec());
        }
        
        // 解码原图
        let img = image::load_from_memory(original_bytes)
            .map_err(|e| anyhow!("解码图片失败: {}", e))?;
        
        let mut webp_bytes = Vec::new();
        
        if lossy && quality > 0.0 {
            // TODO: 有损压缩支持（当前 image crate 可能不支持，降级到无损）
            warn!("有损 WebP 压缩暂不支持，降级使用无损压缩");
            let encoder = image::codecs::webp::WebPEncoder::new_lossless(&mut webp_bytes);
            encoder.encode(&img.to_rgba8(), img.width(), img.height(), image::ColorType::Rgba8.into())
                .map_err(|e| anyhow!("WebP 编码失败: {}", e))?;
        } else {
            // 使用无损压缩
            debug!("使用无损 WebP 压缩");
            let encoder = image::codecs::webp::WebPEncoder::new_lossless(&mut webp_bytes);
            encoder.encode(&img.to_rgba8(), img.width(), img.height(), image::ColorType::Rgba8.into())
                .map_err(|e| anyhow!("WebP 编码失败: {}", e))?;
        }
        
        debug!("WebP {} 压缩完成: {} bytes -> {} bytes", compression_type, original_bytes.len(), webp_bytes.len());
        Ok(webp_bytes)
    }

    /// 静态方法：本地 WebP 转码
    /// 返回 (数据, 是否为WebP格式)
    async fn convert_to_webp_locally_static(client: &Client, url: &str, page: &EhPageUrl) -> Result<(Vec<u8>, bool)> {
        debug!("开始本地 WebP 转码: 页面 {}", page.page());
        
        // 下载原图
        let response = client.get(url).send().await
            .map_err(|e| anyhow!("下载原图失败: {}", e))?;
        let original_bytes = response.bytes().await
            .map_err(|e| anyhow!("读取原图数据失败: {}", e))?;
            
        debug!("原图下载完成: {} bytes", original_bytes.len());
        
        const MAX_FILE_SIZE: usize = 4_900_000; // 4.9MB
        
        // 先尝试无损 WebP 压缩
        match Self::smart_webp_compression(&original_bytes, MAX_FILE_SIZE, 0.0, false).await {
            Ok(lossless_bytes) => {
                debug!("本地无损 WebP 转码完成: {} bytes -> {} bytes", 
                       original_bytes.len(), lossless_bytes.len());
                
                // 检查无损压缩结果
                if lossless_bytes.len() <= MAX_FILE_SIZE && lossless_bytes.len() < original_bytes.len() {
                    debug!("本地无损压缩成功且符合大小限制");
                    Ok((lossless_bytes, true)) // 返回无损WebP
                } else if lossless_bytes.len() > MAX_FILE_SIZE {
                    warn!("本地无损压缩过大 ({} bytes > 4.9MB)，尝试有损压缩", lossless_bytes.len());
                    
                    // 尝试有损压缩
                    match Self::smart_webp_compression(&original_bytes, MAX_FILE_SIZE, 95.0, true).await {
                        Ok(lossy_bytes) => {
                            debug!("本地有损 WebP 转码完成: {} bytes -> {} bytes", 
                                   original_bytes.len(), lossy_bytes.len());
                            
                            if lossy_bytes.len() <= MAX_FILE_SIZE && lossy_bytes.len() < original_bytes.len() {
                                debug!("本地有损压缩成功且符合大小限制");
                                Ok((lossy_bytes, true)) // 返回有损WebP
                            } else {
                                error!("本地有损压缩仍超限制或变大 ({} bytes)，转码失败", lossy_bytes.len());
                                Err(anyhow!("本地有损压缩失败，文件大小仍不符合要求: {} bytes", lossy_bytes.len()))
                            }
                        },
                        Err(e) => {
                            error!("本地有损转码失败: {}，转码终止", e);
                            Err(anyhow!("本地有损压缩失败: {}", e))
                        }
                    }
                } else {
                    warn!("本地无损压缩后文件变大 ({} -> {} bytes)，尝试有损压缩", 
                          original_bytes.len(), lossless_bytes.len());
                    
                    // 无损压缩变大，尝试有损压缩
                    match Self::smart_webp_compression(&original_bytes, MAX_FILE_SIZE, 95.0, true).await {
                        Ok(lossy_bytes) => {
                            debug!("本地有损 WebP 转码完成: {} bytes -> {} bytes", 
                                   original_bytes.len(), lossy_bytes.len());
                            
                            if lossy_bytes.len() <= MAX_FILE_SIZE && lossy_bytes.len() < original_bytes.len() {
                                debug!("本地有损压缩成功且符合大小限制");
                                Ok((lossy_bytes, true)) // 返回有损WebP
                            } else {
                                error!("本地有损压缩仍不理想 ({} bytes)，转码失败", lossy_bytes.len());
                                Err(anyhow!("本地压缩失败，无法满足大小要求"))
                            }
                        },
                        Err(e) => {
                            error!("本地有损转码失败: {}，转码终止", e);
                            Err(anyhow!("本地有损压缩失败: {}", e))
                        }
                    }
                }
            },
            Err(e) => {
                warn!("本地无损转码失败: {}，尝试有损压缩", e);
                
                // 无损失败，尝试有损
                match Self::smart_webp_compression(&original_bytes, MAX_FILE_SIZE, 95.0, true).await {
                    Ok(lossy_bytes) => {
                        debug!("本地有损 WebP 转码完成: {} bytes -> {} bytes", 
                               original_bytes.len(), lossy_bytes.len());
                        
                        if lossy_bytes.len() <= MAX_FILE_SIZE && lossy_bytes.len() < original_bytes.len() {
                            debug!("本地有损压缩成功");
                            Ok((lossy_bytes, true)) // 返回有损WebP
                        } else {
                            error!("本地有损压缩也不理想 ({} bytes)，转码失败", lossy_bytes.len());
                            Err(anyhow!("本地有损压缩失败，无法满足大小要求: {} bytes", lossy_bytes.len()))
                        }
                    },
                    Err(e2) => {
                        error!("本地有损转码也失败: {}，转码终止", e2);
                        Err(anyhow!("本地有损压缩失败: {}", e2))
                    }
                }
            }
        }
    }

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
