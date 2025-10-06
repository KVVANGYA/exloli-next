use std::backtrace::Backtrace;
use std::time::Duration;

use anyhow::{anyhow, bail, Result};
use chrono::{Datelike, Utc};
use futures::StreamExt;
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
use crate::ehentai::{EhClient, EhGallery, EhGalleryUrl, GalleryInfo};
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
                        
                        let (page, (fileindex, url)) = match task {
                            Some(data) => data,
                            None => break, // 没有更多任务
                        };
                        
                        // 获取信号量许可，控制并发
                        let _permit = sem.acquire().await.unwrap();

                        let suffix = url.split('.').last().unwrap_or("jpg");

                        // 先获取 Content-Length 检查文件大小
                        let should_compress = match client.head(&url).send().await {
                            Ok(response) => {
                                if let Some(content_length) = response.headers().get("content-length") {
                                    if let Ok(size_str) = content_length.to_str() {
                                        if let Ok(size) = size_str.parse::<usize>() {
                                            let should_compress = size > 1_000_000; // 超过 1MB
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
                        // 使用 ll 参数启用无损压缩，&n=-1 保留 GIF 所有帧
                        let (download_url, mut filename) = if should_compress {
                            let webp_url = format!("https://wsrv.nl/?url={}&output=webp&ll&n=-1",
                                urlencoding::encode(&url));
                            (webp_url, format!("{}.webp", page.hash()))
                        } else {
                            (url.clone(), format!("{}.{}", page.hash(), suffix))
                        };

                        // 下载图片
                        let mut bytes = match client.get(&download_url).send().await {
                            Ok(response) => match response.bytes().await {
                                Ok(bytes) => bytes,
                                Err(e) => {
                                    error!("下载图片失败 {}: {}", page.page(), e);
                                    return Err(anyhow!("下载图片失败 {}: {}", page.page(), e));
                                }
                            },
                            Err(e) => {
                                error!("请求图片失败 {}: {}", page.page(), e);
                                return Err(anyhow!("请求图片失败 {}: {}", page.page(), e));
                            }
                        };

                        // 检查 WebP 压缩是否失败（文件太小说明是错误页面）
                        if should_compress && bytes.len() < 1000 {
                            warn!("WebP 压缩失败（文件太小: {} bytes），降级使用原图", bytes.len());
                            // 降级使用原图
                            filename = format!("{}.{}", page.hash(), suffix);
                            bytes = match client.get(&url).send().await {
                                Ok(response) => match response.bytes().await {
                                    Ok(b) => b,
                                    Err(e) => {
                                        error!("下载原图失败 {}: {}", page.page(), e);
                                        return Err(anyhow!("下载原图失败 {}: {}", page.page(), e));
                                    }
                                },
                                Err(e) => {
                                    error!("请求原图失败 {}: {}", page.page(), e);
                                    return Err(anyhow!("请求原图失败 {}: {}", page.page(), e));
                                }
                            };
                            debug!("已下载: {} (原图降级, {} bytes)", page.page(), bytes.len());
                        } else {
                            debug!("已下载: {} ({}, {} bytes)", page.page(),
                                if should_compress { "WebP" } else { suffix },
                                bytes.len());
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
                                error!("上传图片失败 {}: {}", page.page(), e);
                                return Err(anyhow!("上传图片失败 {}: {}", page.page(), e));
                            }
                        };
                        debug!("已上传: {}", page.page());

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
