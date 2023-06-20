use std::backtrace::Backtrace;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use chrono::{Datelike, Utc};
use futures::{stream, StreamExt};
use regex::Regex;
use reqwest::Client;
use telegraph_rs::{html_to_node, Telegraph};
use teloxide::prelude::Requester;
use teloxide::types::MessageId;
use teloxide::utils::html::{code_inline, link};
use tokio::time;
use tracing::{debug, error, info, Instrument};

use crate::bot::Bot;
use crate::config::Config;
use crate::database::{GalleryEntity, ImageEntity, MessageEntity, PageEntity};
use crate::ehentai::{EhClient, EhGallery, EhGalleryUrl, GalleryInfo};
use crate::utils::imagebytes::ImageBytes;
use crate::utils::pad_left;
use crate::utils::tags::EhTagTransDB;

#[derive(Debug, Clone)]
pub struct ExloliUploader {
    ehentai: EhClient,
    telegraph: Telegraph,
    bot: Bot,
    config: Config,
    trans: Arc<EhTagTransDB>,
}

impl ExloliUploader {
    pub async fn new(config: Config, ehentai: EhClient, bot: Bot) -> Result<Self> {
        let telegraph = Telegraph::new(&config.telegraph.author_name)
            .author_url(&config.telegraph.author_url)
            .access_token(&config.telegraph.access_token)
            .create()
            .await?;
        let trans = Arc::new(EhTagTransDB::new(&config.exhentai.trans_file));
        Ok(Self { ehentai, config, telegraph, bot, trans })
    }

    /// 每隔 interval 分钟检查一次
    pub async fn start(&self) {
        loop {
            self.check().await;
            time::sleep(Duration::from_secs(self.config.interval * 60)).await;
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
            if let Err(err) = self.check_and_update(&next).await {
                error!("check_and_update: {:?}\n{}", err, Backtrace::force_capture());
            }
            if let Err(err) = self.check_and_upload(&next).await {
                error!("check_and_upload: {:?}\n{}", err, Backtrace::force_capture());
            }
        }
    }

    /// 检查指定画廊是否已经上传，如果没有则进行上传
    ///
    /// 为了避免绕晕自己，这次不考虑父子画廊，只要 id 不同就视为新画廊，只要是新画廊就进行上传
    #[tracing::instrument(skip(self))]
    pub async fn check_and_upload(&self, gallery: &EhGalleryUrl) -> Result<()> {
        if GalleryEntity::check(gallery.id()).await? {
            return Ok(());
        }

        let gallery = self.ehentai.get_gallery(gallery).await?;
        // 上传图片、发布文章
        self.upload_gallery_image(&gallery).await?;
        let article = self.publish_telegraph_article(&gallery).await?;
        // 发送消息
        let text = self.create_message_text(&gallery, &article.url).await?;
        let msg = self.bot.send_message(self.config.telegram.channel_id.clone(), text).await?;
        // 数据入库
        MessageEntity::create(msg.id.0, gallery.url.id(), &article.url).await?;
        GalleryEntity::create(
            gallery.url.id(),
            gallery.url.token(),
            &gallery.title,
            &gallery.title_jp,
            &gallery.tags,
            gallery.pages.len() as i32,
            gallery.parent.map(|u| u.id()),
        )
        .await?;

        Ok(())
    }

    /// 检查指定画廊是否有更新，比如标题、标签
    #[tracing::instrument(skip(self))]
    pub async fn check_and_update(&self, gallery: &EhGalleryUrl) -> Result<()> {
        let entity = match GalleryEntity::get(gallery.id()).await? {
            Some(v) => v,
            _ => return Ok(()),
        };
        let message = MessageEntity::get_by_gallery_id(gallery.id()).await?.unwrap();

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
        if now.day() % seed != 0 {
            return Ok(());
        }

        // 检查 tag 和标题是否有变化
        let gallery = self.ehentai.get_gallery(gallery).await?;
        if gallery.tags == entity.tags.0 && gallery.title == entity.title {
            return Ok(());
        }

        let text = self.create_message_text(&gallery, &message.telegraph).await?;

        self.bot
            .edit_message_text(self.config.telegram.channel_id.clone(), MessageId(message.id), text)
            .await?;

        Ok(())
    }

    /// 重新发布指定画廊的文章
    #[tracing::instrument(skip(self))]
    pub async fn republish(&self, gallery: &GalleryEntity, msg: &MessageEntity) -> Result<()> {
        let article = self.publish_telegraph_article(gallery).await?;
        let text = self.create_message_text(gallery, &article.url).await?;
        self.bot
            .edit_message_text(self.config.telegram.channel_id.clone(), MessageId(msg.id), text)
            .await?;
        MessageEntity::update_telegraph(gallery.id, &article.url).await?;
        Ok(())
    }
}

impl ExloliUploader {
    /// 获取某个画廊里的所有图片，并且上传到 telegrpah，如果已经上传过的，会跳过上传
    async fn upload_gallery_image(&self, gallery: &EhGallery) -> Result<()> {
        // 扫描所有图片
        // 对于已经上传过的图片，不需要重复上传，只需要插入 PageEntity 记录即可
        let mut pages = vec![];
        for page in &gallery.pages {
            match ImageEntity::get_by_hash(page.hash()).await? {
                Some(img) => {
                    // NOTE: 此处存在重复插入的可能，但是由于 PageEntity::create 使用 OR IGNORE，所以不影响
                    PageEntity::create(page.gallery_id(), page.page(), img.id).await?;
                }
                None => pages.push(page.clone()),
            }
        }
        info!("需要下载&上传 {} 张图片", pages.len());

        let (tx, mut rx) = tokio::sync::mpsc::channel(self.config.threads_num);
        let client = self.ehentai.clone();
        let concurrent = self.config.threads_num;

        // E 站的图片是分布式存储的，所以并行下载
        let downloader = tokio::spawn(
            async move {
                let mut stream = stream::iter(pages)
                    .map(|page| {
                        let client = client.clone();
                        async move { (page.clone(), client.get_image_bytes(&page).await) }
                    })
                    .buffered(concurrent);
                while let Some((page, bytes)) = stream.next().await {
                    debug!("已下载: {}", page.page());
                    match bytes {
                        Ok(bytes) => tx.send((page, bytes)).await?,
                        Err(e) => error!("下载失败 {:?}：{:?}", page, e),
                    }
                }
                Result::<()>::Ok(())
            }
            .in_current_span(),
        );

        // 依次将图片上传到 telegraph，并插入 ImageEntity 和 PageEntity 记录
        let client = Client::builder().timeout(Duration::from_secs(30)).build()?;
        let uploader = tokio::spawn(
            async move {
                // TODO: 此处可以考虑一次上传多个图片，减少请求次数，避免触发 telegraph 的 rate limit
                while let Some((page, bytes)) = rx.recv().await {
                    let resp = Telegraph::upload_with(&[ImageBytes(bytes)], &client).await?;
                    let image = ImageEntity::create(page.hash(), &resp[0].src).await?;
                    PageEntity::create(page.gallery_id(), page.page(), image.id).await?;
                    debug!("已上传: {}", page.page());
                }
                Result::<()>::Ok(())
            }
            .in_current_span(),
        );

        let (first, second) = tokio::try_join!(downloader, uploader)?;
        first?;
        second?;

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
        for img in images {
            html.push_str(&format!(r#"<img src="{}">"#, img.url));
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
}
