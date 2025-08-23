use anyhow::{Context, Result};
use teloxide::dispatching::DpHandlerDescription;
use teloxide::dptree::case;
use teloxide::prelude::*;
use teloxide::types::MessageId;
use tracing::info;
use std::sync::Arc;
use tokio::sync::Mutex;
use std::time::{Duration, Instant};

use crate::bot::command::AdminCommand;
use crate::bot::filter::filter_admin_msg;
use crate::bot::{Bot, ThrottledEditor};
use crate::database::{GalleryEntity, MessageEntity};
use crate::ehentai::EhGalleryUrl;
use crate::uploader::{ExloliUploader, UploadProgress};
use crate::{reply_to, try_with_reply};

#[derive(Clone)]
pub struct GalleryProgress {
    pub gallery_id: i32,
    pub total_pages: usize,
    pub existing_pages: usize,
    pub downloaded_pages: usize,
    pub uploaded_pages: usize,
    pub parsed_pages: usize,
    pub current_stage: UploadStage,
    pub status_message: String,
}

#[derive(Clone, Debug)]
pub enum UploadStage {
    Starting,
    Scanning,
    Resolving,
    Uploading,
    Publishing,
    Complete,
    Failed(String),
}

pub fn admin_command_handler() -> Handler<'static, DependencyMap, Result<()>, DpHandlerDescription>
{
    teloxide::filter_command::<AdminCommand, _>()
        .chain(filter_admin_msg())
        .branch(case![AdminCommand::Upload(urls)].endpoint(cmd_upload))
        .branch(case![AdminCommand::Delete].endpoint(cmd_delete))
        .branch(case![AdminCommand::Erase].endpoint(cmd_delete))
        .branch(case![AdminCommand::ReCheck].endpoint(cmd_recheck))
        .branch(case![AdminCommand::ReUpload].endpoint(cmd_reupload))
}

// TODO: 该功能需要移除
async fn cmd_reupload(bot: Bot, msg: Message, uploader: ExloliUploader) -> Result<()> {
    info!("{}: /reupload", msg.from().unwrap().id);
    try_with_reply!(bot, msg, uploader.reupload(vec![]).await);
    Ok(())
}

async fn cmd_recheck(bot: Bot, msg: Message, uploader: ExloliUploader) -> Result<()> {
    info!("{}: /recheck", msg.from().unwrap().id);
    try_with_reply!(bot, msg, uploader.recheck(vec![]).await);
    Ok(())
}

async fn cmd_upload(
    bot: Bot,
    msg: Message,
    uploader: ExloliUploader,
    urls: String,
) -> Result<()> {
    cmd_upload_wrapper(bot, msg, uploader, urls).await
}

// Add a catch-all wrapper to handle any panics
async fn cmd_upload_wrapper(
    bot: Bot,
    msg: Message,
    uploader: ExloliUploader,
    urls: String,
) -> Result<()> {
    match cmd_upload_inner(bot.clone(), msg.clone(), uploader, urls).await {
        Ok(_) => {
            info!("Upload command completed successfully");
            Ok(())
        },
        Err(e) => {
            info!("Upload command failed with error: {}", e);
            reply_to!(bot, msg, format!("命令执行失败: {}", e)).await?;
            Err(e)
        }
    }
}

async fn cmd_upload_inner(
    bot: Bot,
    msg: Message,
    uploader: ExloliUploader,
    urls: String,
) -> Result<()> {
    let user_id = msg.from().unwrap().id;
    info!("{}: /upload {}", user_id, urls);
    
    if urls.trim().is_empty() {
        reply_to!(bot, msg, "请提供至少一个画廊链接").await?;
        return Ok(());
    }
    
    let galleries: Vec<EhGalleryUrl> = urls
        .split_whitespace()
        .filter_map(|url| match url.parse::<EhGalleryUrl>() {
            Ok(gallery) => Some(gallery),
            Err(e) => {
                info!("Failed to parse URL {}: {}", url, e);
                None
            }
        })
        .collect();
    
    if galleries.is_empty() {
        reply_to!(bot, msg, "未找到有效的画廊链接").await?;
        return Ok(());
    }
    
    // 发送初始进度消息
    let progress_text = format!(
        "📤 开始上传 {} 个画廊...\n\n{}",
        galleries.len(),
        create_progress_bar(0, galleries.len(), &[])
    );
    let progress_msg = reply_to!(bot, msg, progress_text).await?;
    
    let mut results = Vec::new();
    
    for (index, gallery) in galleries.iter().enumerate() {
        info!("Processing gallery ID: {}", gallery.id());
        
        // 更新进度：当前正在处理
        let current_results = results.clone();
        let processing_text = format!(
            "📤 上传进度 ({}/{})...\n正在处理: {}\n\n{}",
            index + 1,
            galleries.len(),
            gallery.id(),
            create_progress_bar(index, galleries.len(), &current_results)
        );
        
        bot.edit_message_text(msg.chat.id, progress_msg.id, processing_text).await.ok();
        
        // 创建进度跟踪
        let progress = Arc::new(Mutex::new(GalleryProgress {
            gallery_id: gallery.id(),
            total_pages: 0,
            existing_pages: 0,
            downloaded_pages: 0,
            uploaded_pages: 0,
            parsed_pages: 0,
            current_stage: UploadStage::Starting,
            status_message: "开始处理".to_string(),
        }));
        
        // 创建智能的消息编辑器，结合节流和重试功能
        let inner_bot = teloxide::Bot::new(std::env::var("TELOXIDE_TOKEN").unwrap_or_default());
        let throttled_editor = Arc::new(ThrottledEditor::new(
            inner_bot, 
            Duration::from_secs(5)
        ));
        
        let bot_clone = bot.clone();
        let msg_clone = msg.clone();
        let progress_msg_id = progress_msg.id;
        let progress_clone = progress.clone();
        
        // 执行带进度跟踪的上传
        let galleries_clone = galleries.clone();
        let results_clone = results.clone();
        let editor_clone = throttled_editor.clone();
        let callback = Arc::new(move |prog: GalleryProgress| {
            let bot = bot_clone.clone();
            let msg = msg_clone.clone();
            let galleries = galleries_clone.clone();
            let results = results_clone.clone();
            let editor = editor_clone.clone();
            async move {
                update_gallery_progress_with_editor(&bot, msg.chat.id, progress_msg_id, index, &galleries, &results, &prog, editor).await.ok();
            }
        });
        
        match upload_with_progress_new(&uploader, gallery, false, progress_clone, callback).await {
            Ok(_) => {
                info!("Upload successful for gallery {}", gallery.id());
                results.push((gallery.id(), true, "上传成功".to_string()));
            },
            Err(e) => {
                info!("Upload failed for gallery {}: {}", gallery.id(), e);
                results.push((gallery.id(), false, format!("上传失败 - {}", e)));
            }
        }
        
        // 更新进度：当前项目完成
        let progress_text = format!(
            "📤 上传进度 ({}/{})...\n\n{}",
            index + 1,
            galleries.len(),
            create_progress_bar(index + 1, galleries.len(), &results)
        );
        
        bot.edit_message_text(msg.chat.id, progress_msg.id, progress_text).await.ok();
    }
    
    // 最终结果
    let final_text = format!(
        "✅ 上传完成!\n\n{}",
        create_final_summary(&results)
    );
    
    bot.edit_message_text(msg.chat.id, progress_msg.id, final_text).await?;
    info!("Upload process completed");
    Ok(())
}

fn create_progress_bar(current: usize, total: usize, results: &[(i32, bool, String)]) -> String {
    let progress = if total > 0 { (current * 10) / total } else { 0 };
    let filled = "█".repeat(progress);
    let empty = "░".repeat(10 - progress);
    let percentage = if total > 0 { (current * 100) / total } else { 0 };
    
    let mut text = format!("进度: [{}{}] {}% ({}/{})\n\n", filled, empty, percentage, current, total);
    
    if !results.is_empty() {
        text.push_str("已完成:\n");
        for (id, success, status) in results {
            let icon = if *success { "✅" } else { "❌" };
            text.push_str(&format!("{} {}: {}\n", icon, id, status));
        }
    }
    
    text
}

fn create_final_summary(results: &[(i32, bool, String)]) -> String {
    let successful = results.iter().filter(|(_, success, _)| *success).count();
    let failed = results.len() - successful;
    
    let mut text = format!("总计: {} 个画廊, {} 成功, {} 失败\n\n", results.len(), successful, failed);
    
    for (id, success, status) in results {
        let icon = if *success { "✅" } else { "❌" };
        text.push_str(&format!("{} {}: {}\n", icon, id, status));
    }
    
    text
}

// 新的带进度跟踪的上传函数
async fn upload_with_progress_new<F, Fut>(
    uploader: &ExloliUploader, 
    gallery_url: &EhGalleryUrl, 
    check: bool,
    progress: Arc<Mutex<GalleryProgress>>,
    callback: Arc<F>
) -> Result<()> 
where 
    F: Fn(GalleryProgress) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    // 检查是否需要上传
    if check 
        && GalleryEntity::check(gallery_url.id()).await?
        && MessageEntity::get_by_gallery(gallery_url.id()).await?.is_some()
    {
        let mut prog = progress.lock().await;
        prog.current_stage = UploadStage::Complete;
        prog.status_message = "已存在，跳过".to_string();
        callback(prog.clone()).await;
        return Ok(());
    }

    // 更新进度：开始获取画廊信息
    {
        let mut prog = progress.lock().await;
        prog.current_stage = UploadStage::Scanning;
        prog.status_message = "获取画廊信息...".to_string();
        callback(prog.clone()).await;
    }

    // 创建从 uploader 进度到 gallery 进度的映射回调
    let progress_mapper = {
        let progress = progress.clone();
        let callback = callback.clone();
        move |upload_progress: UploadProgress| {
            let progress = progress.clone();
            let callback = callback.clone();
            async move {
                let mut prog = progress.lock().await;
                prog.total_pages = upload_progress.total_pages;
                prog.downloaded_pages = upload_progress.downloaded_pages;
                prog.uploaded_pages = upload_progress.uploaded_pages;
                prog.parsed_pages = upload_progress.parsed_pages;
                prog.current_stage = UploadStage::Uploading;
                prog.status_message = format!("上传中 - 已解析: {}, 已下载: {}, 已上传: {}", 
                    upload_progress.parsed_pages, 
                    upload_progress.downloaded_pages, 
                    upload_progress.uploaded_pages
                );
                callback(prog.clone()).await;
            }
        }
    };

    // 调用带进度回调的上传方法
    match uploader.try_upload_with_progress(gallery_url, check, Some(progress_mapper)).await {
        Ok(_) => {
            let mut prog = progress.lock().await;
            prog.current_stage = UploadStage::Complete;
            prog.status_message = "上传完成".to_string();
            callback(prog.clone()).await;
            Ok(())
        }
        Err(e) => {
            let mut prog = progress.lock().await;
            prog.current_stage = UploadStage::Failed(e.to_string());
            prog.status_message = format!("上传失败: {}", e);
            callback(prog.clone()).await;
            Err(e)
        }
    }
}


// 使用 ThrottledEditor 的画廊进度更新函数
async fn update_gallery_progress_with_editor(
    _bot: &Bot,
    chat_id: ChatId,
    message_id: MessageId,
    current_gallery_index: usize,
    all_galleries: &[EhGalleryUrl],
    completed_results: &[(i32, bool, String)],
    current_progress: &GalleryProgress,
    editor: Arc<ThrottledEditor>
) -> Result<()> {
    let text = create_gallery_progress_text(current_gallery_index, all_galleries, completed_results, current_progress);
    editor.edit_message_throttled(chat_id, message_id, text).await.map_err(|e| anyhow::anyhow!("Failed to edit message: {}", e))
}

// 带时间间隔控制的画廊进度更新函数（保留作为备用）
async fn update_gallery_progress_throttled(
    bot: &Bot,
    chat_id: ChatId,
    message_id: MessageId,
    current_gallery_index: usize,
    all_galleries: &[EhGalleryUrl],
    completed_results: &[(i32, bool, String)],
    current_progress: &GalleryProgress,
    last_update_time: Arc<Mutex<Instant>>
) -> Result<()> {
    // 检查是否需要限制更新频率
    {
        let mut last_update = last_update_time.lock().await;
        let now = Instant::now();
        if now.duration_since(*last_update) < Duration::from_secs(5) {
            return Ok(()); // 跳过更新，避免频繁调用 API
        }
        *last_update = now;
    }
    
    update_gallery_progress(bot, chat_id, message_id, current_gallery_index, all_galleries, completed_results, current_progress).await
}

// 生成画廊进度文本
fn create_gallery_progress_text(
    current_gallery_index: usize,
    all_galleries: &[EhGalleryUrl],
    completed_results: &[(i32, bool, String)],
    current_progress: &GalleryProgress
) -> String {
    let mut text = format!(
        "📤 上传进度 ({}/{})\n\n",
        current_gallery_index + 1,
        all_galleries.len()
    );
    
    // 显示当前画廊的详细进度
    text.push_str(&format!("当前画廊: {}\n", current_progress.gallery_id));
    text.push_str(&format!("阶段: {:?}\n", current_progress.current_stage));
    text.push_str(&format!("状态: {}\n", current_progress.status_message));
    
    // 显示已解析、已下载、已上传的数量
    text.push_str(&format!("📋 已解析: {}\n", current_progress.parsed_pages));
    text.push_str(&format!("⬇️ 已下载: {}\n", current_progress.downloaded_pages));
    text.push_str(&format!("⬆️ 已上传: {}\n", current_progress.uploaded_pages));
    
    if current_progress.total_pages > 0 {
        let total_to_process = current_progress.total_pages - current_progress.existing_pages;
        if total_to_process > 0 {
            let progress_bar = create_page_progress_bar(current_progress.uploaded_pages, total_to_process);
            text.push_str(&format!("页面进度: {}\n", progress_bar));
            text.push_str(&format!("({}/{} 新页面, {} 已存在)\n", 
                current_progress.uploaded_pages, 
                total_to_process,
                current_progress.existing_pages
            ));
        } else {
            text.push_str("所有页面已存在\n");
        }
    }
    
    text.push_str("\n");
    
    // 显示总体进度
    let overall_progress = create_progress_bar(current_gallery_index, all_galleries.len(), completed_results);
    text.push_str(&overall_progress);
    
    text
}

// 更新画廊进度显示
async fn update_gallery_progress(
    bot: &Bot,
    chat_id: ChatId,
    message_id: MessageId,
    current_gallery_index: usize,
    all_galleries: &[EhGalleryUrl],
    completed_results: &[(i32, bool, String)],
    current_progress: &GalleryProgress
) -> Result<()> {
    let text = create_gallery_progress_text(current_gallery_index, all_galleries, completed_results, current_progress);
    bot.edit_message_text(chat_id, message_id, text).await.ok();
    Ok(())
}

// 创建页面级进度条
fn create_page_progress_bar(current: usize, total: usize) -> String {
    if total == 0 {
        return "无需上传".to_string();
    }
    
    let progress = (current * 10) / total.max(1);
    let filled = "█".repeat(progress);
    let empty = "░".repeat(10 - progress);
    let percentage = (current * 100) / total.max(1);
    
    format!("[{}{}] {}% ({}/{})", filled, empty, percentage, current, total)
}

async fn cmd_delete(bot: Bot, msg: Message, command: AdminCommand) -> Result<()> {
    info!("{}: /delete", msg.from().unwrap().id);
    let reply_to = msg.reply_to_message().context("没有回复消息")?;

    let channel = reply_to.forward_from_chat().context("该消息没有回复画廊")?;
    let channel_msg = reply_to.forward_from_message_id().context("获取转发来源失败")?;

    let msg_entity = MessageEntity::get(channel_msg).await?.unwrap();

    bot.delete_message(reply_to.chat.id, reply_to.id).await?;
    bot.delete_message(channel.id, MessageId(msg_entity.id)).await?;

    if matches!(command, AdminCommand::Delete) {
        GalleryEntity::update_deleted(msg_entity.gallery_id, true).await?;
    } else {
        GalleryEntity::delete(msg_entity.gallery_id).await?;
        MessageEntity::delete(channel_msg).await?;
    }

    Ok(())
}
