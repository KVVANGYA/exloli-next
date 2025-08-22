use anyhow::{Context, Result};
use teloxide::dispatching::DpHandlerDescription;
use teloxide::dptree::case;
use teloxide::prelude::*;
use teloxide::types::MessageId;
use tracing::info;

use crate::bot::command::AdminCommand;
use crate::bot::filter::filter_admin_msg;
use crate::bot::Bot;
use crate::database::{GalleryEntity, MessageEntity};
use crate::ehentai::EhGalleryUrl;
use crate::uploader::ExloliUploader;
use crate::{reply_to, try_with_reply};

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
        
        // 执行上传
        match uploader.try_upload(gallery, false).await {
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
