use anyhow::{anyhow, Context, Result};
use rand::prelude::*;
use reqwest::Url;
use teloxide::dispatching::DpHandlerDescription;
use teloxide::dptree::case;
use teloxide::prelude::*;
use teloxide::types::InputFile;
use teloxide::utils::command::BotCommands;
use teloxide::utils::html::escape;
use tracing::info;
use std::time::Duration;
use std::sync::Arc;

use crate::bot::command::{AdminCommand, PublicCommand};
use crate::bot::filter::filter_admin_msg;
use crate::bot::{ThrottledEditor};
use crate::bot::handlers::{
    cmd_best_keyboard, cmd_best_text, cmd_challenge_keyboard, gallery_preview_url,
};
use crate::bot::scheduler::Scheduler;
use crate::bot::utils::{ChallengeLocker, ChallengeProvider};
use crate::bot::Bot;
use crate::config::Config;
use crate::database::{GalleryEntity, MessageEntity, PollEntity};
use crate::ehentai::{EhGalleryUrl, GalleryInfo};
use crate::tags::EhTagTransDB;
use crate::uploader::ExloliUploader;
use crate::reply_to;

pub fn public_command_handler(
    config: Config,
) -> Handler<'static, DependencyMap, Result<()>, DpHandlerDescription> {
    if config.telegram.allow_public_commands {
        teloxide::filter_command::<PublicCommand, _>()
            .branch(case![PublicCommand::Query(gallery)].endpoint(cmd_query))
            .branch(case![PublicCommand::Ping].endpoint(cmd_ping))
            .branch(case![PublicCommand::Update(url)].endpoint(cmd_update))
            .branch(case![PublicCommand::Best(from, to)].endpoint(cmd_best))
            .branch(case![PublicCommand::Challenge].endpoint(cmd_challenge))
            .branch(case![PublicCommand::Upload(urls)].endpoint(cmd_upload))
            .branch(case![PublicCommand::Help].endpoint(cmd_help))
    } else {
        teloxide::filter_command::<PublicCommand, _>()
            .chain(filter_admin_msg())
            .branch(case![PublicCommand::Query(gallery)].endpoint(cmd_query))
            .branch(case![PublicCommand::Ping].endpoint(cmd_ping))
            .branch(case![PublicCommand::Update(url)].endpoint(cmd_update))
            .branch(case![PublicCommand::Best(from, to)].endpoint(cmd_best))
            .branch(case![PublicCommand::Challenge].endpoint(cmd_challenge))
            .branch(case![PublicCommand::Upload(urls)].endpoint(cmd_upload))
            .branch(case![PublicCommand::Help].endpoint(cmd_help))
    }
}

async fn cmd_help(bot: Bot, msg: Message) -> Result<()> {
    let me = bot.get_me().await?;
    let public_help = PublicCommand::descriptions().username_from_me(&me);
    let admin_help = AdminCommand::descriptions().username_from_me(&me);
    let text = format!("管理员指令：\n{}\n\n公共指令：\n{}", admin_help, public_help);
    reply_to!(bot, msg, escape(&text)).await?;
    Ok(())
}

async fn cmd_upload(
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
        create_progress_bar_public(0, galleries.len(), &[])
    );
    let progress_msg = reply_to!(bot, msg, progress_text).await?;
    
    let mut results = Vec::new();
    
    // 创建智能的消息编辑器，结合节流和重试功能
    let inner_bot = teloxide::Bot::new(std::env::var("TELOXIDE_TOKEN").unwrap_or_default());
    let throttled_editor = Arc::new(ThrottledEditor::new(
        inner_bot, 
        Duration::from_secs(5)
    ));
    
    for (index, gallery) in galleries.iter().enumerate() {
        info!("Processing gallery ID: {}", gallery.id());
        
        // 更新进度：当前正在处理
        let current_results = results.clone();
        let processing_text = format!(
            "📤 上传进度 ({}/{})...\n正在处理: {}\n\n{}",
            index + 1,
            galleries.len(),
            gallery.id(),
            create_progress_bar_public(index, galleries.len(), &current_results)
        );
        
        throttled_editor.edit_message_throttled(msg.chat.id, progress_msg.id, processing_text).await.ok();
        
        // 检查权限
        if GalleryEntity::get(gallery.id()).await?.is_none() {
            results.push((gallery.id(), false, "非管理员只能上传存在上传记录的画廊".to_string()));
        } else {
            // 创建进度跟踪（简化版本，因为公共命令权限限制）
            match uploader.try_upload(gallery, true).await {
                Ok(_) => {
                    info!("Upload successful for gallery {}", gallery.id());
                    results.push((gallery.id(), true, "上传成功".to_string()));
                },
                Err(e) => {
                    info!("Upload failed for gallery {}: {}", gallery.id(), e);
                    results.push((gallery.id(), false, format!("上传失败 - {}", e)));
                }
            }
        }
        
        // 更新进度：当前项目完成
        let progress_text = format!(
            "📤 上传进度 ({}/{})...\n\n{}",
            index + 1,
            galleries.len(),
            create_progress_bar_public(index + 1, galleries.len(), &results)
        );
        
        throttled_editor.edit_message_throttled(msg.chat.id, progress_msg.id, progress_text).await.ok();
    }
    
    // 最终结果
    let final_text = format!(
        "✅ 上传完成!\n\n{}",
        create_final_summary_public(&results)
    );
    
    bot.edit_message_text(msg.chat.id, progress_msg.id, final_text).await?;
    info!("Upload process completed");
    Ok(())
}

fn create_progress_bar_public(current: usize, total: usize, results: &[(i32, bool, String)]) -> String {
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

fn create_final_summary_public(results: &[(i32, bool, String)]) -> String {
    let successful = results.iter().filter(|(_, success, _)| *success).count();
    let failed = results.len() - successful;
    
    let mut text = format!("总计: {} 个画廊, {} 成功, {} 失败\n\n", results.len(), successful, failed);
    
    for (id, success, status) in results {
        let icon = if *success { "✅" } else { "❌" };
        text.push_str(&format!("{} {}: {}\n", icon, id, status));
    }
    
    text
}


async fn cmd_challenge(
    bot: Bot,
    msg: Message,
    trans: EhTagTransDB,
    locker: ChallengeLocker,
    scheduler: Scheduler,
    challange_provider: ChallengeProvider,
) -> Result<()> {
    info!("{}: /challenge", msg.from().unwrap().id);
    let mut challenge = challange_provider.get_challenge().await.unwrap();
    let answer = challenge[0].clone();
    challenge.shuffle(&mut thread_rng());
    let url = format!("https://telegra.ph{}", answer.url);
    let id = locker.add_challenge(answer.id, answer.page, answer.artist.clone());
    let keyboard = cmd_challenge_keyboard(id, &challenge, &trans);
    let reply = bot
        .send_photo(msg.chat.id, InputFile::url(url.parse()?))
        .caption("上述图片来自下列哪位作者的本子？")
        .reply_markup(keyboard)
        .reply_to_message_id(msg.id)
        .await?;
    if !msg.chat.is_private() {
        scheduler.delete_msg(msg.chat.id, msg.id, 120);
        scheduler.delete_msg(msg.chat.id, reply.id, 120);
    }
    Ok(())
}

async fn cmd_best(
    bot: Bot,
    msg: Message,
    (end, start): (u16, u16),
    cfg: Config,
    scheduler: Scheduler,
) -> Result<()> {
    info!("{}: /best {} {}", msg.from().unwrap().id, end, start);
    let text = cmd_best_text(start as i32, end as i32, 0, cfg.telegram.channel_id).await?;
    let keyboard = cmd_best_keyboard(start as i32, end as i32, 0);
    let reply =
        reply_to!(bot, msg, text).reply_markup(keyboard).disable_web_page_preview(true).await?;
    if !msg.chat.is_private() {
        scheduler.delete_msg(msg.chat.id, msg.id, 120);
        scheduler.delete_msg(msg.chat.id, reply.id, 120);
    }
    Ok(())
}

async fn cmd_update(bot: Bot, msg: Message, uploader: ExloliUploader, url: String) -> Result<()> {
    info!("{}: /update {}", msg.from().unwrap().id, url);
    let msg_id = if url.is_empty() {
        msg.reply_to_message()
            .and_then(|msg| msg.forward_from_message_id())
            .ok_or(anyhow!("Invalid URL"))?
    } else {
        Url::parse(&url)?
            .path_segments()
            .and_then(|p| p.last())
            .and_then(|id| id.parse::<i32>().ok())
            .ok_or(anyhow!("Invalid URL"))?
    };
    let msg_entity = MessageEntity::get(msg_id).await?.ok_or(anyhow!("Message not found"))?;
    let gl_entity =
        GalleryEntity::get(msg_entity.gallery_id).await?.ok_or(anyhow!("Gallery not found"))?;

    let reply = reply_to!(bot, msg, "更新中……").await?;

    // 调用 rescan_gallery 把失效画廊重新上传
    uploader.recheck(vec![gl_entity.clone()]).await?;
    // 看一下有没有 tag 或者标题需要更新
    uploader.try_update(&gl_entity.url(), false).await?;
    bot.edit_message_text(msg.chat.id, reply.id, "更新完成").await?;

    Ok(())
}

async fn cmd_ping(bot: Bot, msg: Message, scheduler: Scheduler) -> Result<()> {
    info!("{}: /ping", msg.from().unwrap().id);
    let reply = reply_to!(bot, msg, "pong~").await?;
    if !msg.chat.is_private() {
        scheduler.delete_msg(msg.chat.id, msg.id, 120);
        scheduler.delete_msg(msg.chat.id, reply.id, 120);
    }
    Ok(())
}

async fn cmd_query(bot: Bot, msg: Message, cfg: Config, gallery: EhGalleryUrl) -> Result<()> {
    info!("{}: /query {}", msg.from().unwrap().id, gallery);
    match GalleryEntity::get(gallery.id()).await? {
        Some(gallery) => {
            let poll = PollEntity::get_by_gallery(gallery.id).await?.context("找不到投票")?;
            let preview = gallery_preview_url(cfg.telegram.channel_id, gallery.id).await?;
            let url = gallery.url().url();
            reply_to!(
                bot,
                msg,
                format!(
                    "消息：{preview}\n地址：{url}\n评分：{:.2}（{:.2}）",
                    poll.score * 100.,
                    poll.rank().await? * 100.
                )
            )
            .await?;
        }
        None => {
            reply_to!(bot, msg, "未找到").await?;
        }
    }
    Ok(())
}
