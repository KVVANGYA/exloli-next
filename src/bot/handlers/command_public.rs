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

use crate::bot::command::{AdminCommand, PublicCommand};
use crate::bot::filter::filter_admin_msg;
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
use crate::{reply_to, try_with_reply};

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
    
    let mut results = Vec::new();
    
    for gallery in galleries {
        if GalleryEntity::get(gallery.id()).await?.is_none() {
            results.push(format!("{}: 非管理员只能上传存在上传记录的画廊", gallery.id()));
        } else {
            match uploader.try_upload(&gallery, true).await {
                Ok(_) => results.push(format!("{}: 上传成功", gallery.id())),
                Err(e) => results.push(format!("{}: 上传失败 - {}", gallery.id(), e)),
            }
        }
    }
    
    let response = results.join("\n");
    reply_to!(bot, msg, response).await?;
    Ok(())
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
