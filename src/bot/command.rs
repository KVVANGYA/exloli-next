use teloxide::utils::command::BotCommands;

use crate::ehentai::EhGalleryUrl;

// NOTE: 此处必须实现 Clone，否则不满足 dptree 的 Injectable 约束
#[derive(BotCommands, Clone, PartialEq, Debug)]
#[command(rename_rule = "lowercase")]
pub enum AdminCommand {
    #[command(description = "根据 E 站 URL 上传一个指定画廊，如果已存在，则重新上传")]
    Upload(EhGalleryUrl),
    #[command(description = "删除所回复的画廊")]
    Delete,
    #[command(description = "完全删除所回复的画廊，会导致重新上传")]
    Erase,
    // TODO: 该功能需要移除
    #[command(description = "将 80 分以上的本子中，没有被补档的重新上传")]
    ReUpload,
    #[command(description = "检测并补档 80 分以上或最近两个月的本子的预览")]
    ReCheck,
}

#[derive(BotCommands, Clone, PartialEq, Debug)]
#[command(rename_rule = "lowercase")]
pub enum PublicCommand {
    #[command(description = "根据 E 站 URL 上传一个曾经上传过的画廊")]
    Upload(EhGalleryUrl),
    #[command(description = "根据消息 URL 更新一个指定画廊")]
    Update(String),
    #[command(description = "根据 E 站 URL 查询一个指定画廊")]
    Query(EhGalleryUrl),
    #[command(
        description = "查询从最近 $1 天到 $2 天内的本子排名（$1 < $2）",
        parse_with = "split"
    )]
    Best(u16, u16),
    #[command(description = "想和本 bot 斗斗吗？")]
    Challenge,
    #[command(description = "pong~")]
    Ping,
    #[command(description = "帮助")]
    Help,
}
