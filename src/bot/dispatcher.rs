use teloxide::prelude::*;

use super::handlers::*;
use crate::config::Config;
use crate::manager::uploader::ExloliUploader;

pub async fn start_dispatcher(config: Config, ehentai: ExloliUploader, bot: Bot) {
    let handler = dptree::entry()
        .branch(
            Update::filter_message()
                .branch(admin_command_handler())
                .branch(public_command_handler()),
        )
        .branch(Update::filter_callback_query().endpoint(callback_query_handler));

    Dispatcher::builder(bot, handler)
        .dependencies(dptree::deps![ehentai, config])
        .build()
        .dispatch()
        .await;
}
