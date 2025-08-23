mod auto_retry;
mod command;
mod dispatcher;
mod filter;
mod handlers;
mod scheduler;
mod utils;

pub use dispatcher::start_dispatcher;
pub use auto_retry::{AutoRetryBot, ThrottledEditor};
use teloxide::adaptors::{CacheMe, DefaultParseMode, Throttle};

pub type Bot = CacheMe<DefaultParseMode<Throttle<teloxide::Bot>>>;
