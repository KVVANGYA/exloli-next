use std::time::Duration;
use tokio::time::sleep;
use tracing::{warn, debug};
use teloxide::Bot;
use teloxide::prelude::Requester;
use teloxide::types::{ChatId, MessageId};
use std::sync::Arc;


/// Auto-retry wrapper for Telegram Bot API calls with rate limiting handling
pub struct AutoRetryBot {
    bot: Bot,
    max_retries: u32,
    base_delay: Duration,
}

impl AutoRetryBot {
    pub fn new(bot: Bot) -> Self {
        Self {
            bot,
            max_retries: 3,
            base_delay: Duration::from_secs(1),
        }
    }

    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    pub fn with_base_delay(mut self, base_delay: Duration) -> Self {
        self.base_delay = base_delay;
        self
    }

    /// Edit message text with automatic retry on network errors
    pub async fn edit_message_text_with_retry<T>(
        &self,
        chat_id: ChatId,
        message_id: MessageId,
        text: T,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        T: Into<String> + Clone,
    {
        let mut attempts = 0;
        
        loop {
            match self.bot.edit_message_text(chat_id, message_id, text.clone()).await {
                Ok(_) => {
                    debug!("Message edited successfully after {} attempts", attempts + 1);
                    return Ok(());
                }
                Err(error) => {
                    attempts += 1;
                    if attempts > self.max_retries {
                        warn!("Max retries ({}) exceeded for editing message: {:?}", self.max_retries, error);
                        return Err(Box::new(error));
                    }
                    
                    // Check if it's a rate limiting error by examining the error message
                    let error_str = error.to_string();
                    if error_str.contains("Too Many Requests") || error_str.contains("retry_after") {
                        let delay = self.base_delay * attempts;
                        warn!(
                            "Rate limiting detected, retrying in {} seconds (attempt {}/{})", 
                            delay.as_secs(), attempts, self.max_retries
                        );
                        sleep(delay).await;
                        continue;
                    } else if error_str.contains("timeout") || error_str.contains("network") {
                        let delay = self.base_delay * attempts;
                        warn!(
                            "Network error, retrying in {} seconds (attempt {}/{})", 
                            delay.as_secs(), attempts, self.max_retries
                        );
                        sleep(delay).await;
                        continue;
                    } else {
                        // For other errors, fail immediately
                        debug!("Non-retryable error: {:?}", error);
                        return Err(Box::new(error));
                    }
                }
            }
        }
    }

    /// Get the underlying bot instance for other operations
    pub fn bot(&self) -> &Bot {
        &self.bot
    }
}

/// Throttled message editor that combines time-based throttling with auto-retry
pub struct ThrottledEditor {
    auto_retry_bot: AutoRetryBot,
    last_update: std::sync::Arc<tokio::sync::Mutex<std::time::Instant>>,
    min_interval: Duration,
}

impl ThrottledEditor {
    pub fn new(bot: Bot, min_interval: Duration) -> Self {
        Self {
            auto_retry_bot: AutoRetryBot::new(bot),
            last_update: std::sync::Arc::new(tokio::sync::Mutex::new(
                std::time::Instant::now() - Duration::from_secs(60)
            )),
            min_interval,
        }
    }

    /// Edit message with both throttling and retry logic
    pub async fn edit_message_throttled<T>(
        &self,
        chat_id: ChatId,
        message_id: MessageId,
        text: T,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        T: Into<String> + Clone,
    {
        // Check throttling
        {
            let mut last_update = self.last_update.lock().await;
            let now = std::time::Instant::now();
            let elapsed = now.duration_since(*last_update);
            
            if elapsed < self.min_interval {
                debug!("Throttling: skipping update ({}ms < {}ms)", 
                    elapsed.as_millis(), self.min_interval.as_millis());
                return Ok(()); // Skip this update
            }
            *last_update = now;
        }

        // Perform the edit with retry logic
        self.auto_retry_bot
            .edit_message_text_with_retry(chat_id, message_id, text)
            .await
    }
}