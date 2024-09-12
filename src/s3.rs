use reqwest::Client;
use anyhow::{Result, anyhow};
use tokio::time::{sleep, Duration};
use crate::config::Ipfs as IpfsConfig;

pub struct S3Uploader {
    client: Client,
    config: IpfsConfig,
}

impl S3Uploader {
    pub fn new(config: IpfsConfig) -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()?;
        Ok(Self { client, config })
    }

    pub async fn upload<R: AsyncReadExt + Unpin>(
        &self,
        name: &str,
        reader: &mut R,
    ) -> Result<String> {
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).await?;

        let form = reqwest::multipart::Form::new()
            .part("file", reqwest::multipart::Part::bytes(buffer).file_name(name.to_string()));

        let response = self
            .client
            .post("https://api.img2ipfs.org/api/v0/add?pin=false")
            .multipart(form)
            .send()
            .await?
            .json::<serde_json::Value>()
            .await?;

        let hash = response["Hash"].as_str().ok_or(anyhow!("Invalid response"))?;
        let url = format!("{}{}/?{}", self.config.gateway_host, hash, self.config.gateway_date);

        // 如果启用了二次检查，则验证文件是否可访问
        if self.config.enable_double_check {
            self.verify_file_accessibility(&url).await?;
        }

        Ok(url)
    }

    async fn verify_file_accessibility(&self, url: &str) -> Result<()> {
        const MAX_RETRIES: u32 = 5;
        const RETRY_DELAY: Duration = Duration::from_secs(2);

        for _ in 0..MAX_RETRIES {
            match self.client.head(url).send().await {
                Ok(response) if response.status().is_success() => return Ok(()),
                _ => sleep(RETRY_DELAY).await,
            }
        }

        Err(anyhow!("文件上传后无法访问：{}", url))
    }
}
