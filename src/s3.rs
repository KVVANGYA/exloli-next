use anyhow::Result;
use reqwest::Client;
use tokio::io::AsyncReadExt;
use tracing::{debug, error};

pub struct S3Uploader {
    client: Client,
    gateway_host: String,
    gateway_date: String,
    teletype_token: Option<String>,
}

impl S3Uploader {
    pub fn new(gateway_host: String, gateway_date: String, teletype_token: Option<String>) -> Result<Self> {
        let client = Client::new();
        Ok(Self { client, gateway_host, gateway_date, teletype_token })
    }

    pub async fn upload_multiple<R: AsyncReadExt + Unpin>(
        &self,
        uploads: Vec<(&str, &mut R)>,
    ) -> Result<Vec<String>> {
        let mut urls = Vec::new();
        
        for (name, reader) in uploads {
            let url = if self.teletype_token.is_some() {
                self.upload_to_teletype(name, reader).await
            } else {
                self.upload(name, reader).await
            };
            
            urls.push(url?);
        }
        
        Ok(urls)
    }

    pub async fn upload_to_teletype<R: AsyncReadExt + Unpin>(
        &self,
        name: &str,
        reader: &mut R,
    ) -> Result<String> {
        let token = self.teletype_token.as_ref().ok_or(anyhow::anyhow!("Authorization token is required"))?;
        
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).await?;

        debug!("正在上传到teletype.in: 文件名: {}, 大小: {} 字节, Authorization: {}, Authorization.Clone: {}", name, buffer.len(), token, token.clone());

        let file_extension = name.split('.').last().unwrap_or("jpg");
        let content_type = match file_extension.to_lowercase().as_str() {
            "jpg" | "jpeg" => "image/jpeg",
            "png" => "image/png",
            "gif" => "image/gif",
            "webp" => "image/webp",
            "bmp" => "image/bmp",
            _ => "image/jpeg",
        };

        let part = reqwest::multipart::Part::bytes(buffer)
            .file_name(name.to_string())
            .mime_str(content_type)?;

        let form = reqwest::multipart::Form::new()
            .part("file", part)
            .text("type", "images");

        let response = self
            .client
            .put("https://teletype.in/media/")
            .header("Authorization", token.clone())
            .multipart(form)
            .send()
            .await?;
            
        if !response.status().is_success() {
            let status = response.status();
            let error_text = response.text().await.unwrap_or_else(|_| "无法获取错误详情".to_string());
            error!("Teletype上传失败: 状态码: {}, 错误信息: {}", status, error_text);
            
            if status.as_u16() == 401 || status.as_u16() == 403 {
                return Err(anyhow::anyhow!("Teletype授权失败，请检查令牌: {} - {}", status, error_text));
            }
            return Err(anyhow::anyhow!("上传到teletype.in失败: {} - {}", status, error_text));
        }
        
        let response_text = response.text().await?;
        debug!("Teletype上传成功: 响应: {}", response_text);
        
        let url = response_text.trim().to_string();
        
        if url.is_empty() || !url.starts_with("http") {
            return Err(anyhow::anyhow!("无效的响应URL: {}", response_text));
        }
        
        Ok(url)
    }

    async fn upload_fallback(&self, name: &str, buffer: &[u8]) -> Result<String> {
        debug!("使用备用方式上传: {}", name);
        
        let form = reqwest::multipart::Form::new()
            .part("file", reqwest::multipart::Part::bytes(buffer.to_vec()).file_name(name.to_string()));

        let response = self
            .client
            .post("https://api.img2ipfs.org/api/v0/add?pin=false")
            .multipart(form)
            .send()
            .await?
            .json::<serde_json::Value>()
            .await?;

        let hash = response["Hash"].as_str().ok_or(anyhow::anyhow!("Invalid response"))?;
        let name = response["Name"].as_str().ok_or(anyhow::anyhow!("Invalid response"))?;
        let url = format!("{}{}/?{}&filename={}", self.gateway_host, hash, self.gateway_date, name);

        Ok(url)
    }

    pub async fn upload<R: AsyncReadExt + Unpin>(
        &self,
        name: &str,
        reader: &mut R,
    ) -> Result<String> {
        if self.teletype_token.is_some() {
            return self.upload_to_teletype(name, reader).await;
        }
        
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

        let hash = response["Hash"].as_str().ok_or(anyhow::anyhow!("Invalid response"))?;
        let name = response["Name"].as_str().ok_or(anyhow::anyhow!("Invalid response"))?;
        let url = format!("{}{}/?{}&filename={}", self.gateway_host, hash, self.gateway_date, name);

        Ok(url)
    }
}
