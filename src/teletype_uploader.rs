use anyhow::Result;
use reqwest::Client;
use tokio::io::AsyncReadExt;
use tracing::{debug, error};

#[derive(Clone)]
pub struct S3Uploader {
    client: Client,
    teletype_token: String,
}

impl S3Uploader {
    pub fn new(teletype_token: String) -> Result<Self> {
        let client = Client::new();
        Ok(Self { client, teletype_token })
    }


    pub async fn upload_to_teletype<R: AsyncReadExt + Unpin>(
        &self,
        name: &str,
        reader: &mut R,
    ) -> Result<String> {
        let token = &self.teletype_token;
        
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).await?;

        debug!("正在上传到teletype.in: 文件名: {}, 大小: {} 字节", name, buffer.len());

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
            .put("https://teletype.in/media/") // 一定要添加尾部斜杠
            .header("Authorization", token)
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
        debug!("Teletype上传成功 ({}): 响应: {}", name, response_text);
        
        // 解析JSON响应
        let url = match serde_json::from_str::<serde_json::Value>(&response_text) {
            Ok(json) => {
                // 从JSON中获取url字段
                if let Some(url) = json["url"].as_str() {
                    url.to_string()
                } else {
                    return Err(anyhow::anyhow!("无法从JSON响应中提取URL字段: {}", response_text));
                }
            },
            Err(e) => {
                // 如果不是有效的JSON，尝试直接使用响应文本作为URL
                debug!("无法解析JSON响应，尝试直接使用响应文本: {}", e);
                let url_text = response_text.trim();
                if url_text.starts_with("http") {
                    url_text.to_string()
                } else {
                    return Err(anyhow::anyhow!("无效的响应内容，既不是URL也不是有效JSON: {}", response_text));
                }
            }
        };
        
        debug!("提取的URL ({}): {}", name, url);
        
        if url.is_empty() || !url.starts_with("http") {
            return Err(anyhow::anyhow!("提取的URL无效: {}", url));
        }
        
        Ok(url)
    }


    pub async fn upload<R: AsyncReadExt + Unpin>(
        &self,
        name: &str,
        reader: &mut R,
    ) -> Result<String> {
        self.upload_to_teletype(name, reader).await
    }
}
