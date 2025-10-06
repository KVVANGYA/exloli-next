use anyhow::Result;
use reqwest::Client;
use tokio::io::AsyncReadExt;
use tracing::{debug, error};

#[derive(Clone)]
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
                match self.upload_to_teletype(name, reader).await {
                    Ok(url) => Ok(url),
                    Err(e) => {
                        debug!("Teletype上传失败，尝试备用上传方式: {}", e);
                        let mut buffer = Vec::new();
                        reader.read_to_end(&mut buffer).await?;
                        self.upload_fallback(name, &buffer).await
                    }
                }
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

        let part = reqwest::multipart::Part::bytes(buffer.clone())
            .file_name(name.to_string())
            .mime_str(content_type)?;

        let form = reqwest::multipart::Form::new()
            .part("file", part)
            .text("type", "images");

        let response = self
            .client
            .put("https://teletype.in/media/") // 一定要添加尾部斜杠
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
            
            // 对于524超时错误，进行一次重试
            if status.as_u16() == 524 {
                debug!("检测到524超时错误，等待2秒后重试一次");
                tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                
                // 重新构建表单进行重试
                let part_retry = reqwest::multipart::Part::bytes(buffer.clone())
                    .file_name(name.to_string())
                    .mime_str(content_type)?;

                let form_retry = reqwest::multipart::Form::new()
                    .part("file", part_retry)
                    .text("type", "images");

                match self
                    .client
                    .put("https://teletype.in/media/")
                    .header("Authorization", token.clone())
                    .multipart(form_retry)
                    .send()
                    .await 
                {
                    Ok(retry_response) => {
                        if retry_response.status().is_success() {
                            debug!("524重试成功");
                            let retry_text = retry_response.text().await?;
                            match serde_json::from_str::<serde_json::Value>(&retry_text) {
                                Ok(json) => {
                                    if let Some(url) = json["url"].as_str() {
                                        return Ok(url.to_string());
                                    } else {
                                        return Err(anyhow::anyhow!("重试成功但无法从JSON响应中提取URL: {}", retry_text));
                                    }
                                },
                                Err(_) => {
                                    let url_text = retry_text.trim();
                                    if url_text.starts_with("http") {
                                        return Ok(url_text.to_string());
                                    } else {
                                        return Err(anyhow::anyhow!("重试成功但响应内容无效: {}", retry_text));
                                    }
                                }
                            };
                        } else {
                            debug!("524重试仍然失败: {}", retry_response.status());
                        }
                    }
                    Err(e) => {
                        debug!("524重试请求失败: {}", e);
                    }
                }
            }
            
            return Err(anyhow::anyhow!("上传到teletype.in失败: {} - {}", status, error_text));
        }
        
        let response_text = response.text().await?;
        debug!("Teletype上传成功: 响应: {}", response_text);
        
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
        
        debug!("提取的URL: {}", url);
        
        if url.is_empty() || !url.starts_with("http") {
            return Err(anyhow::anyhow!("提取的URL无效: {}", url));
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
            match self.upload_to_teletype(name, reader).await {
                Ok(url) => return Ok(url),
                Err(e) => {
                    debug!("Teletype上传失败，尝试备用上传方式: {}", e);
                    // 错误时继续执行下面的备用上传代码
                }
            }
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
