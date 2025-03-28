use anyhow::Result;
use reqwest::Client;
use tokio::io::AsyncReadExt;

pub struct S3Uploader {
    client: Client,
    gateway_host: String,
    gateway_date: String,
    auth_token: Option<String>,
}

impl S3Uploader {
    pub fn new(gateway_host: String, gateway_date: String, teletype_token: Option<String>) -> Result<Self> {
        let client = Client::new();
        Ok(Self { client, gateway_host, gateway_date, auth_token: teletype_token })
    }

    pub async fn upload_multiple<R: AsyncReadExt + Unpin>(
        &self,
        uploads: Vec<(&str, &mut R)>,
    ) -> Result<Vec<String>> {
        let mut urls = Vec::new();
        
        for (name, reader) in uploads {
            let url = if self.auth_token.is_some() {
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
        let token = self.auth_token.as_ref().ok_or(anyhow::anyhow!("Authorization token is required"))?;
        
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).await?;

        let form = reqwest::multipart::Form::new()
            .text("type", "images")
            .part("file", reqwest::multipart::Part::bytes(buffer).file_name(name.to_string()));

        let response = self
            .client
            .put("https://teletype.in/media/")
            .header("Authorization", token)
            .multipart(form)
            .send()
            .await?;
            
        if !response.status().is_success() {
            return Err(anyhow::anyhow!("Failed to upload to teletype.in: {}", response.status()));
        }
        
        let response_text = response.text().await?;
        let url = response_text.trim().to_string();
        
        if url.is_empty() {
            return Err(anyhow::anyhow!("Empty response URL from teletype.in"));
        }
        
        Ok(url)
    }

    pub async fn upload<R: AsyncReadExt + Unpin>(
        &self,
        name: &str,
        reader: &mut R,
    ) -> Result<String> {
        if self.auth_token.is_some() {
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
