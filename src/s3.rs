use anyhow::Result;
use reqwest::Client;
use tokio::io::AsyncReadExt;

pub struct S3Uploader {
    client: Client,
}

impl S3Uploader {
    pub fn new() -> Result<Self> {
        let client = Client::new();
        Ok(Self { client })
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

        let hash = response["Hash"].as_str().ok_or(anyhow::anyhow!("Invalid response"))?;
        let url = format!("https://ipfs.io/ipfs/{}", hash);

        Ok(url)
    }
}
