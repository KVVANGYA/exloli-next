use chrono::prelude::*;
use futures::prelude::*;
use indexmap::IndexMap;
use once_cell::sync::Lazy;
use regex::Regex;
use reqwest::header::*;
use reqwest::Client;
use scraper::{Html, Selector};
use serde::Serialize;
use std::fmt::Debug;
use std::time::Duration;
use tracing::{debug, error, info, Instrument};

use super::error::*;
use super::types::*;
use crate::utils::html::SelectorExtend;

macro_rules! headers {
    ($($k:ident => $v:expr), *) => {{
        [
            $(($k, $v.parse().unwrap()),)*
        ].into_iter().collect::<HeaderMap>()
    }};
}

macro_rules! send {
    ($e:expr) => {{
        // 注意：我们无法直接从RequestBuilder获取URL，所以我们需要在调用宏之前记录URL
        $e.send().await.and_then(reqwest::Response::error_for_status)
    }};
}

macro_rules! selector {
    ($selector:tt) => {
        Selector::parse($selector).unwrap()
    };
}

#[derive(Debug, Clone)]
pub struct EhClient(pub Client);

impl EhClient {
    #[tracing::instrument(skip(cookie))]
    pub async fn new(cookie: &str) -> Result<Self> {
        info!("登陆 E 站中");
        // 将 cookie 日志级别改为 debug，避免在生产环境泄露敏感信息
        debug!("初始 cookie: {}", cookie);
        
        // 创建一个带 cookie store 的客户端
        let client = Client::builder()
            .cookie_store(true)
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(30))
            .build()?;

        // 手动设置cookie头部
        let cookie_value = HeaderValue::from_str(cookie).map_err(|e| anyhow::anyhow!("Invalid cookie: {}", e))?;
        let mut headers = HeaderMap::new();
        headers.insert(COOKIE, cookie_value);
        
        // 设置其他必要的请求头
        headers.insert(ACCEPT, HeaderValue::from_static("text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7"));
        headers.insert(ACCEPT_LANGUAGE, HeaderValue::from_static("zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6"));
        headers.insert(REFERER, HeaderValue::from_static("https://exhentai.org/"));
        headers.insert(USER_AGENT, HeaderValue::from_static("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0"));

        // 获取必要的 cookie
        debug!("访问 uconfig.php");
        let uconfig_url = "https://exhentai.org/uconfig.php";
        debug!("发送请求: {}", uconfig_url);
        let resp1 = send!(client.get(uconfig_url).headers(headers.clone()))?;
        debug!("uconfig.php 响应状态: {:?}", resp1.status());
        debug!("uconfig.php 响应URL: {}", resp1.url());
        
        // 检查uconfig.php响应中的set-cookie头
        if let Some(cookie_headers) = resp1.headers().get_all("set-cookie").iter().next() {
            debug!("uconfig.php 返回的 set-cookie 头: {:?}", cookie_headers);
        }
        
        debug!("访问 mytags");
        let mytags_url = "https://exhentai.org/mytags";
        debug!("发送请求: {}", mytags_url);
        let resp2 = send!(client.get(mytags_url).headers(headers))?;
        debug!("mytags 响应状态: {:?}", resp2.status());
        debug!("mytags 响应URL: {}", resp2.url());
        
        // 检查mytags响应中的set-cookie头
        if let Some(cookie_headers) = resp2.headers().get_all("set-cookie").iter().next() {
            debug!("mytags 返回的 set-cookie 头: {:?}", cookie_headers);
        }
        
        let mytags_content = resp2.text().await?;
        debug!("mytags 响应长度: {}", mytags_content.len());

        // 设置最终使用的默认头部，但不包括COOKIE，因为cookie应该由cookie store管理
        let final_headers = headers! {
            ACCEPT => "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            ACCEPT_ENCODING => "gzip, deflate, br, zstd", 
            ACCEPT_LANGUAGE => "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            CACHE_CONTROL => "no-cache",
            PRAGMA => "no-cache",
            REFERER => "https://exhentai.org/",
            UPGRADE_INSECURE_REQUESTS => "1",
            USER_AGENT => "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0"
        };

        // 重新构建客户端，使用相同的cookie store但设置默认头部
        let client = Client::builder()
            .cookie_store(true)
            .default_headers(final_headers)
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(30))
            .build()?;

        Ok(Self(client))
    }

    /// 访问指定页面，返回画廊列表
    #[tracing::instrument(skip(self, params))]
    async fn page<T: Serialize + ?Sized + Debug>(
        &self,
        url: &str,
        params: &T,
        next: &str,
    ) -> Result<(Vec<EhGalleryUrl>, Option<String>)> {
        let full_url = format!("{}?next={}", url, next);
        debug!("发送请求: {}", full_url);
        let resp = send!(self.0.get(url).query(params).query(&[("next", next)]))?;
        let html = Html::parse_document(&resp.text().await?);

        let selector = selector!("table.itg.gltc tr");
        let gl_list = html.select(&selector);

        let mut ret = vec![];
        // 第一个是 header
        for gl in gl_list.skip(1) {
            let title = gl.select_text("td.gl3c.glname a div.glink").unwrap();
            let url = gl.select_attr("td.gl3c.glname a", "href").unwrap();
            debug!(url, title);
            ret.push(url.parse()?)
        }

        let next = html
            .select_attr("a#dnext", "href")
            .and_then(|s| s.rsplit('=').next().map(|s| s.to_string()));

        Ok((ret, next))
    }

    /// 搜索前 N 页的本子，返回一个异步迭代器
    #[tracing::instrument(skip(self, params))]
    pub fn search_iter<'a, T: Serialize + ?Sized + Debug>(
        &'a self,
        params: &'a T,
    ) -> impl Stream<Item = EhGalleryUrl> + 'a {
        self.page_iter("https://exhentai.org", params)
    }

    /// 获取指定页面的画廊列表，返回一个异步迭代器
    #[tracing::instrument(skip(self, params))]
    pub fn page_iter<'a, T: Serialize + ?Sized + Debug>(
        &'a self,
        url: &'a str,
        params: &'a T,
    ) -> impl Stream<Item = EhGalleryUrl> + 'a {
        stream::unfold(Some("0".to_string()), move |next| {
            async move {
                match next {
                    None => None,
                    Some(next) => match self.page(url, params, &next).await {
                        Ok((gls, next)) => {
                            debug!("下一页 {:?}", next);
                            Some((stream::iter(gls), next))
                        }
                        Err(e) => {
                            error!("search error: {}", e);
                            None
                        }
                    },
                }
            }
            .in_current_span()
        })
        .flatten()
    }

    #[tracing::instrument(skip(self))]
    pub async fn archive_gallery(&self, url: &EhGalleryUrl) -> Result<()> {
        static RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"or=(?P<or>[0-9a-z-]+)").unwrap());

        let archive_url = "https://exhentai.org/archiver.php";
        debug!("发送请求: {}", archive_url);
        let resp = send!(self.0.get(url.url()))?;
        let html = Html::parse_document(&resp.text().await?);
        let onclick = html.select_attr("p.g2 a", "onclick").unwrap();

        let or = RE.captures(&onclick).and_then(|c| c.name("or")).unwrap().as_str();

        debug!("发送请求: {}?gid={}&token={}&or={}", archive_url, url.id(), url.token(), or);
        send!(self
            .0
            .post(archive_url)
            .query(&[("gid", &*url.id().to_string()), ("token", url.token()), ("or", or)])
            .form(&[("hathdl_xres", "org")]))?;

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn get_gallery(&self, url: &EhGalleryUrl) -> Result<EhGallery> {
        // NOTE: 由于 Html 是 !Send 的，为了避免它被包含在 Future 上下文中，这里将它放在一个单独的作用域内
        // 参见：https://rust-lang.github.io/async-book/07_workarounds/03_send_approximation.html
        let (title, title_jp, parent, tags, favorite, mut pages, posted, mut next_page) = {
            debug!("发送请求: {}", url.url());
            
            // 添加更多调试信息
            let request_url = url.url();
            let request = self.0.get(&request_url);
            
            // 检查当前客户端的cookie
            debug!("准备发送请求到: {}", request_url);
            
            // 输出请求头信息
            let resp = send!(request)?;
            
            // 检查响应状态
            debug!("响应状态: {:?}", resp.status());
            debug!("响应URL: {}", resp.url());
            
            // 检查响应头
            debug!("响应头: {:?}", resp.headers());
            
            // 检查响应中的set-cookie头
            if let Some(cookie_headers) = resp.headers().get_all("set-cookie").iter().next() {
                debug!("响应返回的 set-cookie 头: {:?}", cookie_headers);
            }
            
            // 检查是否有重定向到首页或其他非画廊页面
            let final_url = resp.url().as_str();
            if final_url != &request_url {
                debug!("请求被重定向，原始URL: {}, 最终URL: {}", request_url, final_url);
                if final_url == "https://exhentai.org/" || final_url == "https://e-hentai.org/" {
                    return Err(anyhow::anyhow!("请求被重定向到首页，cookie可能无效或已过期").into());
                }
                if final_url.contains("bounce_login") || final_url.contains("login") {
                    return Err(anyhow::anyhow!("请求被重定向到登录页面，cookie无效或已过期").into());
                }
            }
            
            // 获取响应内容
            let bytes = resp.bytes().await?;
            debug!("响应字节长度: {}", bytes.len());
            
            // 检查内容编码并尝试解压
            let content = if let Some(encoding) = resp.headers().get("content-encoding") {
                let encoding_str = encoding.to_str().unwrap_or("");
                debug!("内容编码: {}", encoding_str);
                
                match encoding_str {
                    "zstd" => {
                        // 尝试解压zstd内容
                        match zstd::decode_all(&bytes[..]) {
                            Ok(decompressed) => {
                                debug!("zstd解压成功，解压后长度: {}", decompressed.len());
                                String::from_utf8_lossy(&decompressed).to_string()
                            }
                            Err(e) => {
                                debug!("zstd解压失败: {}", e);
                                String::from_utf8_lossy(&bytes).to_string()
                            }
                        }
                    }
                    _ => String::from_utf8_lossy(&bytes).to_string()
                }
            } else {
                String::from_utf8_lossy(&bytes).to_string()
            };
            
            debug!("响应内容长度: {}", content.len());
            
            // 如果内容长度很小，可能是错误页面
            if content.len() < 1000 {
                debug!("响应内容可能不是有效的画廊页面: {:?}", &content);
                // 检查是否是重定向页面
                if content.contains("location.replace") || content.contains("redirect") {
                    return Err(anyhow::anyhow!("收到重定向响应，可能是cookie无效或已过期").into());
                }
                // 检查是否是二进制内容（可能是压缩内容未正确解压）
                if content.contains('\0') || content.chars().any(|c| c as u32 > 127) {
                    return Err(anyhow::anyhow!("收到二进制内容，可能是压缩内容未正确解压或权限不足").into());
                }
                return Err(anyhow::anyhow!("收到的响应内容不符合预期，可能是未授权访问或重定向").into());
            }
            
            // 检查内容是否是HTML
            if !content.trim_start().starts_with("<!DOCTYPE html>") && !content.trim_start().starts_with("<html") {
                debug!("响应内容不是HTML格式: {}", &content[..std::cmp::min(500, content.len())]);
                return Err(anyhow::anyhow!("收到的响应不是HTML格式，可能是未授权访问").into());
            }
            
            let html = Html::parse_document(&content);

            // 英文标题、日文标题、父画廊
            let title = html.select_text("h1#gn").ok_or_else(|| {
                // 如果找不到标题，输出更多调试信息
                debug!("无法找到画廊标题 (h1#gn)，页面HTML结构可能不正确");
                debug!("页面前1000个字符: {}", &content[..std::cmp::min(1000, content.len())]);
                anyhow::anyhow!("无法找到画廊标题 (h1#gn)")
            })?;
            let title_jp = html.select_text("h1#gj");
            let parent = html.select_attr("td.gdt2 a", "href").and_then(|s| s.parse().ok());

            // 画廊 tag
            let mut tags = IndexMap::new();
            let selector = selector!("div#taglist tr");
            for ele in html.select(&selector) {
                let namespace = html.select_text("td.tc")
                    .ok_or_else(|| anyhow::anyhow!("无法找到标签命名空间 (td.tc)"))?
                    .trim_matches(':')
                    .to_string();
                let tag = ele.select_texts("td div a");
                tags.insert(namespace, tag);
            }

            // 收藏数量
            let favorite_text = html.select_text("#favcount").ok_or_else(|| anyhow::anyhow!("无法找到收藏数量 (#favcount)"))?;
            let favorite = favorite_text.split(' ').next().unwrap().parse()
                .map_err(|_| anyhow::anyhow!("无法解析收藏数量: {}", favorite_text))?;

            // 发布时间
            let gdt2_texts = html.select_texts("td.gdt2");
            if gdt2_texts.is_empty() {
                return Err(anyhow::anyhow!("无法找到画廊信息 (td.gdt2)").into());
            }
            let posted = &gdt2_texts[0];
            let posted = NaiveDateTime::parse_from_str(posted, "%Y-%m-%d %H:%M")
                .map_err(|_| anyhow::anyhow!("无法解析发布时间: {}", posted))?;

            // 每一页的 URL
            let pages = html.select_attrs("div#gdt a", "href");

            // 下一页的 URL
            let next_page = html.select_attr("table.ptb td:last-child a", "href");

            (title, title_jp, parent, tags, favorite, pages, posted, next_page)
        };

        while let Some(next_page_url) = &next_page {
            debug!("发送请求: {}", next_page_url);
            let resp = send!(self.0.get(next_page_url))?;
            let html = Html::parse_document(&resp.text().await?);
            // 每一页的 URL
            pages.extend(html.select_attrs("div#gdt a", "href"));
            // 下一页的 URL
            next_page = html.select_attr("table.ptb td:last-child a", "href");
        }

        let pages = pages.into_iter().map(|s| s.parse()).collect::<Result<Vec<_>>>()?;
        info!("图片数量：{}", pages.len());

        let cover = url.cover();

        Ok(EhGallery {
            url: url.clone(),
            title,
            title_jp,
            parent,
            tags,
            favorite,
            pages,
            posted,
            cover,
        })
    }

    /// 获取画廊的某一页的图片的 fileindex 和实际地址和 nl
    #[tracing::instrument(skip(self))]
    pub async fn get_image_url(&self, page: &EhPageUrl) -> Result<(u32, String)> {
        debug!("发送请求: {}", page.url());
        let resp = send!(self.0.get(&page.url()))?;
        let (original_url, url, nl, fileindex) = {
            let html = Html::parse_document(&resp.text().await?);

            // 优先尝试获取原图链接 (div#i6 div a[href*="fullimg"])
            let original_url = html
                .select(&selector!("div#i6 div a[href*=\"fullimg\"]"))
                .next()
                .and_then(|ele| ele.value().attr("href"))
                .map(|s| s.to_string());

            let url = html.select_attr("img#img", "src").unwrap();
            let nl = html.select_attr("img#img", "onerror").and_then(extract_nl);
            let fileindex = extract_fileindex(&url).unwrap();
            (original_url, url, nl, fileindex)
        };

        // 优先使用原图链接
        if let Some(original_url) = original_url {
            debug!("发现原图链接: {}", original_url);
            // 获取302跳转后的真实URL
            match self.0.get(&original_url).send().await {
                Ok(resp) => {
                    let final_url = resp.url().to_string();
                    debug!("原图跳转后的URL: {}", final_url);
                    return Ok((fileindex, final_url));
                }
                Err(e) => {
                    debug!("原图链接请求失败: {}, 降级使用普通图片", e);
                    // 如果原图链接失效，降级使用普通图片
                }
            }
        }

        self.fallback_to_normal_image(page, url, nl, fileindex).await
    }

    /// 降级使用普通图片
    async fn fallback_to_normal_image(
        &self,
        page: &EhPageUrl,
        url: String,
        nl: Option<String>,
        fileindex: u32,
    ) -> Result<(u32, String)> {
        debug!("发送 HEAD 请求: {}", url);
        if send!(self.0.head(&url)).is_ok() {
            Ok((fileindex, url))
        } else if let Some(nl) = &nl {
            let page_with_nl = page.with_nl(nl);
            debug!("发送请求: {}", page_with_nl.url());
            let resp = send!(self.0.get(&page_with_nl.url()))?;
            let html = Html::parse_document(&resp.text().await?);
            let url = html.select_attr("img#img", "src").unwrap();
            Ok((fileindex, url))
        } else {
            Err(EhError::HaHUrlBroken(url))
        }
    }
}

fn extract_fileindex(url: &str) -> Option<u32> {
    static RE1: Lazy<Regex> = Lazy::new(|| Regex::new(r"fileindex=(?P<fileindex>\d+)").unwrap());
    static RE2: Lazy<Regex> = Lazy::new(|| Regex::new(r"/om/(?P<fileindex>\d+)/").unwrap());
    let captures = RE1.captures(url).or_else(|| RE2.captures(url))?;
    let fileindex = captures.name("fileindex")?.as_str().parse().ok()?;
    Some(fileindex)
}

fn extract_nl(onerror: String) -> Option<String> {
    static RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"nl\('(?P<nl>.+)'\)").unwrap());
    let captures = RE.captures(&onerror)?;
    Some(captures.name("nl")?.as_str().to_string())
}