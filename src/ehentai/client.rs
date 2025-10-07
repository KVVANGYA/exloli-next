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
use tracing::{debug, error, info, warn, Instrument};

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
pub struct EhClient {
    pub client: Client,
    pub cookie: String,
}

impl EhClient {
    /// 通用响应处理函数，处理各种压缩格式和内容验证
    async fn process_response(response: reqwest::Response, page_name: &str) -> Result<String> {
        let headers = response.headers().clone();
        
        // 检查内容编码
        let encoding = headers.get("content-encoding").map(|h| h.to_str().unwrap_or(""));
        if let Some(encoding_str) = encoding {
            debug!("{} 内容编码: {}", page_name, encoding_str);
        }
        
        // 对于zstd，我们需要手动处理，因为reqwest不支持
        // 对于其他格式（gzip, deflate, br），让reqwest自动处理
        let content = match encoding {
            Some("zstd") => {
                // zstd需要手动解压
                let bytes = response.bytes().await?;
                debug!("{} zstd压缩响应字节长度: {}", page_name, bytes.len());
                
                if bytes.is_empty() {
                    warn!("{} 收到空的zstd压缩数据", page_name);
                    String::new()
                } else {
                    debug!("{} zstd原始数据: {:?}", page_name, &bytes[..std::cmp::min(32, bytes.len())]);
                    match zstd::decode_all(&bytes[..]) {
                        Ok(decompressed) => {
                            debug!("{} zstd解压成功，解压后长度: {}", page_name, decompressed.len());
                            if decompressed.is_empty() && page_name.contains("画廊") {
                                warn!("{} zstd解压后内容为空，这可能表示画廊已被删除或访问权限问题", page_name);
                                warn!("原始zstd数据: {:?}", bytes);
                            }
                            String::from_utf8_lossy(&decompressed).to_string()
                        }
                        Err(e) => {
                            warn!("{} zstd解压失败: {}，使用原始数据", page_name, e);
                            warn!("失败的zstd数据: {:?}", &bytes[..std::cmp::min(32, bytes.len())]);
                            String::from_utf8_lossy(&bytes).to_string()
                        }
                    }
                }
            }
            _ => {
                // 其他格式（包括br, gzip, deflate）由reqwest自动处理
                match response.text().await {
                    Ok(text) => {
                        debug!("{} 响应内容长度: {} 字符（自动解压）", page_name, text.len());
                        
                        // 特殊调试：如果是画廊页面且内容为空，输出更多信息
                        if text.is_empty() && page_name.contains("画廊") {
                            warn!("{} 自动解压后内容为空，可能是服务器返回空响应", page_name);
                            if let Some(encoding_str) = encoding {
                                debug!("内容编码为: {}", encoding_str);
                            } else {
                                debug!("没有内容编码头");
                            }
                        }
                        
                        text
                    }
                    Err(e) => {
                        warn!("{} 读取响应文本失败: {}", page_name, e);
                        return Err(anyhow::anyhow!("读取响应内容失败: {}", e).into());
                    }
                }
            }
        };
        
        Ok(content)
    }

    #[tracing::instrument(skip(cookie))]
    pub async fn new(cookie: &str) -> Result<Self> {
        info!("登陆 E 站中");
        
        // 清理cookie字符串，移除换行符和多余空格，确保单行格式
        // 这是关键修复：HTTP头不能包含换行符，否则服务器返回0字节响应
        let cleaned_cookie = cookie
            .replace('\n', "")
            .replace('\r', "")
            .trim()
            .to_string();
        
        // 将 cookie 日志级别改为 debug，避免在生产环境泄露敏感信息
        debug!("原始 cookie: {}", cookie);
        debug!("清理后 cookie: {}", cleaned_cookie);
        
        // 设置完整的浏览器请求头，模拟真实浏览器行为
        let final_headers = headers! {
            ACCEPT => "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            ACCEPT_ENCODING => "gzip, deflate, br", 
            ACCEPT_LANGUAGE => "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            CACHE_CONTROL => "no-cache",
            PRAGMA => "no-cache",
            REFERER => "https://exhentai.org/",
            UPGRADE_INSECURE_REQUESTS => "1",
            USER_AGENT => "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0"
        };

        // 创建客户端
        let client = Client::builder()
            .cookie_store(true)
            .default_headers(final_headers)
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(30))
            .build()?;

        let eh_client = Self {
            client: client.clone(),
            cookie: cleaned_cookie.clone(),
        };
        
        // 需要通过访问特定页面来补全cookie
        debug!("开始cookie补全流程，基础cookie: {}", cleaned_cookie);
        
        // 步骤1: 访问首页设置基础cookie
        debug!("步骤1: 访问首页设置基础cookie");
        let initial_resp = eh_client.client
            .get("https://exhentai.org/")
            .header(reqwest::header::COOKIE, &cleaned_cookie)
            .send()
            .await;
            
        match initial_resp {
            Ok(response) => {
                debug!("首页请求成功，状态码: {:?}", response.status());
                
                // 检查是否被重定向到登录页面
                let final_url = response.url().as_str();
                if final_url.contains("bounce_login") || final_url.contains("login") {
                    return Err(anyhow::anyhow!("基础Cookie无效或已过期，被重定向到登录页面: {}", final_url).into());
                }
                
                // 检查响应头中的set-cookie
                let headers = response.headers();
                for cookie_header in headers.get_all("set-cookie") {
                    debug!("首页响应set-cookie: {:?}", cookie_header);
                }
                
                // 读取响应内容验证
                match Self::process_response(response, "首页").await {
                    Ok(content) => {
                        if content.len() < 1000 {
                            warn!("首页响应内容过短: {} 字符，可能是cookie失效", content.len());
                            debug!("首页响应内容: {:?}", &content[..std::cmp::min(200, content.len())]);
                            if content.contains("location.replace") || content.contains("bounce_login") {
                                return Err(anyhow::anyhow!("基础Cookie无效，首页收到重定向响应").into());
                            }
                        } else {
                            debug!("首页访问成功，响应内容长度: {} 字符", content.len());
                        }
                    }
                    Err(e) => {
                        warn!("读取首页响应内容失败: {}", e);
                    }
                }
            }
            Err(e) => {
                warn!("首页请求失败: {}", e);
                return Err(anyhow::anyhow!("无法连接到ExHentai服务器: {}", e).into());
            }
        }
        
        // 步骤2: 访问 /uconfig.php 获取用户配置相关cookie
        debug!("步骤2: 访问 /uconfig.php 补全cookie");
        let uconfig_resp = eh_client.client
            .get("https://exhentai.org/uconfig.php")
            .header("referer", "https://exhentai.org/")
            .header(reqwest::header::COOKIE, &cleaned_cookie)
            .send()
            .await;
            
        match uconfig_resp {
            Ok(response) => {
                debug!("uconfig.php 请求成功，状态码: {:?}", response.status());
                
                let headers = response.headers();
                for cookie_header in headers.get_all("set-cookie") {
                    debug!("uconfig.php set-cookie: {:?}", cookie_header);
                }
                
                // 检查响应内容
                match Self::process_response(response, "uconfig.php").await {
                    Ok(content) => {
                        if content.is_empty() {
                            debug!("uconfig.php 返回空内容，这对某些用户可能是正常的");
                        } else if content.len() < 100 {
                            warn!("uconfig.php 响应内容过短: {} 字符，内容: {:?}", content.len(), &content[..std::cmp::min(50, content.len())]);
                            if content.contains("location.replace") || content.contains("bounce_login") {
                                warn!("uconfig.php 重定向到登录页面，这可能影响cookie完整性");
                            }
                        } else if content.contains("bounce_login") || content.contains("login") {
                            warn!("uconfig.php 重定向到登录页面，cookie可能需要刷新");
                        } else {
                            debug!("uconfig.php 访问成功，响应长度: {} 字符", content.len());
                        }
                    }
                    Err(e) => {
                        warn!("读取 uconfig.php 响应失败: {}", e);
                    }
                }
            }
            Err(e) => {
                warn!("uconfig.php 请求失败: {}", e);
                // 不中断流程，继续下一步
            }
        }
        
        // 步骤3: 访问 /mytags 获取标签相关cookie
        debug!("步骤3: 访问 /mytags 补全cookie");
        let mytags_resp = eh_client.client
            .get("https://exhentai.org/mytags")
            .header("referer", "https://exhentai.org/")
            .header(reqwest::header::COOKIE, &cleaned_cookie)
            .send()
            .await;
            
        match mytags_resp {
            Ok(response) => {
                debug!("mytags 请求成功，状态码: {:?}", response.status());
                
                let headers = response.headers();
                for cookie_header in headers.get_all("set-cookie") {
                    debug!("mytags set-cookie: {:?}", cookie_header);
                }
                
                // 检查响应内容
                match Self::process_response(response, "mytags").await {
                    Ok(content) => {
                        if content.is_empty() {
                            debug!("mytags 返回空内容，这对某些用户可能是正常的");
                        } else if content.len() < 100 {
                            warn!("mytags 响应内容过短: {} 字符，内容: {:?}", content.len(), &content[..std::cmp::min(50, content.len())]);
                            if content.contains("location.replace") || content.contains("bounce_login") {
                                warn!("mytags 重定向到登录页面，这可能影响cookie完整性");
                            }
                        } else if content.contains("bounce_login") || content.contains("login") {
                            warn!("mytags 重定向到登录页面，cookie可能需要刷新");
                        } else {
                            debug!("mytags 访问成功，响应长度: {} 字符", content.len());
                        }
                    }
                    Err(e) => {
                        warn!("读取 mytags 响应失败: {}", e);
                    }
                }
            }
            Err(e) => {
                warn!("mytags 请求失败: {}", e);
                // 不中断流程
            }
        }
        
        // 步骤4: 最终验证 - 再次访问首页确认cookie完整性
        debug!("步骤4: 最终验证cookie完整性");
        let final_resp = eh_client.client
            .get("https://exhentai.org/")
            .header(reqwest::header::COOKIE, &cleaned_cookie)
            .send()
            .await;
            
        match final_resp {
            Ok(response) => {
                debug!("最终验证请求成功，状态码: {:?}", response.status());
                
                let final_url = response.url().as_str();
                if final_url.contains("bounce_login") || final_url.contains("login") {
                    return Err(anyhow::anyhow!("Cookie补全后仍然无效，被重定向到登录页面").into());
                }
                
                match Self::process_response(response, "最终验证").await {
                    Ok(content) => {
                        if content.len() < 1000 {
                            debug!("最终验证响应内容: {:?}", &content[..std::cmp::min(200, content.len())]);
                            
                            // 如果是空内容但之前首页验证成功，可能是session问题，但cookie应该已经补全
                            if content.is_empty() {
                                warn!("最终验证返回空内容，但首页验证已成功，cookie补全可能已完成");
                                info!("Cookie补全流程完成，尽管最终验证内容为空，但基于首页成功访问判断cookie有效");
                            } else {
                                // 检查是否是JavaScript重定向
                                if content.contains("location.replace") || content.contains("window.location") {
                                    return Err(anyhow::anyhow!("Cookie补全失败，收到重定向响应: {}", content).into());
                                }
                                // 检查是否是登录重定向
                                if content.contains("bounce_login") {
                                    return Err(anyhow::anyhow!("Cookie补全失败，被重定向到登录页面").into());
                                }
                                return Err(anyhow::anyhow!("Cookie补全失败，最终验证响应内容过短: {} 字符，内容前200字符: {:?}", content.len(), &content[..std::cmp::min(200, content.len())]).into());
                            }
                        } else {
                            info!("Cookie补全成功，最终验证通过，响应长度: {} 字符", content.len());
                        }
                    }
                    Err(e) => {
                        return Err(anyhow::anyhow!("最终验证失败: {}", e).into());
                    }
                }
            }
            Err(e) => {
                return Err(anyhow::anyhow!("最终验证请求失败: {}", e).into());
            }
        }

        Ok(eh_client)
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
        
        // 创建请求构建器并添加调试信息
        let request_builder = self.client.get(url).query(params).query(&[("next", next)])
            .header(reqwest::header::COOKIE, &self.cookie);
        debug!("请求构建器创建完成");
        
        let resp = send!(request_builder)?;
        debug!("收到响应，状态码: {:?}", resp.status());
        
        // 检查响应头中的set-cookie
        let headers = resp.headers();
        if let Some(cookie_headers) = headers.get_all("set-cookie").iter().next() {
            debug!("响应返回的 set-cookie 头: {:?}", cookie_headers);
        }
        
        // 检查请求是否被重定向
        let final_url = resp.url().as_str();
        if final_url != &full_url {
            debug!("请求被重定向，原始URL: {}, 最终URL: {}", full_url, final_url);
        }
        
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
        let resp = send!(self.client.get(url.url()))?;
        let html = Html::parse_document(&resp.text().await?);
        let onclick = html.select_attr("p.g2 a", "onclick").unwrap();

        let or = RE.captures(&onclick).and_then(|c| c.name("or")).unwrap().as_str();

        debug!("发送请求: {}?gid={}&token={}&or={}", archive_url, url.id(), url.token(), or);
        send!(self
            .client
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
            let request = self.client.get(&request_url)
                .header(reqwest::header::COOKIE, &self.cookie);
            
            debug!("准备发送请求到: {}", request_url);
            debug!("使用cookie: {}", &self.cookie);
            
            // 发送请求并获取响应
            let resp = send!(request)?;
            
            // 检查响应状态
            debug!("响应状态: {:?}", resp.status());
            debug!("响应URL: {}", resp.url());
            
            // 检查是否有重定向到首页或其他非画廊页面
            let final_url = resp.url().as_str();
            let request_url = url.url(); // 重新获取request_url用于比较
            if final_url != &request_url {
                debug!("请求被重定向，原始URL: {}, 最终URL: {}", request_url, final_url);
                if final_url == "https://exhentai.org/" || final_url == "https://e-hentai.org/" {
                    return Err(anyhow::anyhow!("请求被重定向到首页，cookie可能无效或已过期").into());
                }
                if final_url.contains("bounce_login") || final_url.contains("login") {
                    return Err(anyhow::anyhow!("请求被重定向到登录页面，cookie无效或已过期").into());
                }
            }
            
            // 获取响应头信息
            let headers = resp.headers().clone();
            debug!("响应头: {:?}", headers);
            
            // 检查响应中的set-cookie头
            if let Some(cookie_headers) = headers.get_all("set-cookie").iter().next() {
                debug!("响应返回的 set-cookie 头: {:?}", cookie_headers);
            }
            
            // 使用统一的响应处理函数，并添加特殊调试
            let mut content = Self::process_response(resp, "画廊页面").await?;
            
            // 调试：如果是画廊页面且内容为空，输出详细信息
            if content.is_empty() {
                error!("画廊页面返回空内容");
                debug!("请求URL: {}", request_url);
                debug!("响应头: {:?}", headers);
                return Err(anyhow::anyhow!("画廊页面返回空内容，可能是cookie问题或服务器状态问题").into());
            }
            
            debug!("响应内容长度: {}", content.len());
            
            // 如果内容长度很小，可能是错误页面
            if content.len() < 1000 {
                debug!("响应内容可能不是有效的画廊页面: {:?}", &content[..std::cmp::min(500, content.len())]);
                
                // 检查是否是重定向页面
                if content.contains("location.replace") || content.contains("redirect") {
                    return Err(anyhow::anyhow!("收到重定向响应，可能是cookie无效或已过期").into());
                }
                
                // 检查是否包含错误信息
                if content.contains("This gallery has been removed") || content.contains("Gallery Not Available") {
                    return Err(anyhow::anyhow!("画廊已被删除或不可用").into());
                }
                
                // 检查是否是权限错误
                if content.contains("You do not have permission") || content.contains("Access denied") {
                    return Err(anyhow::anyhow!("没有访问权限，可能需要更新cookie或账户权限不足").into());
                }
                
                // 检查是否是登录页面
                if content.contains("bounce_login") || content.contains("member login") {
                    return Err(anyhow::anyhow!("被重定向到登录页面，cookie可能已过期").into());
                }
                
                // 检查是否是二进制内容（可能是压缩内容未正确解压）
                if content.contains('\0') || content.chars().any(|c| c as u32 > 127) {
                    return Err(anyhow::anyhow!("收到二进制内容，可能是压缩内容未正确解压或权限不足").into());
                }
                
                return Err(anyhow::anyhow!("收到的响应内容不符合预期，可能是画廊已被删除、未授权访问或重定向").into());
            }
            
            // 检查内容是否是HTML（处理可能的BOM）
            let trimmed_content = content.trim_start();
            if !trimmed_content.starts_with("<!DOCTYPE html>") && !trimmed_content.starts_with("<html") {
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
            let resp = send!(self.client.get(next_page_url))?;
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
        let resp = send!(self.client.get(&page.url()))?;
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
            match self.client.get(&original_url).send().await {
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
        if send!(self.client.head(&url)).is_ok() {
            Ok((fileindex, url))
        } else if let Some(nl) = &nl {
            let page_with_nl = page.with_nl(nl);
            debug!("发送请求: {}", page_with_nl.url());
            let resp = send!(self.client.get(&page_with_nl.url()))?;
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