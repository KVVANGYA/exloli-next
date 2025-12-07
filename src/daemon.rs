use anyhow::Result;
use tokio::process::Command;
use tokio::time::{sleep, Duration};
use tracing::{error, info, warn};
use std::env;

pub async fn start_daemon(app_path: &str) -> Result<()> {
    info!("守护进程启动，监控应用程序: {}", app_path);

    loop {
        info!("启动应用程序: {}", app_path);
        let mut child = Command::new(app_path)
            .env("IS_DAEMON_CHILD", "1") // 设置环境变量，指示子进程作为守护进程的子进程运行
            .spawn()
            .expect("无法启动应用程序");

        let status = child.wait().await.expect("等待应用程序失败");

        if !status.success() {
            error!("应用程序意外退出，状态: {:?}", status);
            warn!("将在 5 秒后重启应用程序...");
            sleep(Duration::from_secs(5)).await;
        } else {
            info!("应用程序正常退出，守护进程停止。");
            break;
        }
    }
    Ok(())
}
