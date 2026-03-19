use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use clawfs::relay::{RelayStatus, RelayWriteRequest, RelayWriteResponse};
use reqwest::Client;
use serde_json::Value;
use tempfile::TempDir;

struct RelayExecutorGuard {
    child: Child,
    _root: TempDir,
}

impl Drop for RelayExecutorGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn relay_executor_path() -> Result<std::path::PathBuf> {
    if let Some(path) = std::env::var_os("CARGO_BIN_EXE_clawfs_relay_executor") {
        return Ok(std::path::PathBuf::from(path));
    }

    let mut path = std::env::current_exe().context("locate integration test binary")?;
    path.pop(); // test binary name
    path.pop(); // deps
    path.push("clawfs_relay_executor");
    if cfg!(windows) {
        path.set_extension("exe");
    }
    Ok(path)
}

fn spawn_relay_executor(
    root: TempDir,
    listen_addr_file: &std::path::Path,
) -> Result<RelayExecutorGuard> {
    let exe = relay_executor_path()?;
    let store_path = root.path().join("store");
    let wal_path = root.path().join("wal");

    std::fs::create_dir_all(&store_path).context("create store dir")?;
    std::fs::create_dir_all(&wal_path).context("create wal dir")?;

    let child = Command::new(exe)
        .arg("--object-provider")
        .arg("local")
        .arg("--store-path")
        .arg(&store_path)
        .arg("--object-prefix")
        .arg("test")
        .arg("--listen")
        .arg("127.0.0.1:0")
        .env("CLAWFS_RELAY_LISTEN_FILE", listen_addr_file)
        .arg("--nvme-path")
        .arg(&wal_path)
        .arg("--flush-interval-ms")
        .arg("50")
        .arg("--max-buffer-entries")
        .arg("16")
        .arg("--relay-required")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .context("spawn relay executor")?;

    Ok(RelayExecutorGuard { child, _root: root })
}

async fn wait_for_health(client: &Client, base_url: &str) -> Result<Value> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let response = client.get(format!("{base_url}/health")).send().await;
        if let Ok(response) = response
            && response.status().is_success()
        {
            let json = response
                .json::<Value>()
                .await
                .context("decode health JSON")?;
            return Ok(json);
        }
        if Instant::now() >= deadline {
            anyhow::bail!("timed out waiting for relay executor health");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_listen_addr(path: &std::path::Path) -> Result<String> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(addr) = tokio::fs::read_to_string(path).await {
            let addr = addr.trim().to_string();
            if !addr.is_empty() {
                return Ok(addr);
            }
        }
        if Instant::now() >= deadline {
            anyhow::bail!("timed out waiting for relay executor listen address");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_writeback_acks_before_flush() -> Result<()> {
    let root = tempfile::tempdir().context("create temp root")?;
    let listen_addr_file = root.path().join("listen.addr");
    let _guard = spawn_relay_executor(root, &listen_addr_file)?;

    let client = Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .context("build reqwest client")?;
    let base_url = format!("http://{}", wait_for_listen_addr(&listen_addr_file).await?);

    let health = wait_for_health(&client, &base_url).await?;
    assert_eq!(health["status"], "ok");
    assert_eq!(health["mode"], "write_back");

    let initial_generation = health["generation"]
        .as_u64()
        .context("health generation missing")?;
    assert_eq!(health["write_back"]["flush_lag"].as_u64(), Some(0));
    assert_eq!(
        health["write_back"]["committed_gen"].as_u64(),
        Some(initial_generation)
    );
    assert_eq!(
        health["write_back"]["flusher_halted"].as_bool(),
        Some(false)
    );

    let request =
        RelayWriteRequest::new("test-client", "test", 1, initial_generation, vec![], vec![]);

    let start = Instant::now();
    let response = client
        .post(format!("{base_url}/relay_write"))
        .json(&request)
        .send()
        .await
        .context("submit relay_write")?;
    let elapsed = start.elapsed();

    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let body: RelayWriteResponse = response.json().await.context("decode relay response")?;
    assert_eq!(body.status, RelayStatus::Committed);
    assert_eq!(body.committed_generation, Some(initial_generation + 1));
    assert!(
        elapsed < Duration::from_millis(500),
        "write-back should acknowledge quickly; took {:?}",
        elapsed
    );

    let mut final_health = None;
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        let health = client
            .get(format!("{base_url}/health"))
            .send()
            .await
            .context("poll health")?
            .json::<Value>()
            .await
            .context("decode health poll")?;
        if health["write_back"]["flush_lag"].as_u64() == Some(0) {
            final_health = Some(health);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let health = final_health.context("timed out waiting for flush_lag=0")?;
    assert_eq!(health["status"], "ok");
    assert_eq!(health["mode"], "write_back");
    assert_eq!(health["generation"].as_u64(), Some(initial_generation + 1));
    assert_eq!(health["write_back"]["buffer_depth"].as_u64(), Some(0));
    assert_eq!(
        health["write_back"]["committed_gen"].as_u64(),
        Some(initial_generation + 1)
    );
    assert_eq!(health["write_back"]["flush_lag"].as_u64(), Some(0));
    assert_eq!(
        health["write_back"]["flusher_halted"].as_bool(),
        Some(false)
    );

    Ok(())
}
