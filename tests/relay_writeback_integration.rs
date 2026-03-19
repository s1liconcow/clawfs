use std::path::PathBuf;
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

async fn free_port() -> Result<u16> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .context("bind ephemeral port")?;
    Ok(listener.local_addr().context("read ephemeral port")?.port())
}

fn relay_executor_path() -> Result<PathBuf> {
    if let Some(path) = std::env::var_os("CARGO_BIN_EXE_clawfs_relay_executor") {
        return Ok(PathBuf::from(path));
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

fn spawn_relay_executor(port: u16, root: TempDir) -> Result<RelayExecutorGuard> {
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
        .arg(format!("127.0.0.1:{port}"))
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

#[tokio::test(flavor = "multi_thread")]
async fn test_writeback_acks_before_flush() -> Result<()> {
    let root = tempfile::tempdir().context("create temp root")?;
    let port = free_port().await?;
    let _guard = spawn_relay_executor(port, root)?;

    let client = Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .context("build reqwest client")?;
    let base_url = format!("http://127.0.0.1:{port}");

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
