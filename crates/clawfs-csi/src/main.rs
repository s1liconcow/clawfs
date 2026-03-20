use clap::Parser;
use clawfs_csi::csi::{identity_server, node_server};
use clawfs_csi::{ClawFSIdentityService, ClawFSNodeService};
use log::info;
use std::path::Path;
use tokio::net::UnixListener;
use tonic::transport::Server;
use tower::ServiceBuilder;

#[derive(Parser, Debug)]
#[command(name = "clawfs-csi")]
#[command(about = "CSI Node Driver for ClawFS", long_about = None)]
struct Args {
    /// Path to the CSI socket (Unix domain socket)
    #[arg(long, default_value = "/csi/csi.sock")]
    endpoint: String,

    /// Node ID for this CSI node
    #[arg(long)]
    node_id: String,

    /// Log level
    #[arg(long, default_value = "info")]
    log_level: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // Initialize logging
    env_logger::builder()
        .filter_level(args.log_level.parse()?)
        .format_timestamp_secs()
        .init();

    info!("Starting ClawFS CSI driver");
    info!("Node ID: {}", args.node_id);
    info!("Endpoint: {}", args.endpoint);

    // Remove existing socket if it exists
    if Path::new(&args.endpoint).exists() {
        std::fs::remove_file(&args.endpoint)
            .map_err(|e| format!("Failed to remove existing socket: {}", e))?;
    }

    // Create parent directory if it doesn't exist
    if let Some(parent) = Path::new(&args.endpoint).parent() {
        if !parent.exists() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("Failed to create socket directory: {}", e))?;
        }
    }

    // Bind Unix socket
    let listener = UnixListener::bind(&args.endpoint)
        .map_err(|e| format!("Failed to bind socket: {}", e))?;

    info!("Created Unix socket at {}", args.endpoint);

    // Create services
    let identity_service = ClawFSIdentityService::default();
    let node_service = ClawFSNodeService::new(args.node_id);

    info!("Starting gRPC server...");

    // Build and run server
    Server::builder()
        .layer(
            ServiceBuilder::new()
                .layer(tower_http::trace::TraceLayer::new_for_grpc())
                .into_inner(),
        )
        .add_service(identity_server::IdentityServer::new(identity_service))
        .add_service(node_server::NodeServer::new(node_service))
        .serve_with_incoming(tokio_stream::wrappers::UnixListenerStream::new(listener))
        .await?;

    Ok(())
}
