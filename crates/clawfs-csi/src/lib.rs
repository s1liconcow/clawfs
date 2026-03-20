pub mod csi {
    tonic::include_proto!("csi.v1");
}

pub mod node;
pub mod identity;

pub use node::ClawFSNodeService;
pub use identity::ClawFSIdentityService;
