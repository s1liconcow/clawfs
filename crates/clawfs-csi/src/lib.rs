pub mod csi {
    tonic::include_proto!("csi.v1");
}

pub mod identity;
pub mod node;

pub use identity::ClawFSIdentityService;
pub use node::ClawFSNodeService;
