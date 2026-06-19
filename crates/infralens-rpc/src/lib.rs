pub mod internal {
    pub mod v1 {
        tonic::include_proto!("infralens.internal.v1");
    }
}

pub mod scatter_gather;
pub mod server;

pub use scatter_gather::ScatterGather;
