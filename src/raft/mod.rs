use std::time::Duration;
use actix::prelude::*;
// use actix::{Arbiter, FinishStream};
use actix_raft::{
    config::{Config, SnapshotPolicy},
    NodeId, Raft,
};
// use tonic::{Request, Response, Status};
use tempfile::tempdir_in;

use crate::hash_ring::RingType;
use crate::network::Network;
use crate::server::{Server};

use storage::{
    StorageData,
    StorageResponse,
    StorageError,
    Storage
};

pub use self::{
    client::{RaftClient, InitRaft, AddNode, RemoveNode, ChangeRaftClusterConfig}
};


pub mod network;
pub mod storage;
pub mod client;
// mod http_client;
// mod grpc_client;


/// Concrete Raft type pulling together the protocol implementations for the actuator
pub type ActuatorRaft = Raft<
    StorageData,
    StorageResponse,
    StorageError,
    Network,
    Storage
>;

pub struct RaftBuilder;

impl RaftBuilder {
    pub fn new(
        id: NodeId,
        members: Vec<NodeId>,
        network: Addr<Network>,
        ring: RingType,
        server: Addr<Server>,
    ) -> Addr<ActuatorRaft> {
        let id = id;
        let raft_members = members.clone();
        let metrics_rate = 5; //dmr 1;
        let temp_dir = tempdir_in("/tmp").expect("Tempdir to be created without error.");
        let snapshot_dir = temp_dir.path().to_string_lossy().to_string();
        let config = Config::build(snapshot_dir.clone())
            .election_timeout_min(3_000)
            .election_timeout_max(5_000)
            .heartbeat_interval(300)
            .metrics_rate(Duration::from_secs(metrics_rate))
            .snapshot_policy(SnapshotPolicy::default())
            .snapshot_max_chunk_size(10_000)
            .validate()
            .expect("Raft config to be created without error.");

        let storage = Storage::create(move |_| Storage::new(
            raft_members,
            snapshot_dir,
            ring,
            server
        ));

        let raft_network = network.clone(); //dmr why all the cloning!?
        let raft_storage  = storage.clone();  //dmr: why extra clone!?

        Raft::create(move |_| {
            Raft::new(
                id,
                config,
                raft_network.clone(), //dmr why all the cloning!?
                raft_storage,
                raft_network.recipient(),
            )
        })
    }
}
