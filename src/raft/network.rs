use actix::prelude::*;
use actix_raft::{RaftNetwork, messages as raft_protocol};
// use tracing::*;
use log::error;

use crate::network::{Network, remote::SendRemoteMessage};
use super::storage::StorageData as Data;

//todo: work out proper failure handling
const ERR_ROUTING_FAILURE: &str = "Failed to send RCP to node target.";

//////////////////////////////////////////////////////////////////////////////
// Impl RaftNetwork //////////////////////////////////////////////////////////
impl RaftNetwork<Data> for Network {}


impl Handler<raft_protocol::AppendEntriesRequest<Data>> for Network {
    type Result = ResponseActFuture<Self, raft_protocol::AppendEntriesResponse, ()>;

    fn handle(
        &mut self,
        msg: raft_protocol::AppendEntriesRequest<Data>,
        _ctx: &mut Self::Context
    ) -> Self::Result {
        let target_id = msg.target;
        if let Some(node) = self.get_node(msg.target) {
            if self.isolated_nodes.contains(&msg.target) || self.isolated_nodes.contains(&msg.leader_id) {
                return Box::new(fut::err(()));
            }

            let req = node.send(SendRemoteMessage(msg));

            return Box::new(
                fut::wrap_future(req)
                    .map_err(move |err, _, _| error!("{}[{}]: {:?}", ERR_ROUTING_FAILURE, target_id, err))
                    .and_then(|res, _, _| fut::result(res)),
            );
        }

        Box::new( fut::err(()))
    }
}


impl Handler<raft_protocol::VoteRequest> for Network {
    type Result = ResponseActFuture<Self, raft_protocol::VoteResponse, ()>;

    fn handle(
        &mut self,
        msg: raft_protocol::VoteRequest,
        _ctx: &mut Self::Context
    ) -> Self::Result {
        if let Some(node) = self.get_node(msg.target) {
            if self.isolated_nodes.contains(&msg.target) || self.isolated_nodes.contains(&msg.candidate_id) {
                return Box::new(fut::err(()));
            }

            let req = node.send(SendRemoteMessage(msg));

            return Box::new(
                fut::wrap_future(req)
                    .map_err(|err, _, _| error!("Vote Request - {}: {:?}", ERR_ROUTING_FAILURE, err))
                    .and_then(|res, _, _| fut::result(res)),
            );
        }

        Box::new(fut::err(()))
     }
}

impl Handler<raft_protocol::InstallSnapshotRequest> for Network {
    type Result = ResponseActFuture<Self, raft_protocol::InstallSnapshotResponse, ()>;

    fn handle(
        &mut self,
        msg: raft_protocol::InstallSnapshotRequest,
        _ctx: &mut Self::Context
    ) -> Self::Result {
        if let Some(node) = self.get_node(msg.target) {
            if self.isolated_nodes.contains(&msg.target) || self.isolated_nodes.contains(&msg.leader_id) {
                return Box::new(fut::err(()));
            }

            let req = node.send(SendRemoteMessage(msg));

            return Box::new(
                fut::wrap_future(req)
                    .map_err(|err, _, _| error!("Install Snapshot Request - {}: {:?}", ERR_ROUTING_FAILURE, err))
                    .and_then(|res, _, _| fut::result(res)),
            );
        }

        Box::new(fut::err(()))
    }
}
