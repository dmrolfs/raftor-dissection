use std::time::Duration;
use actix::prelude::*;
// use actix::{Arbiter, FinishStream};
use actix_raft::{Raft, NodeId, SnapshotPolicy, Config};
use tracing::*;
use tonic::{Request, Response, Status};
use tempfile::{tempdir_in, TempDir};

use crate::raft::network::{
    ActuatorRaft,
    router::RaftRouter,
};

use crate::api::raft::{
    router_server::{Router, RouterServer},
    RaftVoteRequest,
    RaftVoteResponse,
    RaftAppendEntriesRequest,
    RaftAppendEntriesResponse,
    RaftInstallSnapshotRequest,
    RaftInstallSnapshotResponse,
};
use actix_raft::messages::{
    VoteRequest,
    VoteResponse,
};


impl From<VoteRequest> for RaftVoteRequest {
    fn from(request: VoteRequest) -> Self {
        RaftVoteRequest {
            target: request.target,
            term: request.term,
            candidate_id: request.candidate_id,
            last_log_index: request.last_log_index,
            last_log_term: request.last_log_term,
        }
    }
}

impl From<VoteResponse> for RaftVoteResponse {
    fn from(response: VoteResponse) -> Self {
        RaftVoteResponse {
            term: response.term,
            vote_granted: response.vote_granted,
            is_candidate_unknown: response.is_candidate_unknown,
        }
    }
}


#[derive(Debug)]
pub struct RaftStubService {
    network: Addr<RaftRouter>,
}

#[tonic::async_trait]
impl Router for RaftStubService {
    /// An RPC invoked by the leader to replicate log entries (§5.3); also used as heartbeat (§5.2).
    #[instrument]
    async fn append_entries(
        &self,
        request: Request<RaftAppendEntriesRequest>,
    ) -> Result<Response<RaftAppendEntriesResponse>, Status> {
        event!( Level::INFO, ?request, "append entries to raft log and heartbeat");
        Ok( Response::new( RaftAppendEntriesResponse::default()))
    }

    /// Invoked by the Raft leader to send chunks of a snapshot to a follower (§7).
    ///
    /// The result type of calling with this message type is RaftInstallSnapshotResponse. The Raft
    /// spec assigns no significance to failures during the handling or sending of RPCs and all
    /// RPCs are handled in an idempotent fashion, so Raft will almost always retry sending a
    /// failed RPC, depending on the state of the Raft.
    #[instrument]
    async fn install_snapshot(
        &self,
        request: Request<RaftInstallSnapshotRequest>,
    ) -> Result<Response<RaftInstallSnapshotResponse>, Status> {
        event!( Level::INFO, ?request, "install snapshot from leader");
        Ok( Response::new( RaftInstallSnapshotResponse::default()))
    }

    /// An RPC invoked by candidates to gather votes (§5.2).
    ///
    /// The result type of calling the Raft actor with this message type is `RaftVoteResponse`.
    /// The Raft spec assigns no significance to failures during the handling or sending of RPCs and
    /// all RPCs are handled in an idempotent fashion, so Raft will almost always retry sending a
    /// failed RPC, depending on the state of the Raft.
    #[instrument]
    async fn vote(
        &self,
        request: Request<RaftVoteRequest>,
    ) -> Result<Response<RaftVoteResponse>, Status> {
        event!( Level::INFO, ?request, "vote called with request:VoteRequest");

        let payload = request.get_ref();
        let vr = VoteRequest {
            target: payload.target,
            term: payload.term,
            candidate_id: payload.candidate_id,
            last_log_index: payload.last_log_index,
            last_log_term: payload.last_log_term,
        };

        //todo: WORK HERE!!!

        // let bar = fut::wrap_future(
        //     self.network.send(vr)
        // )
        //     .map_err(|error, _, _|
        //          event!( Level::ERROR, ?error, ?request, "vote request failed in raft network");
        //         Err( Status::internal(format!("Vote request failed in raft network: {}", error)))
        //     );

        // // .map_err(|error, _: &mut Self, _|
        // );

        unimplemented!()
        // let bar = fut::wrap_future(
        //     self.network.send(vr.clone())
        // )
        //     .map_err(|error|
        //         // event!( Level::ERROR, ?error, %request, "vote request failed in raft network");
        //         Err( Status::internal(format!("Vote request failed in raft network: {}", error)))
        //     )
        //     .and_then(|res| res)
        //     .and_then( |resp| Response::new(resp));


        // let foo = fut::wrap_future( self.network.send(vr))
        //     .and_then(|error|
        //                // event!( Level::ERROR, ?error, %request, "vote request failed in raft network");
        //                 Err( Status::internal(format!("Vote request failed in raft network: {}", error)))
        //     )
        //     .and_then(|res| res);
        //
        //     self.network.send( vr.clone() ).await.unwrap();
        // Ok( Response::new(foo));
        // unimplemented!()
        // let task = fut::wrap_future( self.network.send(vr));
        // let t1 = task
        //     .map_err(|error, _, _|
        //         event!( Level::ERROR, ?error, %request, "vote request failed in raft network");
        // Err( Status::internal(format!("Vote request failed in raft network: {}", error)))
        // )
        // .and_then(|res, _, _| fut::result(res));

        // let task = fut::wrap_future(
        //   self.network.send(vr)
        // )
        //     .map_err( |error, _, _| panic!(error))
        //     .and_then(|res, _, _| fut::result(res));
        //
        // let ar = Arbiter::
        //
        // unimplemented!()
        // let task = fut::wrap_future(self.network.send(payload));
        //     .map_err(|error, _, _|
        //         // event!( Level::ERROR, ?error, %request, "vote request failed in raft network");
        //         Err( Status::internal(format!("Vote request failed in raft network: {}", error)))
        //     )
        // .and_then(|res, _, _| fut::result(Ok(Response::new(
        //     res.into::<RaftVoteResponse>()
        // ))))
        //
        // Ok( Response::new( RaftVoteResponse::default() ) )
    }
}


pub fn make_service(network: Addr<RaftRouter> ) -> RouterServer<RaftStubService> {
    let raft_stub = RaftStubService { network, };
    RouterServer::new( raft_stub )
}
