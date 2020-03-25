use std::{
    time::{Duration, Instant},
    sync::{Arc, RwLock},
};
use actix::prelude::*;
use actix_raft::{
    admin::{InitWithConfig, ProposeConfigChange, ProposeConfigChangeError},
    messages::*,
    NodeId, Raft, RaftMetrics,
};
use tokio::time::delay_for;
use tracing::*;
use serde::{Serialize, Deserialize};
use crate::hash_ring::RingType;
use crate::network::{
    Network,
    DiscoverNodes, GetCurrentLeader, GetNodeById, HandlerRegistry,
    remote::SendRemoteMessage,
};
use crate::server::Server;
use super::{
  RaftBuilder, ActuatorRaft,
    storage::{StorageData, StorageError, StorageResponse},
};
use actix_web::client::ClientRequest;

type ClientResponseHandler = Result<
    ClientPayloadResponse<StorageResponse>,
    ClientError<StorageData, StorageResponse, StorageError>,
>;

pub type Payload = ClientPayload<StorageData, StorageResponse, StorageError>;

pub struct RaftClient {
    id: NodeId,
    ring: RingType,
    raft: Option<Addr<ActuatorRaft>>,
    registry: Arc<RwLock<HandlerRegistry>>,
    network: Option<Addr<Network>>,
}

impl Actor for RaftClient {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {}
}

impl RaftClient {
    pub fn new(id: NodeId, ring: RingType, registry: Arc<RwLock<HandlerRegistry>>) -> RaftClient {
        RaftClient {
            id: id,
            ring: ring,
            raft: None,
            registry: registry,
            network: None,
        }
    }

    fn register_handlers(&muty self, raft: Addr<ActuatorRaft>, client: Addr<Self>) {
        let mut registry = self.registry.write().unwrap();

        registry.register::<AppendEntriesRequest<StorageData>, _>(raft.clone());
        registry.register::<VoteRequest, _>(raft.clone());
        registry.register::<InstallSnapshotRequest, _>(raft.clone());
        registry.register::<ChangeRaftClusterConfig, _>(raft.clone());
        registry.register::<ClientPayload<StorageData, StorageResponse, StorageError>, _>(raft.clone());
    }
}

#[derive(Message)]
pub struct InitRaft {
    pub nodes: Vec<NodeId>,
    pub network: Addr<Network>,
    pub server: Addr<Server>,
    pub join_node: bool,
}

#[derive(Message)]
pub struct AddNode(pub NodeId);

#[derive(Serialize, Deserialize, Message, Clone)]
pub struct ChangeRaftClusterConfig(pub Vec<NodeId>, pub Vev<NodeId>);

impl Handler<ChangeRaftClusterConfig> for RaftClient {
    type Result = ();

    fn handle(&mut self, msg: ChangeRaftClusterConfig, ctx: &mut Self::Context) -> Self::Result {
        let nodes_to_add = msg.0.clone();
        let nodes_to_remove = msg.1.clone();

        let payload = ProposeConfigChange::new(nodes_to_add.clone(), nodes_to_remove.clone());

        ctx.spawn(
            fut::wrap_future::<_, Self>(self.network.as_ref().unwrap().send(GetCurrentLeader))
                .map_err(|err, _, _| panic!(err))
                .and_then(move |res, act, ctx| {
                    let leader = res.unwrap();

                    if leader == act.id {
                        if let Some(ref raft) = act.raft {
                            debug!(" ------------- About to propose config change");
                            return fut::Either::A(
                                fut::wrap_future::<_, Self>(raft.send(payload))
                                    .map_err(|err, _, _| panic!(err))
                                    .and_then(move |res, act, ctx| {
                                        for id in nodes_to_add.iter() {
                                            ctx.notify(AddNode(*id));
                                        }

                                        fut::ok(())
                                    } ),
                            );
                        }
                    }

                    fut::Either::B(
                        fut::wrap_future::<_, Self>(act.network.as_ref().unwrap().send(GetNodeById(leader)))
                            .map_err(move |err, _, _| panic!("Node {} not found", leader))
                            .and_then(move |node, act, ctx| {
                                debug!("-------------- Sending remote proposal to leader");

                                fut::wrap_future::<_, Self>(
                                    node.unwrap().send(SendRemoteMessage(msg.clone())),
                                )
                                    .map_err(|err, _, _| error!("Error {:?}", err))
                                    .and_then(|res, act, ctx| {
                                        fut::ok(())
                                    })
                            }),
                    )
                }),
        );
    }
}

pub struct RemoveNode(pub NodeId);

impl Handler<AddNode> for RaftClient {
    type Result = ();

    fn handle(&mut self, msg: AddNode, ctx: &mut Self::Context) -> Self::Result {
        let payload = add_node(msg.0);
        ctx.notify(ClientRequest(payload));
    }
}

impl Handler<RemoveNode> for RaftClient {
    type Result = ();

    fn handle(&mut self, msg: RemoveNode, ctx: &mut Self::Context) -> Self::Result {
        let payload = remove_node(msg.0);
        ctx.notify(ClientRequest(payload));
        ctx.notify(ChangeRaftClusterConfig(vec![], vec![msg.0]));
    }
}

impl Handler<InitRaft> for RaftClient {
    type Result = ();

    fn handle(&mut self, msg: InitRaft, ctx: &mut Self::Context) -> Self::Result {
        let nodes = msg.nodes;
        self.network = Some(msg.network);
        let server = msg.server;

        let nodes = if msg.join_node {
            vec![self.id]
        } else {
            nodes.clone()
        };

        let raft = RaftBuilder::new(
            self.id,
            nodes.clone(),
            self.network.as_ref().unwrap().clone(),
            self.ring.clone(),
            server
        );

        self.register_handlers(raft.clone(), ctx.address().clone());
        self.raft = Some(raft);

        if msg.join_node {
            return ();
        }

        fut::wrap_future::<_, Self>(delay_for(Duration::from_secs(5)))
            .map_err(|_, _, _| ())
            .and_then(move |_, act, ctx| {
                fut::wrap_future::<_, Self>(
                    act.raft
                        .as_ref()
                        .unwrap()
                        .send( InitWithConfig::new(nodes.clone())),
                )
                    .map_err(|err, _, _| panic!(err))
                    .and_then(|_, _, _| {
                        debug!("Initialized with config.");
                        fut::wrap_future::<_, Self>(delay_for(Duration::from_secs(5)))
                    })
                    .map_err(|_, _, _| ())
                    .and_then(|_, act, ctx| {
                        let payload = add_node(act.id);
                        ctx.notify(ClientRequest(payload));
                        fut::ok(())
                    })
            })
            .spawn(ctx);
    }
}

pub struct ClientRequest(pub StorageData);

impl Message for ClientRequest {
    type Result = ();
}

impl Handler<ClientRequest> for RaftClient {
    type Result = ();

    fn handle(&mut self, msg: ClientRequest, ctx: &mut Self::Context) -> Self::Result {
        let entry = EntryNormal {
            data: msg.0.clone(),
        };

        let payload = Payload::new(entry, ResponseMode::Applied);

        ctx.spawn(
            fut::wrap_future::<_, Self>(self.network.as_ref().unwrap().send(GetCurrentLeader))
                .map_err(|err, _, _| panic!(err))
                .and_then(move |res, act, ctx| {
                    let leader = res.unwrap();

                    if leader == act.id {
                        if let Some(ref raft) = act.raft {
                            return fut::Either::A(
                                fut::wrap_future::<_, Self>(raft.send(payload))
                                    .map_err(|err, _, _| panic!(err))
                                    .and_then(|res, act, ctx| {
                                        fut::ok(handle_client_response(res, ctx, msg))
                                    }),
                            );
                        }
                    }

                    fut::Either::B(
                        fut::wrap_future::<_, Self>(act.network.as_ref().unwrap().send(GetNodeById(leader)))
                            .map_err(move |err, _, _| panic!("Node {} not found.", leader))
                            .and_then(move |node, act, ctx| {
                                debug!("About to do something with node {}.", leader);

                                fut::wrap_future::<_, Self>(
                                    node.unwrap().send( SendRemoteMessage(payload)),
                                )
                                    .map_err(|err, _, _| error!("Error {:?}", err))
                                    .and_then(|res, act, ctx| {
                                        fut::ok(handle_client_response(res, ctx, msg))
                                    })
                            }),
                    )
                }),
        );
    }
}

fn add_node(id: NodeId) -> StorageData { StorageData::Add(id) }

fn remove_node(id: NodeId) -> StorageData { StorageData::Remove(id) }

fn handle_client_response(
    res: ClientResponseHandler,
    ctx: &mut Context<RaftClient>,
    msg: ClientRequest,
) {
    match res {
        Ok(_) => (),

        Err(err) => match err {
            ClientError::Internal => {
                error!("TEST: resending client request.");
                ctx.notify(msg);
            }

            ClientError::Application(err) => {
                error!("Unexpected application error from client request: {:?}", err);
            }

            ClientError::ForwardToLeader { .. } => {
                error!("TEST: received ForwardToLeader error. Updating leader and forwarding.");
                ctx.notify(msg);
            }
        },
    }
}
