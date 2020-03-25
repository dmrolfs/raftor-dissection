use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, RwLock},
    time::Duration,
    time::Instant, //dmr
};

use actix_web::client::Client;
use actix::prelude::*;
use actix_raft::{
    NodeId,
    metrics::RaftMetrics,
};

use tokio::io::AsyncRead;
use tokio::net::{TcpListener, TcpStream};
//dmr use tokio::time::delay_for;
use tokio::timer::Delay; //dmr
//dmr use tokio_util::codec::FramedRead;
use tokio::codec::FramedRead; //dmr
// use tracing::*;
use log::{debug, error, info, warn};

use serde::{Serialize, Deserialize, de::DeserializeOwned};

// use tonic::{Request, Response, Status};
use crate::raft::client::{RaftClient, RemoveNode};
use crate::config::{NetworkType, NodeInfo, ConfigSchema,};
use crate::hash_ring::RingType;
use crate::server;
use crate::utils::generate_node_id;

use remote::{RemoteMessage, SendRemoteMessage, DispatchMessage};


mod codec;
mod node;
mod recipient;
pub mod remote;
mod node_session;

pub use self::codec::{ClientNodeCodec, NodeCodec, NodeRequest, NodeResponse};
// pub use self::{
//     DiscoverNodes, DistributeMessage, GetCurrentLeader, GetNode, GetNodeAddr, GetNodeById, Network, PeerConnected, DistributeAndWait, NodeDisconnect, RestoreNode, GetNodes, GetClusterState, SetClusterState, NetworkState, Handshake,
// };
pub use self::node::Node;
pub use self::recipient::{HandlerRegistry, Provider, RemoteMessageHandler};
pub use self::node_session::NodeSession;

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub enum NetworkState {
    Initialized,
    SingleNode,
    Cluster,
}

/// An actor which implements the network transport and implements the `RaftNetwork` trait.
// #[derive(Default)]
pub struct Network {
    id: NodeId,
    net_type: NetworkType,      //todo: how is this used?
    address: Option<String>,    //todo: how is this used? Better Type?
    discovery_host: String,     //todo: how is this used? Better Type?
    peers: Vec<String>,         //todo: how is this used? Better Type?
    nodes: BTreeMap<NodeId, Addr<Node>>,
    nodes_connected: Vec<NodeId>,
    /// Nodes which are isolated can neither send nor receive frames.
    pub isolated_nodes: Vec<NodeId>,
    nodes_info: HashMap<NodeId, NodeInfo>,
    server: Option<Addr<server::Server>>, //todo: how is this used? Better Type?
    state: NetworkState,
    metrics: Option<RaftMetrics>,
    sessions: BTreeMap<NodeId, Addr<NodeSession>>, //todo: how is this used? Better Type?
    ring: RingType,
    raft: Addr<RaftClient>,
    registry: Arc<RwLock<HandlerRegistry>>,//todo: how is this used?
    info: NodeInfo,
    join_mode: bool, //todo: how is this used? Better Type?
    //
    //
    // pub routing_table: BTreeMap<NodeId, Addr<ActuatorRaft>>,
    // /// The count of all messages which have passed through this system.
    // routed: (u64, u64, u64, u64), // AppendEntries, Vote, InstallSnapshot, Other.
}

impl std::fmt::Debug for Network {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Network(id:{:?}, state:{:?}, net_type:{:?}, info:{:?}, nodes:{:?}, isolated_nodes:{:?}, metrics:{:?}",
            self.id,
            self.state,
            self.info,
            self.net_type,
            self.nodes_connected,
            self.isolated_nodes,
            self.metrics,
        )
    }
}

impl Network {
    pub fn new(
        id: NodeId,
        ring: RingType,
        registry: Arc<RwLock<HandlerRegistry>>,
        net_type: NetworkType,
        raft: Addr<RaftClient>,
        discovery_host: String,
        info: NodeInfo,
    ) -> Network {
        Network {
            id: id,
            address: None,
            net_type: net_type,
            discovery_host: discovery_host,
            peers: Vec::new(),
            nodes: BTreeMap::new(),
            nodes_connected: Vec::new(),
            isolated_nodes: Vec::new(),
            nodes_info: HashMap::new(),
            server: None,
            state: NetworkState::Initialized,
            metrics: None,
            sessions: BTreeMap::new(),
            ring: ring,
            raft: raft,
            registry: registry,
            info: info,
            join_mode: false,
        }
    }

    pub fn configure(&mut self, config: ConfigSchema) {
        let nodes = config.nodes;

        for node in nodes.iter() {
            let id = generate_node_id(node.cluster_addr.as_str());
            self.nodes_info.insert(id, node.clone());
        }
    }

    /// Register a new node to the network.
    pub fn register_node(&mut self, id: NodeId, info: &NodeInfo, addr: Addr<Self>) {
        let info = info.clone();

        let network_address = self.address.as_ref().unwrap().clone();
        let local_id = self.id;
        let net_type = self.net_type.clone();
        let peer_addr = match self.net_type {
            NetworkType::App => info.app_addr.clone(),
            NetworkType::Cluster => info.cluster_addr.clone(),
        };

        if peer_addr == *network_address {
            return ();
        }

        self.restore_node(id); // restore node if needed

        if !self.nodes.contains_key(&id) {
            let node = Node::new(
                id, local_id, peer_addr, addr, net_type, self.info.clone()
            ).start();

            self.nodes.insert(id, node);
        }
    }

    /// Get a node from the network by its id.
    pub fn get_node(&self, id: NodeId) -> Option<&Addr<Node>> { self.nodes.get(&id) }

    pub fn bind(&mut self, address: &str) { self.address = Some(address.to_owned()); }

    /// Isolate the network of the specified node.
    pub fn isolate_node(&mut self, id: NodeId) {
        if let Some((idx, _)) = self.isolated_nodes.iter().enumerate().find(|(_, e)| *e == &id) {
            debug!("Network node already isolated {} idx:{}.", &id, idx);
            return ();
        }

        debug!("Isolating network for node {}.", &id);
        self.isolated_nodes.push(id);
    }

    /// Restore the network of the specified node.
    pub fn restore_node(&mut self, id: NodeId) {
        if let Some((idx, _)) = self.isolated_nodes.iter().enumerate().find(|(_, e)| *e == &id) {
            debug!("Restoring network for node {}.", &id);
            self.isolated_nodes.remove(idx);
        }
    }
}

#[derive(Message)]
pub struct NodeDisconnect(pub NodeId);

impl Handler<NodeDisconnect> for Network {
    type Result = ();

    fn handle(&mut self, msg: NodeDisconnect, ctx: &mut Self::Context) -> Self::Result {
        let id = msg.0;
        self.isolated_nodes.push(id);
        self.nodes_info.remove(&id);
        self.nodes.remove(&id);

        if self.net_type != NetworkType::Cluster {
            return ();
        }

        // RemoveNove only if node is leader
        fut::wrap_future::<_, Self>( ctx.address().send(GetCurrentLeader))
            .map_err(|_, _, _| ())
            .and_then(move |res, net, ctx| {
                let leader = res.unwrap();

                if leader == id {
                    warn!("?INFINITE_LOOP?: NodeDisconnect Leader: {} == {}", leader, id);
                    //dmr fut::wrap_future::<_, Self>(delay_for(Duration::from_secs(1)))
                    fut::wrap_future::<_, Self>(Delay::new(Instant::now() + Duration::from_secs(1))) //dmr
                        .map_err(|_, _, _| ())
                        .and_then(|_, _net, ctx| {
                            ctx.notify(msg);
                            fut::ok(())
                        })
                        .spawn(ctx);

                    return fut::ok(());
                }

                if leader == net.id {
                    Arbiter::spawn(net.raft.send(RemoveNode(id))
                        .map_err(|_| ())
                        .and_then(|_| {
                            futures::future::ok(())
                        }));
                }

                fut::ok(())
            })
            .spawn(ctx);
    }
}

#[derive(Message)]
pub struct Handshake(pub NodeId, pub NodeInfo);

impl Handler<Handshake> for Network {
    type Result = ();

    fn handle(&mut self, msg: Handshake, ctx: &mut Self::Context) -> Self::Result {
        self.nodes_info.insert(msg.0, msg.1.clone());
        self.register_node(msg.0, &msg.1, ctx.address().clone());
    }
}

#[derive(Message)]
pub struct RestoreNode(pub NodeId);

impl Handler<RestoreNode> for Network {
    type Result = ();

    fn handle(&mut self, msg: RestoreNode, _ctx: &mut Self::Context) -> Self::Result {
        let id = msg.0;
        self.restore_node(id);
    }
}

pub struct GetClusterState;

impl Message for GetClusterState {
    type Result = Result<NetworkState, ()>;
}

impl Handler<GetClusterState> for Network {
    type Result = Result<NetworkState, ()>;

    fn handle(&mut self, _msg: GetClusterState, _ctx: &mut Self::Context) -> Self::Result {
        Ok(self.state.clone())
    }
}

#[derive(Message)]
pub struct SetClusterState(pub NetworkState);

impl Handler<SetClusterState> for Network {
    type Result = ();

    fn handle(&mut self, msg: SetClusterState, _ctx: &mut Self::Context) -> Self::Result {
        self.state = msg.0;
    }
}

pub struct GetNodes;

impl Message for GetNodes {
    type Result = Result<HashMap<NodeId, NodeInfo>, ()>;
}

impl Handler<GetNodes> for Network {
    type Result = Result<HashMap<NodeId, NodeInfo>, ()>;

    fn handle(&mut self, _: GetNodes, _ctx: &mut Self::Context) -> Self::Result {
        let nodes = self.nodes_info.clone();
        Ok(nodes)
    }
}

pub struct DiscoverNodes;

impl Message for DiscoverNodes {
    type Result = Result<(Vec<NodeId>, bool), ()>;
}

impl Handler<DiscoverNodes> for Network {
    type Result = ResponseActFuture<Self, (Vec<NodeId>, bool), ()>;

    fn handle(&mut self, _: DiscoverNodes, _ctx: &mut Self::Context) -> Self::Result {
        Box::new(
            //dmr fut::wrap_future::<_, Self>(delay_for(Duration::from_secs(5)))
            fut::wrap_future::<_, Self>(Delay::new(Instant::now() + Duration::from_secs(5))) //dmr
                .map_err(|_, _, _| ())
                .and_then(|_, net: &mut Network, _|
                    fut::result(
                        Ok((net.nodes_connected.clone(), net.join_mode))
                    )
                ),
        )
    }
}


impl Actor for Network {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        let network_address = self.address.as_ref().unwrap().clone();

        //todo convert to grpc?
        let cluster_state_route = format!("http://{}/cluster/state", self.discovery_host.as_str());
        let cluster_nodes_route = format!("http://{}/cluster/nodes", self.discovery_host.as_str());

        println!("Listening on {}", network_address);
        info!("Listening on {}", network_address);
        println!("Local node id: {}", self.id);
        info!("Local node id: {}", self.id);

        //todo convert to grpc server
        self.listen(ctx);
        self.nodes_connected.push(self.id);

        //todo convert to instantiate grpc client
        //dmr let mut client = Client::default();
        let client = Client::default(); //dmr

        fut::wrap_future::<_, Self>(client.get(cluster_state_route).send())
            .map_err(|_, _, _| ())
            .and_then(move |res, _, _| {
                let mut res = res;

                fut::wrap_future::<_, Self>(res.body()).then(move |resp, _, _| {
                    if let Ok(body) = resp {
                        // let state = serde_cbor::from_slice::<Result<NetworkState, ()>>(&body)
                        //     .unwrap().unwrap();
                        let state = serde_json::from_slice::<Result<NetworkState, ()>>(&body)
                            .unwrap().unwrap();

                        if state == NetworkState::Cluster {
                            //todo: Send register commend to cluster
                            return fut::Either::A(fut::wrap_future::<_, Self>(client.get(cluster_nodes_route).send())
                                .map_err(|err, _, _| error!("HTTP Cluster Error {:?}", err))
                                .and_then(|res, _net, _| {
                                    let mut res = res;

                                    fut::wrap_future::<_, Self>(res.body()).then(|resp, net, _| {
                                        if let Ok(body) = resp {
                                            // let mut nodes = serde_cbor::from_slice::<Result<HashMap<NodeId, NodeInfo>, ()>>(&body)
                                            //     .unwrap().unwrap();
                                            let mut nodes = serde_json::from_slice::<Result<HashMap<NodeId, NodeInfo>, ()>>(&body)
                                                .unwrap().unwrap();

                                            nodes.insert(net.id, net.info.clone());

                                            net.nodes_info = nodes;
                                            net.join_mode = true;
                                        }

                                        fut::ok(())
                                    })
                                })
                                .and_then(|_, _, _| fut::ok(()))
                            );
                        }
                    }

                    fut::Either::B(fut::ok(()))
                })
            })
            .map_err(|err, _, _| error!("HTTP Cluster Error {:?}", err))
            .and_then(move |_, net, ctx| {
                let nodes = net.nodes_info.clone();

                for (id, info) in &nodes {
                    net.register_node(*id, info, ctx.address().clone());
                }

                fut::ok(())
            })
            .spawn(ctx);
    }


    // fn started(&mut self, ctx: &mut Self::Context) {
    //     ctx.run_interval( Duration::from_secs(1), |act, _| {
    //         debug!(
    //             "RaftRouter [AppendEntries={}, Vote={}, InstallSnapshot={}, Other={}]",
    //             act.routed.0,
    //             act.routed.1,
    //             act.routed.2,
    //             act.routed.3
    //         );
    //     });
    // }
}

#[derive(Message)]
struct NodeConnect(TcpStream);

impl Network {
    //todo: rework for grpc
    fn listen(&mut self, ctx: &mut Context<Self>) {
        let server_addr = self.address.as_ref().unwrap().as_str().parse().unwrap();
        let listener = TcpListener::bind(&server_addr).unwrap();

        ctx.add_message_stream(listener.incoming().map_err(|_|()).map(NodeConnect));
    }
}

impl Handler<NodeConnect> for Network {
    type Result = ();

    fn handle(&mut self, msg: NodeConnect, ctx: &mut Self::Context) -> Self::Result {
        let addr = ctx.address();
        let registry = self.registry.clone();
        let net_type = self.net_type.clone();

        NodeSession::create(move |ctx| {
            let (r, w) = msg.0.split();
            NodeSession::add_stream( FramedRead::new(r, NodeCodec), ctx);
            NodeSession::new(
                actix::io::FramedWrite::new(w, NodeCodec, ctx),
                addr,
                registry,
                net_type,
            )
        });
    }
}

pub struct GetNodeAddr(pub String);

impl Message for GetNodeAddr {
    type Result = Result<Addr<Node>, ()>;
}

impl Handler<GetNodeAddr> for Network {
    type Result = ResponseActFuture<Self, Addr<Node>, ()>;

    fn handle(&mut self, msg: GetNodeAddr, ctx: &mut Self::Context) -> Self::Result {
        let res = fut::wrap_future(ctx.address().send(GetNode(msg.0)))
            .map_err(|_, _: &mut Network, _| error!("GetNodeAddr Error."))
            .and_then(|res, net, _| {
                if let Ok(info) = res {
                    let id = info.0;
                    let node = net.nodes.get(&id).unwrap();

                    fut::result(Ok(node.clone()))
                } else {
                    fut::result(Err(()))
                }
            });

        Box::new(res)
    }
}

pub struct GetNodeById(pub NodeId);

impl Message for GetNodeById {
    type Result = Result<Addr<Node>, ()>;
}

impl Handler<GetNodeById> for Network {
    type Result = Result<Addr<Node>, ()>;

    fn handle(&mut self, msg: GetNodeById, _ctx: &mut Self::Context) -> Self::Result {
        if let Some(ref node) = self.get_node(msg.0) {
            Ok((*node).clone())
        } else {
            Err(())
        }
    }
}

#[derive(Message)]
pub struct DistributeMessage<M>(pub String, pub M)
    where
        M: RemoteMessage + 'static,
        M::Result: Send + Serialize + DeserializeOwned;

pub struct DistributeAndWait<M>(pub String, pub M)
    where
        M: RemoteMessage + 'static,
        M::Result: Send + Serialize + DeserializeOwned;

impl<M> Message for DistributeAndWait<M>
    where
        M: RemoteMessage + 'static,
        M::Result: Send + Serialize + DeserializeOwned
{
    type Result = Result<M::Result, ()>;
}

impl<M> Handler<DistributeMessage<M>> for Network
    where
        M: RemoteMessage + 'static,
        M::Result: Send + Serialize + DeserializeOwned,
{
    type Result = ();

    fn handle(&mut self, msg: DistributeMessage<M>, _ctx: &mut Self::Context) -> Self::Result {
        let ring = self.ring.read().unwrap();
        // issue w mut ring borrowing resovled by oronsh/rust-hash-ring fork
        let node_id = ring.get_node( msg.0.clone()).unwrap();

        if let Some(ref node) = self.get_node(*node_id) {
            node.do_send(DispatchMessage(msg.1))
        }
    }
}

impl<M> Handler<DistributeAndWait<M>> for Network
    where
        M: RemoteMessage + 'static,
        M::Result: Send + Serialize + DeserializeOwned,
{
    type Result = Response<M::Result, ()>;

    fn handle(&mut self, msg: DistributeAndWait<M>, _ctx: &mut Self::Context) -> Self::Result {
        let ring = self.ring.read().unwrap();
        let node_id = ring.get_node(msg.0.clone()).unwrap();

        if let Some(ref node) = self.get_node(*node_id) {
            let fut = node
                .send( SendRemoteMessage(msg.1))
                .map_err(|_| ())
                .and_then(|res| futures::future::ok(res));

            Response::fut(fut)
        } else {
            Response::fut(futures::future::err(()))
        }
    }
}

pub struct GetNode(pub String);

impl Message for GetNode {
    type Result = Result<(NodeId, String), ()>;
}

impl Handler<GetNode> for Network {
    type Result = Result<(NodeId, String), ()>;

    fn handle(&mut self, msg: GetNode, _ctx: &mut Self::Context) -> Self::Result {
        let ring = self.ring.read().unwrap();
        let node_id = ring.get_node(msg.0).unwrap();

        let default = NodeInfo::default();
        // let default = NodeInfo {
        //     public_addr: "".to_owned(),
        //     app_addr: "".to_owned(),
        //     cluster_addr: "".to_owned(),
        // };

        let node = self.nodes_info.get(node_id).unwrap_or(&default);
        Ok((*node_id, node.public_addr.to_owned()))
    }
}

#[derive(Message)]
pub struct PeerConnected(pub NodeId);

impl Handler<PeerConnected> for Network {
    type Result = ();

    fn handle(&mut self, msg: PeerConnected, _ctx: &mut Self::Context) -> Self::Result {
        self.nodes_connected.push(msg.0);
    }
}

// GetCurrentLeader //////////////////////////////////////////////////////////
/// Get the current leader of the cluster from the perspective of the Raft metrics.
///
/// A return value of Ok(None) indicates that the current leader is unknown or the cluster hasn't
/// come to consensus on the leader yet.
pub struct GetCurrentLeader;

impl Message for GetCurrentLeader {
    type Result = Result<NodeId, ()>;
}

impl Handler<GetCurrentLeader> for Network {
    type Result = ResponseActFuture<Self, NodeId, ()>;

    fn handle(&mut self, msg: GetCurrentLeader, _ctx: &mut Self::Context) -> Self::Result {
        if let Some(ref mut metrics) = self.metrics {
            if let Some(leader) = metrics.current_leader {
                Box::new(fut::result(Ok(leader)))
            } else {
                Box::new(
                    //dmr fut::wrap_future::<_, Self>(delay_for(Duration::from_secs(1)))
                    fut::wrap_future::<_, Self>(Delay::new(Instant::now() + Duration::from_secs(1))) //dmr
                        .map_err(|_, _, _| ())
                        .and_then(|_, _, ctx| {
                            fut::wrap_future::<_, Self>(ctx.address().send(msg))
                                .map_err(|_, _, _| ())
                                .and_then(|res, _, _| fut::result(res))
                        })
                )
            }
        } else {
            Box::new(
                //dmr fut::wrap_future::<_, Self>(delay_for(Duration::from_secs(1)))
                fut::wrap_future::<_, Self>(Delay::new(Instant::now() + Duration::from_secs(1))) //dmr
                    .map_err(|_, _, _| ())
                    .and_then(|_, _, ctx| {
                        fut::wrap_future::<_, Self>(ctx.address().send(msg))
                            .map_err(|_, _, _| ())
                            .and_then(|res, _, _| fut::result(res))
                    })
            )
        }
    }
}

//////////////////////////////////////////////////////////////////////////////
// RaftMetrics ///////////////////////////////////////////////////////////////

impl Handler<RaftMetrics> for Network {
    type Result = ();

    fn handle(&mut self, msg: RaftMetrics, _: &mut Self::Context) -> Self::Result {
        debug!("Metrics: node={} state={:?} leader={:?} term={} index={} applied={} cfg={{join={} members={:?} non_voters={:?} removing={:?}}}",
               msg.id, msg.state, msg.current_leader, msg.current_term, msg.last_log_index, msg.last_applied,
               msg.membership_config.is_in_joint_consensus, msg.membership_config.members,
               msg.membership_config.non_voters, msg.membership_config.removing,
        );

        self.metrics = Some(msg);
    }
}
