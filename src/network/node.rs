use std::collections::HashMap;
use std::marker::PhantomData;
use std::time::Duration;

use tokio::net::TcpStream;
//dmr use tokio::net::tcp::WriteHalf;
use tokio::io::{AsyncRead, WriteHalf}; //dmr
use tokio::sync::oneshot;

//dmr use tokio_util::codec::FramedRead;
use tokio::codec::FramedRead; //dmr

use actix::prelude::*;
use actix_raft::NodeId;
// use tracing::*;
use log::{debug, error, info, warn};
use serde::{Serialize, de::DeserializeOwned};

use crate::config::{NetworkType, NodeInfo};
use super::codec::{ClientNodeCodec, NodeRequest, NodeResponse};
use super::{Network, PeerConnected};
use super::remote::{
    RemoteMessage, RemoteMessageResult,
    DispatchMessage, SendRemoteMessage,
};

#[derive(PartialEq)]
enum NodeState {
    Registered,
    Connected,
}

pub struct Node {
    id: NodeId,             //todo: how is this used? how diff than local_id?
    local_id: NodeId,       //todo: how is this used? How diff than id?
    mid: u64,               //todo: how is this used? Better Type?
    state: NodeState,
    peer_addr: String,      //todo: how is this used? Better Type?
    framed: Option<actix::io::FramedWrite<WriteHalf<TcpStream>, ClientNodeCodec>>,
    requests: HashMap<u64, oneshot::Sender<String>>,
    network: Addr<Network>,
    net_type: NetworkType,
    info: NodeInfo,
}

impl Node {
    pub fn new(
        id: u64,
        local_id: NodeId,
        peer_addr: String,
        network: Addr<Network>,
        net_type: NetworkType,
        info: NodeInfo,
    ) -> Self {
        info!("Registering INFO {:#?}", info);
        Node {
            id: id,
            local_id: local_id,
            mid: 0,
            state: NodeState::Registered,
            peer_addr: peer_addr,
            framed: None,
            requests: HashMap::new(),
            network: network,
            net_type: net_type,
            info: info,
        }
    }

    //todo: convert into gRPC bi-directional connection?
    fn connect(&mut self, ctx: &mut Context<Self>) {
        // node if already connected
        if self.state == NodeState::Connected {
            return ();
        }

        //todo: convert into gRPC bi-directional connection?
        warn!("Connecting self-node #{} to remote peer-addr:{}...", self.id, self.peer_addr);

        let remote_addr = self.peer_addr.as_str().parse().unwrap();
        let conn = TcpStream::connect(&remote_addr)
            .map_err( |err| error!("Error: {:?}", err))
            .map(TcpConnect)
            .into_stream();

        warn!("Connected self-node #{} to peer-addr:{}", self.id, self.peer_addr);
        ctx.add_message_stream(conn);
    }

    //todo: convert into gRPC?
    fn heartbeat(&self, ctx: &mut Context<Self>) {
        ctx.run_later(Duration::new(1, 0), |node, ctx| {
            node.framed.as_mut().unwrap().write( NodeRequest::Ping);
            node.heartbeat(ctx);
        });
    }
}

impl Actor for Node {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        warn!("Node #{} is connecting.", self.id);
        ctx.notify(Connect);
    }

    fn stopping(&mut self, _ctx: &mut Self::Context) -> Running {
        warn!("Node #{} is disconnecting.", self.id);
        self.state = NodeState::Registered;
        Running::Continue
    }

    // fn stopped(&mut self, _ctx: &mut Self::Context) {
    //     warn!("Node #{} disconnected.", self.id);
    //     self.state = NodeState::Registered;
    // }
}

#[derive(Message)]
struct TcpConnect(TcpStream);

#[derive(Message)]
struct Connect;

impl Handler<TcpConnect> for Node {
    type Result = ();

    fn handle(&mut self, msg: TcpConnect, ctx: &mut Self::Context) -> Self::Result {
        info!("Connecting to remote node #{}", self.id);
        self.state = NodeState::Connected;

        let (r, w) = msg.0.split();
        Node::add_stream( FramedRead::new(r, ClientNodeCodec), ctx);
        self.framed = Some(actix::io::FramedWrite::new(w, ClientNodeCodec, ctx));

        self.network.do_send(PeerConnected(self.id));
        self.framed
            .as_mut()
            .unwrap()
            .write(NodeRequest::Join(self.local_id, self.info.clone()));

        match self.net_type {
            NetworkType::Cluster => self.heartbeat(ctx),
            _ => ()
        }
    }
}

impl<M> Handler<DispatchMessage<M>> for Node
where
    M: RemoteMessage + 'static,
    M::Result: Send + Serialize + DeserializeOwned,
{
    type Result = ();

    fn handle(&mut self, msg: DispatchMessage<M>, _ctx: &mut Self::Context) -> Self::Result {
        if let Some(ref mut framed) = self.framed {
            // let body = serde_cbor::to_vec::<M>(&msg.0).unwrap();
            let body = serde_json::to_string::<M>(&msg.0).unwrap();
            let request = NodeRequest::Dispatch(M::type_id().to_owned(), body);
            framed.write(request);
        }
    }
}

impl<M> Handler<SendRemoteMessage<M>> for Node
where
    M: RemoteMessage + 'static,
    M::Result: Send + Serialize + DeserializeOwned,
{
    type Result = RemoteMessageResult<M>;

    fn handle(&mut self, msg: SendRemoteMessage<M>, _ctx: &mut Self::Context) -> Self::Result {
        let (tx, rx) = oneshot::channel::<String>();

        if let Some(ref mut framed) = self.framed {
            self.mid += 1;
            self.requests.insert(self.mid, tx);

            // let body = serde_cbor::to_vec::<M>(&msg.0).unwrap();
            let body = serde_json::to_string::<M>(&msg.0).unwrap();
            let request = NodeRequest::Message(self.mid, M::type_id().to_owned(), body);
            framed.write(request);
        }

        RemoteMessageResult {
            rx: rx,
            m: PhantomData,
        }
    }
}

impl Handler<Connect> for Node {
    type Result = ();

    fn handle(&mut self, _msg: Connect, ctx: &mut Self::Context) -> Self::Result {
        ctx.run_later(Duration::new(2, 0), |node, ctx| {
            node.connect(ctx);
            ctx.notify(Connect);
        });
    }
}

impl actix::io::WriteHandler<std::io::Error> for Node {}

//todo: rework for grpc?
impl StreamHandler<NodeResponse, std::io::Error> for Node {
    fn handle(&mut self, msg: NodeResponse, _ctx: &mut Self::Context) {
        match msg {
            NodeResponse::Result(mid, data) => {
                if let Some(tx) = self.requests.remove(&mid) {
                    let _ = tx.send(data);
                }
            }

            NodeResponse::Ping => {
                info!("Client got Ping from {}", self.id);
            }

            _ => (),
        }
    }
}