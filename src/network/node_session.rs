use std::{
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};
use actix::prelude::*;
use actix_raft::NodeId;
// use tracing::*;
use log::{debug, info};

use tokio::{
    io::WriteHalf,
    net::TcpStream,
    sync::oneshot,
};

use crate::config::NetworkType;
use super::{
    HandlerRegistry, Network, NodeDisconnect, Handshake
};
use super::codec::{NodeCodec, NodeRequest, NodeResponse};


pub struct NodeSession {
    last_heartbeat: Instant,
    network: Addr<Network>,
    net_type: NetworkType,
    framed: actix::io::FramedWrite<WriteHalf<TcpStream>, NodeCodec>,
    id: Option<NodeId>,
    registry: Arc<RwLock<HandlerRegistry>>,
}

impl NodeSession {
    pub fn new(
        framed: actix::io::FramedWrite<WriteHalf<TcpStream>, NodeCodec>,
        network: Addr<Network>,
        registry: Arc<RwLock<HandlerRegistry>>,
        net_type: NetworkType,
    ) -> NodeSession {
        NodeSession {
            last_heartbeat: Instant::now(),
            framed: framed,
            network,
            id: None,
            registry: registry,
            net_type: net_type,
        }
    }

    fn heartbeat(&self, ctx: &mut Context<Self>) {
        ctx.run_interval( Duration::new(1, 0), |act, ctx| {
            // if Instant::now().duration_since(act.last_heartbeat) > Duration::new(10, 0) {
            if Instant::now().duration_since(act.last_heartbeat) > Duration::new(30, 0) {
                info!("Client heartbeat timed out, disconnecting!");
                ctx.stop();
            }

            // Reply heartbeat
            act.framed.write(NodeResponse::Ping);
        });
    }
}

impl Actor for NodeSession {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        match self.net_type {
            NetworkType::Cluster => self.heartbeat(ctx),
            _ => ()
        }
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        self.network.do_send(NodeDisconnect(self.id.unwrap()));
    }
}

impl actix::io::WriteHandler<std::io::Error> for NodeSession {}

impl StreamHandler<NodeRequest, std::io::Error> for NodeSession {
    fn handle(&mut self, msg: NodeRequest, ctx: &mut Self::Context) {
        match msg {
            NodeRequest::Ping => {
                self.last_heartbeat = Instant::now();
            }

            NodeRequest::Join(id, info) => {
                self.id = Some(id);
                self.network.do_send(Handshake(self.id.unwrap(), info));
            }

            NodeRequest::Message(mid, type_id, body) => {
                let (tx, rx) = oneshot::channel();
                let registry = self.registry.read().unwrap();

                if let Some(ref handler) = registry.get(type_id.as_str()) {
                    handler.handle(body, tx);

                    fut::wrap_future::<_, Self>(rx)
                        .then(move |res, act, _| {
                            debug!("Got remote message {:?}", res);

                            match res {
                                Ok(res) => act.framed.write(NodeResponse::Result(mid, res)),
                                Err(_) => (),
                            }

                            fut::ok(())
                        })
                        .spawn(ctx)
                }
            }

            NodeRequest::Dispatch(type_id, body) => {
                let (tx, rx) = oneshot::channel();
                let registry = self.registry.read().unwrap();

                if let Some(ref handler) = registry.get(type_id.as_str()) {
                    handler.handle(body, tx);

                    fut::wrap_future::<_, Self>(rx)
                        .then(|_, _, _| fut::ok(()))
                        .spawn(ctx)
                }
            }
        }
    }
}