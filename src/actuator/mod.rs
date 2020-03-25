use std::env;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tokio::timer::Delay;
// use tracing::*;
use log::{error, info};
use actix::prelude::*;
use actix_web::client::Client;
use actix_raft::NodeId;
use config;

use crate::config::{ConfigSchema, NetworkType, NodeInfo};
use crate::hash_ring::{self, RingType};
use crate::network::{HandlerRegistry, Network, DiscoverNodes, SetClusterState, NetworkState};
use crate::raft::{RaftClient, InitRaft};
use crate::server::Server;
use crate::utils;

mod handlers;

pub struct Actuator {
    id: NodeId,
    pub raft: Addr<RaftClient>,
    pub app_network: Addr<Network>,
    pub cluster_network: Addr<Network>,
    pub server: Addr<Server>,
    discovery_host: String,
    ring: RingType,
    registry: Arc<RwLock<HandlerRegistry>>,
    info: NodeInfo,
}

impl Actuator {
    pub fn new() -> Actuator {
        let mut config = config::Config::default();

        config
            .merge(config::File::with_name("config/application"))
            .unwrap()
            .merge(config::Environment::with_prefix("APP"))
            .unwrap();

        let mut config = config.try_into::<ConfigSchema>().unwrap();
        info!("Actuator configuration: {:?}", config);

        // create consistent-hash ring
        let ring = hash_ring::Ring::new(10);

        let registry = Arc::new(RwLock::new(HandlerRegistry::new()));

        let args: Vec<String> = env::args().collect();
        let cluster_address = args[1].as_str();
        let app_address = args[2].as_str();
        let public_address = args[3].as_str();

        let node_info = NodeInfo {
            cluster_addr: cluster_address.to_owned(),
            app_addr: app_address.to_owned(),
            public_addr: public_address.to_owned(),
        };
        info!("Actuator node info: {:?}", node_info);

        let node_id = utils::generate_node_id(cluster_address);

        Actuator::add_node_to_config(node_info.clone(), &mut config);

        let cluster_arb = Arbiter::new();
        let app_arb = Arbiter::new();
        let raft_arb = Arbiter::new();

        let raft_client = RaftClient::new(node_id, ring.clone(), registry.clone());
        let raft = RaftClient::start_in_arbiter(&raft_arb, |_| raft_client);

        let mut cluster_network = Network::new(
            node_id,
            ring.clone(),
            registry.clone(),
            NetworkType::Cluster,
            raft.clone(),
            config.discovery_host.clone(),
            node_info.clone(),
        );

        let mut app_network = Network::new(
            node_id,
            ring.clone(),
            registry.clone(),
            NetworkType::App,
            raft.clone(),
            config.discovery_host.clone(),
            node_info.clone(),
        );

        cluster_network.configure(config.clone());
        cluster_network.bind(cluster_address);

        app_network.configure(config.clone());
        app_network.bind(app_address);

        let cluster_network_addr = Network::start_in_arbiter(&cluster_arb, |_| cluster_network);
        let app_network_addr = Network::start_in_arbiter(&app_arb, |_| app_network);

        let server = Server::new(app_network_addr.clone(), ring.clone(), node_id);
        let server_addr = server.start();

        Actuator {
            id: node_id,
            app_network: app_network_addr,
            cluster_network: cluster_network_addr,
            raft: raft,
            server: server_addr,
            ring: ring,
            registry: registry,
            discovery_host: config.discovery_host.clone(),
            info: node_info,
        }
    }

    fn add_node_to_config(node: NodeInfo, config: &mut ConfigSchema) {
        let index = config.nodes.iter().position(|r| r == &node);

        if index.is_none() {
            config.nodes.push(node);
        }
    }
}

impl Actor for Actuator {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        fut::wrap_future::<_, Self>(self.cluster_network.send(DiscoverNodes))
            .map_err(|err, _, _| panic!(err))
            .and_then(|res, act, _| {
                let res = res.unwrap();
                let nodes = res.0;
                let join_node = res.1;

                fut::wrap_future::<_, Self>(act.raft.send(InitRaft{
                    nodes,
                    network: act.cluster_network.clone(),
                    server: act.server.clone(),
                    join_mode: join_node
                }))
                    .map_err(|err, _, _| panic!(err))
                    .and_then(move |_, act, ctx| {
                        let client = Client::default();
                        let cluster_nodes_route = format!("http://{}/cluster/join", act.discovery_host.as_str());

                        act.app_network.do_send(SetClusterState(NetworkState::Cluster));
                        act.cluster_network.do_send(SetClusterState(NetworkState::Cluster));

                        if join_node {
                            fut::wrap_future::<_, Self>(
                                client.put(cluster_nodes_route)
                                    .header("Content-Type", "application/json")
                                    .send_json(&act.id)
                            )
                                .map_err(|err, _, _| error!("Error joining cluster {:?}", err))
                                .and_then(|_, _, _| {
                                    fut::wrap_future::<_, Self>(Delay::new(Instant::now() + Duration::from_secs(1)))
                                        .map_err(|_, _, _| ())
                                        .and_then(|_, _, _| {
//                                            act.raft.do_send(AddNode(act.id));
                                            fut::ok(())
                                        })
                                })
                                .spawn(ctx);
                        }

                        fut::ok(())
                    })
            })
            .spawn(ctx);

        self.register_handlers();
    }
}