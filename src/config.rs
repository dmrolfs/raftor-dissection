use serde::{Serialize, Deserialize};
use hocon::HoconLoader;
use structopt::StructOpt;
// use tracing::*;
use log::info;
use crate::opt::Opt;

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum NetworkType {
    Cluster,
    App,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct NodeInfo {
    pub cluster_addr: String,
    pub app_addr: String,
    pub public_addr: String,
}

impl Default for NodeInfo {
    fn default() -> Self {
        NodeInfo {
            public_addr: "".to_owned(),
            app_addr: "".to_owned(),
            cluster_addr: "".to_owned(),
        }
    }
}

#[derive(Deserialize, Debug, Copy, Clone)]
pub enum JoinStrategy {
    Static,
    Dynamic,
}

pub type NodeList = Vec<NodeInfo>;

#[derive(Deserialize, Debug, Clone)]
pub struct ConfigSchema {
    pub discovery_host: String,
    pub join_strategy: JoinStrategy,
    pub nodes: NodeList,
}


////////////////////////////////////////////////////////////////////////////////////
//todo: reconsider below here

#[derive(Debug, Deserialize)]
pub struct Configuration {
    pub port: u32,
}

// #[instrument]
pub fn load() -> Result<Configuration, Box<dyn std::error::Error>> {
    let opt = Opt::from_args();
    info!("CLI Options {:?}", opt);

    let config_path = opt.configuration_path
        .as_ref()
        .map( |p| p.as_path() )
        .unwrap_or( std::path::Path::new("config/reference.conf") );

    let conf: Configuration = HoconLoader::new().load_file( config_path )?.hocon()?.resolve()?;
    info!("HOCON {:?}: {:?}", config_path, conf );

    info!("port opt:{:?} conf:{:?}", opt.port, conf.port);
    let port = resolve(opt.port, Some(conf.port), ).unwrap_or( 10_000 );

    Ok( Configuration { port } )
}

/// consider redesign to add fallback to hocon object with fallback to given Opt struct.
/// maybe a macro?
fn resolve<T>( primary: Option<T>, fallback: Option<T>, ) -> Option<T> {
    if primary.is_some() {
        primary
    } else if fallback.is_some() {
        fallback
    } else {
        None
    }
}
