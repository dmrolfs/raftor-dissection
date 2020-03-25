use std::path::PathBuf;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(name = "raft-stub")]
pub struct Opt {
    /// server listening port
    #[structopt(short, long)]
    pub port: Option<u32>,

    /// application configuration
    #[structopt(long = "config", parse(from_os_str))]
    pub configuration_path:  Option<PathBuf>
}