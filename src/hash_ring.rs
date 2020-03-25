use std::sync::{Arc, RwLock};
use hash_ring::HashRing;
use actix_raft::NodeId;

pub type RingType = Arc<RwLock<HashRing<NodeId>>>;

pub struct Ring;

impl Ring {
    /// Creates a new hash ring with the specified nodes.
    /// Replicas is the number of virtual nodes each node has to make a better distribution.
    pub fn new(replicas: isize) -> RingType {
        Arc::new(RwLock::new(HashRing::new(Vec::new(), replicas)))
    }
}
