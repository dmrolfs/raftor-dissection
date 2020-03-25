use std::{
    collections::BTreeMap,
    path::PathBuf
};
// use serde_json as json;
use rmp_serde as rmps;
// use tracing::*;
use log::{debug, error, info};
use actix::prelude::*;
use actix_raft::{
    messages::{
        EntryPayload,
        EntrySnapshotPointer,
        MembershipConfig,
    },
    storage::{
        AppendEntryToLog, GetLogEntries, ReplicateToLog,
        CreateSnapshot, CurrentSnapshotData, GetCurrentSnapshot, InstallSnapshot,
        GetInitialState, HardState, InitialState, SaveHardState,
        ApplyEntryToStateMachine, ReplicateToStateMachine,
        RaftStorage,
    },
    NodeId,
};

use crate::hash_ring::RingType;
use crate::server::{Server, Rebalance};
use super::{Entry, StorageData, StorageResponse, StorageError};
use super::{SnapshotActor, CreateSnapshotWithData, DeserializeSnapshot, SyncInstallSnapshot};

/// A concrete implementation of the `RaftStorage` trait.
///
/// This is primarily for testing and demo purposes. In a real application, storing Raft's data
/// on a stable storage medium is expected.
///
/// This storage implementation structures its data as an append-only immutable log. The contents
/// of the entries given to this storage implementation are not ready or manipulated.
// #[derive(Debug)]
pub struct MemoryStorage {
    pub hs: HardState,
    pub log: BTreeMap<u64, Entry>,
    pub snapshot_data: Option<CurrentSnapshotData>,
    pub snapshot_dir: String,
    pub state_machine: BTreeMap<u64, Entry>,
    pub snapshot_actor: Addr<SnapshotActor>,
    pub ring: RingType,
    pub server: Addr<Server>,
}

impl MemoryStorage {
    /// Create a new instance.
    pub fn new(
        members: Vec<NodeId>,
        snapshot_dir: String,
        ring: RingType,
        server: Addr<Server>
    ) -> Self {
        let snapshot_dir_pathbuf = std::path::PathBuf::from(snapshot_dir.clone());
        let membership = MembershipConfig{
            members,
            non_voters: vec![],
            removing: vec![],
            is_in_joint_consensus: false,
        };

        Self {
            hs: HardState {
                current_term: 0,
                voted_for: None,
                membership,
            },
            log: Default::default(),
            snapshot_data: None,
            snapshot_dir,
            state_machine: Default::default(),
            snapshot_actor: SyncArbiter::start(
            1,
            move || {
                SnapshotActor(snapshot_dir_pathbuf.clone())
            }),
            ring: ring,
            server: server,
        }
    }
}

impl Actor for MemoryStorage {
    type Context = Context<Self>;

    /// Start this actor.
    fn started(&mut self, _ctx: &mut Self::Context) {}
}

impl RaftStorage<StorageData, StorageResponse, StorageError> for MemoryStorage {
    type Actor = Self;
    type Context = Context<Self>;
}

impl Handler<GetInitialState<StorageError>> for MemoryStorage {
    type Result = ResponseActFuture<Self, InitialState, StorageError>;

    fn handle(&mut self, _: GetInitialState<StorageError>, _: &mut Self::Context) -> Self::Result {
        let (last_log_index, last_log_term) = self.log.iter().last().map(|e| (*e.0, e.1.term)).unwrap_or((0,0));

        Box::new(fut::ok(InitialState {
            last_log_index,
            last_log_term,
            last_applied_log: self.state_machine.iter().last().map(|e| *e.0).unwrap_or(0),
            hard_state: self.hs.clone(),
        }))
    }
}

impl Handler<SaveHardState<StorageError>> for MemoryStorage {
    type Result = ResponseActFuture<Self, (), StorageError>;

    fn handle(&mut self, msg: SaveHardState<StorageError>, _: &mut Self::Context) -> Self::Result {
        self.hs = msg.hs;
        Box::new(fut::ok(()))
    }
}

impl Handler<GetLogEntries<StorageData, StorageError>> for MemoryStorage {
    type Result = ResponseActFuture<Self, Vec<Entry>, StorageError>;

    fn handle(
        &mut self,
        msg: GetLogEntries<StorageData, StorageError>,
        _: &mut Self::Context
    ) -> Self::Result {
        Box::new(fut::ok(
            self.log
                .range(msg.start..msg.stop)
                .map(|e| e.1.clone())
                .collect(),
        ))
    }
}

impl Handler<AppendEntryToLog<StorageData, StorageError>> for MemoryStorage {
    type Result = ResponseActFuture<Self, (), StorageError>;

    fn handle(
        &mut self,
        msg: AppendEntryToLog<StorageData, StorageError>,
        _: &mut Self::Context
    ) -> Self::Result {
        self.log.insert(msg.entry.index, (*msg.entry).clone());
        Box::new(fut::ok(()))
    }
}

impl Handler<ReplicateToLog<StorageData, StorageError>> for MemoryStorage {
    type Result = ResponseActFuture<Self, (), StorageError>;

    fn handle(
        &mut self,
        msg: ReplicateToLog<StorageData, StorageError>,
        _: &mut Self::Context
    ) -> Self::Result {
        msg.entries.iter().for_each(|e| {
            self.log.insert(e.index, e.clone());
        });

        Box::new(fut::ok(()))
    }
}

impl Handler<ApplyEntryToStateMachine<StorageData, StorageResponse, StorageError>> for MemoryStorage {
    type Result = ResponseActFuture<Self, StorageResponse, StorageError>;

    fn handle(
        &mut self,
        msg: ApplyEntryToStateMachine<StorageData, StorageResponse, StorageError>,
        _ctx: &mut Self::Context
    ) -> Self::Result {
        let res = if let Some(old) = self
            .state_machine
            .insert(msg.payload.index, (*msg.payload).clone())
        {
            let err = StorageError::from_message(
                format!("Critical error. State machine entries are not allowed to be overwritten. Entry: {:?}", old )
            );
            error!("{}", err);
            Err(err)
        } else {
            if let EntryPayload::Normal(entry) = &msg.payload.payload {
                let mut ring = self.ring.write().unwrap();
                match (*entry).data {
                    StorageData::Add(node_id) => {
                        info!("Adding node {}", node_id);
                        ring.add_node(&node_id);
                        self.server.do_send(Rebalance)
                    }

                    StorageData::Remove(node_id) => {
                        info!("Removing node {}", node_id);
                        ring.remove_node(&node_id)
                    }
                }
            } else {
            }

            Ok(StorageResponse)
        };

        Box::new(fut::result(res))
    }
}


impl Handler<ReplicateToStateMachine<StorageData, StorageError>> for MemoryStorage {
    type Result = ResponseActFuture<Self, (), StorageError>;

    fn handle(
        &mut self,
        msg: ReplicateToStateMachine<StorageData, StorageError>,
        _ctx: &mut Self::Context
    ) -> Self::Result {
        let res = msg.payload.iter().try_for_each(|e| {
            if let Some(old) = self.state_machine.insert(e.index, e.clone()) {
                let err = StorageError::from_message(
                    format!("Critical error. State machine entries are not allowed to be overwritten. Entry: {:?}", old)
                );
                error!( "{}", err );
                return Err(err)
            }

            if let EntryPayload::Normal(entry) = &e.payload {
                let mut ring = self.ring.write().unwrap();
                match entry.data {
                    StorageData::Add(node_id) => {
                        info!("Adding node {}", node_id);
                        ring.add_node(&node_id);
                        self.server.do_send(Rebalance)
                    }

                    StorageData::Remove(node_id) => {
                        info!("Removing node {}", node_id);
                        ring.remove_node(&node_id)
                    }
                }
            } else {

            }

            Ok(())
        });

        Box::new(fut::result(res))
    }
}

impl Handler<CreateSnapshot<StorageError>> for MemoryStorage {
    type Result = ResponseActFuture<Self, CurrentSnapshotData, StorageError>;

    fn handle(
        &mut self,
        msg: CreateSnapshot<StorageError>,
        _: &mut Self::Context
    ) -> Self::Result {
        debug!(
            "Creating new snapshot under '{}' through index {}.",
            &self.snapshot_dir, &msg.through
        );

        // Serialize snapshot data.
        let through = msg.through;

        let entries = self
            .log
            .range(0u64..=through)
            .map(|(_, v)| v.clone())
            .collect::<Vec<_>>();

        debug!("Creating snapshot with {} entries.", entries.len());
        let (index, term) = entries.last().map(|e| (e.index, e.term)).unwrap_or((0,0));
        //dmr let snapdata = match cbor::to_vec(&entries) {
        let snapdata = match rmps::to_vec(&entries) {
            Ok(snapdata) => snapdata,

            Err(err) => {
                let serr = StorageError::from_error( "Error serializing log for creating a snapshot.", &err );
                error!("{}", serr);
                return Box::new(fut::err(serr));
            }
        };

        // Create snapshot file and write snapshot data to it.
        let filename = format!("{}", msg.through);
        let filepath = std::path::PathBuf::from(self.snapshot_dir.clone()).join(filename);

        Box::new(
            fut::wrap_future(
                self.snapshot_actor.send(CreateSnapshotWithData(filepath.clone(), snapdata))
            )
                .map_err(|err, _, _| panic!("Error communicating with snapshot actor. {}", err))
                .and_then(|res, _, _| fut::result(res))
                // Clean up old log entries which are now part of the new snapshot.
                .and_then(move |_, act: &mut Self, _| {
                    let path = filepath.to_string_lossy().to_string();
                    debug!("Finished creating snapshot file at {}", &path);
                    act.log = act.log.split_off(&through);
                    let pointer = EntrySnapshotPointer { path };
                    let entry = Entry::new_snapshot_pointer(pointer.clone(), index, term);
                    act.log.insert(through, entry);

                    // Cache the most recent snapshot data.
                    let current_snap_data = CurrentSnapshotData {
                        term,
                        index,
                        membership: act.hs.membership.clone(),
                        pointer
                    };

                    act.snapshot_data = Some(current_snap_data.clone());

                    fut::ok(current_snap_data)
                })
        )
    }
}

impl Handler<InstallSnapshot<StorageError>> for MemoryStorage {
    type Result = ResponseActFuture<Self, (), StorageError>;

    fn handle(
        &mut self,
        msg: InstallSnapshot<StorageError>,
        _: &mut Self::Context
    ) -> Self::Result {
        let (index, term) = (msg.index, msg.term);
        Box::new(
            fut::wrap_future(self.snapshot_actor.send(SyncInstallSnapshot(msg)))
                .map_err(|err, _, _|  panic!("Error communicating with snapshot actor. {}", err))
                .and_then(|res, _, _| fut::result(res))

                // Snapshot file has been created. Perform final steps of this algorithm.
                .and_then(move |pointer, act: &mut Self, ctx| {
                    // Cache the most recent snapshot data.
                    act.snapshot_data = Some(CurrentSnapshotData {
                        index,
                        term,
                        membership: act.hs.membership.clone(),
                        pointer: pointer.clone(),
                    });

                    // Update target index with the new snapshot pointer.
                    let entry = Entry::new_snapshot_pointer(pointer.clone(), index, term);
                    act.log = act.log.split_off(&index);
                    let previous = act.log.insert(index, entry);

                    // If there are any logs newer than 'index', then we are done. Else, the state
                    // machine should be reset, and recreated from the new snapshot.
                    match &previous {
                        Some(entry) if entry.index == index && entry.term == term => {
                            fut::Either::A(fut::ok(()))
                        }

                        // There are no newer entries in the log, which means that we need to
                        // rebuild the state machine. Open the snapshot file read out its entries
                        _ => {
                            let pathbuf = PathBuf::from(pointer.path);
                            fut::Either::B(act.rebuild_state_machine_from_snapshot(ctx, pathbuf))
                        }
                    }
                })
        )
    }
}

impl Handler<GetCurrentSnapshot<StorageError>> for MemoryStorage {
    type Result = ResponseActFuture<Self, Option<CurrentSnapshotData>, StorageError>;

    fn handle(
        &mut self,
        _: GetCurrentSnapshot<StorageError>,
        _: &mut Self::Context
    ) -> Self::Result {
        debug!("Checking for current snapshot.");
        Box::new(fut::ok(self.snapshot_data.clone()))
    }
}

impl MemoryStorage {
    /// Rebuild the state machine from the specified snapshot.
    fn rebuild_state_machine_from_snapshot(
        &mut self,
        _: &mut Context<Self>,
        path: std::path::PathBuf
    ) -> impl ActorFuture<Actor=Self, Item=(), Error=StorageError> {
        // Read full contents of the snapshot file.
        fut::wrap_future(self.snapshot_actor.send(DeserializeSnapshot(path)))
            .map_err(|err, _, _| panic!("Error communicating with snapshot actor. {}", err))
            .and_then(|res, _, _| fut::result(res))

            // Rebuild state machine from the deserialized data.
            .and_then(|entries, act: &mut Self, _| {
                act.state_machine.clear();
                act.state_machine.extend(
                    entries.into_iter().map(|e| (e.index, e))
                );
                fut::ok(())
            })
            .map(|_, _, _| debug!("Finished rebuilding statemachine from snapshot successfully."))
    }
}
