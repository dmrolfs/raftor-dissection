use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::{Seek, SeekFrom, Write},
    path::PathBuf
};
use serde::{Serialize, Deserialize};
// use serde_json as json;
use rmp_serde as rmps;
// use tracing::*;
use log::error;
use actix::prelude::*;
use actix_raft::{
    messages::{
        Entry as RaftEntry,
        EntrySnapshotPointer,
    },
    storage::{CurrentSnapshotData, InstallSnapshot, HardState},
    AppData, AppDataResponse, AppError,
    NodeId,
};

pub mod memory;

pub type Storage = memory::MemoryStorage;

type Entry = RaftEntry<StorageData>;

/// The concrete data type used by the all `Storage` systems.
#[derive(Copy, Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum StorageData {
    Add(NodeId),
    Remove(NodeId),
}

impl AppData for StorageData {}

/// The concrete data type use for responding from the storage engine when
/// applying logs to the state machine.
#[derive(Copy, Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct StorageResponse;

impl AppDataResponse for StorageResponse {}


/// The concrete error type use by the `Storage` system.
#[derive(Debug, Serialize, Deserialize)]
pub struct StorageError {
    pub message: String,
}

impl StorageError {
    pub fn from_message<S>( message: S ) -> StorageError
    where
        S: AsRef<str>
    {
        StorageError {
            message: message.as_ref().to_owned(),
        }
    }

    pub fn from_error<S, E:>( message: S, error: E ) -> StorageError
    where
        S: AsRef<str>,
        E: std::fmt::Display,
    {
        Self::from_message( format!( "{}: {}", message.as_ref(), error ) )
    }
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "StorageError: {}", self.message )
    }
}

impl std::error::Error for StorageError {}

impl AppError for StorageError {}


//////////////////////////////////////////////////////////////////////////////////////////////////
// SnapshotActor /////////////////////////////////////////////////////////////////////////////////

/// A simple synchronous actor for interfacing with the filesystem for snapshots.
pub struct SnapshotActor(std::path::PathBuf);

impl Actor for SnapshotActor {
    type Context = SyncContext<Self>;
}

struct SyncInstallSnapshot(InstallSnapshot<StorageError>);


//////////////////////////////////////////////////////////////////////////////////////////////////
// CreateSnapshotWithData ////////////////////////////////////////////////////////////////////////

struct CreateSnapshotWithData(PathBuf, Vec<u8>);

impl Message for CreateSnapshotWithData {
    type Result = Result<(), StorageError>;
}

impl Handler<CreateSnapshotWithData> for SnapshotActor {
    type Result = Result<(), StorageError>;

    fn handle(&mut self, msg: CreateSnapshotWithData, _: &mut Self::Context) -> Self::Result {
        fs::write(msg.0.clone(), msg.1)
            .map_err(|err| {
                let serr = StorageError::from_error("Error writing snapshot file.", err);
                error!("{}", serr);
                serr
            })
    }
}


//////////////////////////////////////////////////////////////////////////////////////////////////
// DeserializeSnapshot ///////////////////////////////////////////////////////////////////////////

struct DeserializeSnapshot(PathBuf);

impl Message for DeserializeSnapshot {
    type Result = Result<Vec<Entry>, StorageError>;
}

impl Handler<DeserializeSnapshot> for SnapshotActor {
    type Result = Result<Vec<Entry>, StorageError>;

    fn handle(&mut self, msg: DeserializeSnapshot, _: &mut Self::Context) -> Self::Result {
        fs::read(msg.0)
            .map_err(|err| {
                let serr = StorageError::from_error(
                    "Error reading contents of snapshot file.",
                    err
                );
                error!("{}", serr);
                serr
            })

            // Deserialize the data of the snapshot file.
            .and_then(|snapdata| {
                //dmr cbor::from_slice::<Vec<Entry>>(snapdata.as_slice())
                rmps::from_slice::<Vec<Entry>>(snapdata.as_slice())
                    .map_err(|err| {
                        let serr = StorageError::from_error(
                            "Error deserializing snapshot contents.",
                            err
                        );
                        error!("{}", serr);
                        serr
                    })
            })
    }
}


//////////////////////////////////////////////////////////////////////////////
// SyncInstallSnapshot ///////////////////////////////////////////////////////

impl Message for SyncInstallSnapshot {
    type Result = Result<EntrySnapshotPointer, StorageError>;
}

impl Handler<SyncInstallSnapshot> for SnapshotActor {
    type Result = Result<EntrySnapshotPointer, StorageError>;

    fn handle(&mut self, msg: SyncInstallSnapshot, _: &mut Self::Context) -> Self::Result {
        let filename = format!("{}", &msg.0.index);
        let filepath = std::path::PathBuf::from(self.0.clone()).join(filename);

        // Create the new snapshot file.
        let mut snapfile = File::create(&filepath)
            .map_err(|err| {
                let serr = StorageError::from_error(
                    "Error creating new snapshot file. {}",
                    err
                );
                error!("{}", serr);
                serr
            })?;

        let chunk_stream = msg.0.stream
            .map_err(|_| {
                let serr = StorageError::from_message(
                    "Snapshot chunk stream hit an error in the memory_storage system."
                );
                error!("{}", serr);
                 serr
            })
            .wait();

        let mut did_process_final_chunk = false;
        for chunk in chunk_stream {
            let chunk = chunk?;
            snapfile
                .seek(SeekFrom::Start(chunk.offset))
                .map_err(|err| {
                    let serr = StorageError::from_error(
                        "Error seeking to file location for writing snapshot chunk.",
                        err
                    );
                    error!("{}", serr);
                    serr
            })?;

            snapfile
                .write_all(&chunk.data)
                .map_err(|err| {
                    let serr = StorageError::from_error(
                        "Error writing snapshot chunk to snapshot file.",
                        err
                    );
                    error!("{}", serr);
                    serr
            })?;

            if chunk.done {
                did_process_final_chunk = true;
            }

            let _ = chunk.cb.send(());
        }

        if !did_process_final_chunk {
            let serr = StorageError::from_message(
                "Prematurely exiting snapshot chunk stream. Never hit final chunk."
            );
            error!("{}", serr);
            Err(serr)
        } else {
            Ok(EntrySnapshotPointer {
                path: filepath.to_string_lossy().to_string(),
            })
        }
    }
}


//////////////////////////////////////////////////////////////////////////////////////////////////
// Other Message Types & Handlers ////////////////////////////////////////////////////////////////
//
// NOTE WELL: these following types, just as the MemoryStorage system overall, is intended
// primarily for testing purposes. Don't build your application using this storage implementation.

/// Get the current state of the storage engine.
pub struct GetCurrentState;

impl Message for GetCurrentState {
    type Result = Result<CurrentStateData, ()>;
}

/// The current state of the storage engine.
pub struct CurrentStateData {
    pub hs: HardState,
    pub log: BTreeMap<u64, Entry>,
    pub snapshot_data: Option<CurrentSnapshotData>,
    pub snapshot_dir: String,
    pub state_machine: BTreeMap<u64, Entry>,
}

impl Handler<GetCurrentState> for Storage {
    type Result = Result<CurrentStateData, ()>;

    fn handle(&mut self, _: GetCurrentState, _: &mut Self::Context) -> Self::Result {
        Ok(CurrentStateData {
            hs: self.hs.clone(),
            log: self.log.clone(),
            snapshot_data: self.snapshot_data.clone(),
            snapshot_dir: self.snapshot_dir.clone(),
            state_machine: self.state_machine.clone(),
        })
    }
}


pub struct GetNode(pub String);

impl Message for GetNode {
    type Result = Result<NodeId, ()>;
}

impl Handler<GetNode> for Storage {
    type Result = Result<NodeId, ()>;

    fn handle(&mut self, msg: GetNode, _ctx: &mut Self::Context) -> Self::Result {
        let ring = self.ring.read().unwrap();
        if let Some(node_id) = ring.get_node(msg.0) {
            Ok(*node_id)
        } else {
            Err(())
        }
    }
}
