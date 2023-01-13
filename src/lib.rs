//
// Copyright (c) 2022 ZettaScale Technology
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
//

use async_std::sync::{Arc, Mutex};
use async_trait::async_trait;
use log::{debug, error, trace, warn};
use rocksdb::{ColumnFamilyDescriptor, IteratorMode, Options, WriteBatch, DB};
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use uhlc::NTP64;
use zenoh::buffers::{reader::HasReader, writer::HasWriter};
use zenoh::prelude::*;
use zenoh::properties::Properties;
use zenoh::time::{new_reception_timestamp, Timestamp};
use zenoh::Result as ZResult;
use zenoh_backend_traits::config::{StorageConfig, VolumeConfig};
use zenoh_backend_traits::*;
use zenoh_codec::{RCodec, WCodec, Zenoh060};
use zenoh_core::{bail, zerror};
use zenoh_util::{zenoh_home, Timed, TimedEvent, Timer};

/// The environement variable used to configure the root of all storages managed by this RocksdbBackend.
pub const SCOPE_ENV_VAR: &str = "ZBACKEND_ROCKSDB_ROOT";

/// The default root (whithin zenoh's home directory) if the ZBACKEND_ROCKSDB_ROOT environment variable is not specified.
pub const DEFAULT_ROOT_DIR: &str = "zbackend_rocksdb";

// Properies used by the Backend
//  - None

// Properies used by the Storage
pub const PROP_STORAGE_DIR: &str = "dir";
pub const PROP_STORAGE_CREATE_DB: &str = "create_db";
pub const PROP_STORAGE_READ_ONLY: &str = "read_only";
pub const PROP_STORAGE_ON_CLOSURE: &str = "on_closure";

// Column family names
const CF_PAYLOADS: &str = rocksdb::DEFAULT_COLUMN_FAMILY_NAME;
const CF_DATA_INFO: &str = "data_info";

const GIT_VERSION: &str = git_version::git_version!(prefix = "v", cargo_prefix = "v");
lazy_static::lazy_static! {
    static ref LONG_VERSION: String = format!("{} built with {}", GIT_VERSION, env!("RUSTC_VERSION"));
    static ref GC_PERIOD: Duration = Duration::new(5, 0);
    static ref MIN_DELAY_BEFORE_REMOVAL: NTP64 = NTP64::from(Duration::new(5, 0));
}

pub(crate) enum OnClosure {
    DestroyDB,
    DoNothing,
}

#[allow(dead_code)]
const CREATE_BACKEND_TYPECHECK: CreateVolume = create_volume;

#[no_mangle]
pub fn create_volume(_unused: VolumeConfig) -> ZResult<Box<dyn Volume>> {
    // For some reasons env_logger is sometime not active in a loaded library.
    // Try to activate it here, ignoring failures.
    let _ = env_logger::try_init();
    debug!("RocksDB backend {}", LONG_VERSION.as_str());

    let root = if let Some(dir) = std::env::var_os(SCOPE_ENV_VAR) {
        PathBuf::from(dir)
    } else {
        let mut dir = PathBuf::from(zenoh_home());
        dir.push(DEFAULT_ROOT_DIR);
        dir
    };
    let mut properties = Properties::default();
    properties.insert("root".into(), root.to_string_lossy().into());
    properties.insert("version".into(), LONG_VERSION.clone());

    let admin_status = properties
        .0
        .into_iter()
        .map(|(k, v)| (k, serde_json::Value::String(v)))
        .collect();
    Ok(Box::new(RocksdbBackend { admin_status, root }))
}

pub struct RocksdbBackend {
    admin_status: serde_json::Value,
    root: PathBuf,
}

#[async_trait]
impl Volume for RocksdbBackend {
    fn get_admin_status(&self) -> serde_json::Value {
        self.admin_status.clone()
    }

    fn get_capability(&self) -> Capability {
        Capability {
            persistence: Persistence::Durable,
            history: History::Latest,
            read_cost: 0,
        }
    }

    async fn create_storage(&mut self, config: StorageConfig) -> ZResult<Box<dyn Storage>> {
        let volume_cfg = match config.volume_cfg.as_object() {
            Some(v) => v,
            None => bail!("rocksdb backed storages need volume-specific configurations"),
        };

        let read_only = match volume_cfg.get(PROP_STORAGE_READ_ONLY) {
            None | Some(serde_json::Value::Bool(false)) => false,
            Some(serde_json::Value::Bool(true)) => true,
            _ => {
                bail!(
                    "Optional property `{}` of rocksdb storage configurations must be a boolean",
                    PROP_STORAGE_READ_ONLY
                )
            }
        };

        let on_closure = match volume_cfg.get(PROP_STORAGE_ON_CLOSURE) {
            Some(serde_json::Value::String(s)) if s == "destroy_db" => OnClosure::DestroyDB,
            Some(serde_json::Value::String(s)) if s == "do_nothing" => OnClosure::DoNothing,
            None => OnClosure::DoNothing,
            _ => {
                bail!(
                    r#"Optional property `{}` of rocksdb storage configurations must be either "do_nothing" (default) or "destroy_db""#,
                    PROP_STORAGE_ON_CLOSURE
                )
            }
        };

        let db_path = match volume_cfg.get(PROP_STORAGE_DIR) {
            Some(serde_json::Value::String(dir)) => {
                let mut db_path = self.root.clone();
                db_path.push(dir);
                db_path
            }
            _ => {
                bail!(
                    r#"Required property `{}` for File System Storage must be a string"#,
                    PROP_STORAGE_DIR
                )
            }
        };

        let mut opts = Options::default();
        match volume_cfg.get(PROP_STORAGE_CREATE_DB) {
            Some(serde_json::Value::Bool(true)) => opts.create_if_missing(true),
            Some(serde_json::Value::Bool(false)) | None => {}
            _ => {
                bail!(
                    r#"Optional property `{}` of rocksdb storage configurations must be a boolean"#,
                    PROP_STORAGE_CREATE_DB
                )
            }
        }
        opts.create_missing_column_families(true);
        let db = if read_only {
            DB::open_cf_for_read_only(&opts, &db_path, [CF_PAYLOADS, CF_DATA_INFO], true)
        } else {
            let cf_payloads = ColumnFamilyDescriptor::new(CF_PAYLOADS, opts.clone());
            let cf_data_info = ColumnFamilyDescriptor::new(CF_DATA_INFO, opts.clone());
            DB::open_cf_descriptors(&opts, &db_path, vec![cf_payloads, cf_data_info])
        }
        .map_err(|e| {
            zerror!(
                "Failed to open data-info database from {:?}: {}",
                db_path,
                e
            )
        })?;
        let db = Arc::new(Mutex::new(Some(db)));

        let timer = if read_only {
            None
        } else {
            // start periodic GC event
            let t = Timer::default();
            let gc = TimedEvent::periodic(*GC_PERIOD, GarbageCollectionEvent { db: db.clone() });
            let _ = t.add_async(gc).await;
            Some(t)
        };
        Ok(Box::new(RocksdbStorage {
            config,
            on_closure,
            read_only,
            db,
            timer,
        }))
    }

    fn incoming_data_interceptor(&self) -> Option<Arc<dyn Fn(Sample) -> Sample + Send + Sync>> {
        None
    }

    fn outgoing_data_interceptor(&self) -> Option<Arc<dyn Fn(Sample) -> Sample + Send + Sync>> {
        None
    }
}

struct RocksdbStorage {
    config: StorageConfig,
    on_closure: OnClosure,
    read_only: bool,
    // Note: rocksdb isn't thread-safe. See https://github.com/rust-rocksdb/rust-rocksdb/issues/404
    db: Arc<Mutex<Option<DB>>>,
    // Note: Timer is kept to not be dropped and keep the GC periodic event running
    #[allow(dead_code)]
    timer: Option<Timer>,
}

#[async_trait]
impl Storage for RocksdbStorage {
    fn get_admin_status(&self) -> serde_json::Value {
        self.config.to_json_value()
    }

    // When receiving a PUT operation
    async fn put(&mut self, key: OwnedKeyExpr, sample: Sample) -> ZResult<StorageInsertionResult> {
        // Get lock on DB
        let db_cell = self.db.lock().await;
        let db = db_cell.as_ref().unwrap();

        // Store the sample
        trace!("Storing key, sample: {} : {}", key, sample);
        if !self.read_only {
            // put payload and data_info in DB
            put_kv(db, &key, sample.value, sample.timestamp.unwrap())
        } else {
            warn!("Received PUT for read-only DB on {:?} - ignored", key);
            Err("Received update for read-only DB".into())
        }
    }

    // When receiving a DEL operation
    async fn delete(
        &mut self,
        key: OwnedKeyExpr,
        _timestamp: Timestamp,
    ) -> ZResult<StorageInsertionResult> {
        // Get lock on DB
        let db_cell = self.db.lock().await;
        let db = db_cell.as_ref().unwrap();

        trace!("Deleting key: {}", key);
        if !self.read_only {
            // delete file
            delete_kv(db, &key)
        } else {
            warn!("Received DELETE for read-only DB on {:?} - ignored", key);
            Err("Received update for read-only DB".into())
        }
    }

    // When receiving a GET operation
    async fn get(&mut self, key_expr: OwnedKeyExpr, _parameters: &str) -> ZResult<Vec<Sample>> {
        // Get lock on DB
        let db_cell = self.db.lock().await;
        let db = db_cell.as_ref().unwrap();

        // Get the matching key/value
        trace!(
            "getting key `{}` with parameters `{}`",
            key_expr,
            _parameters
        );
        match get_kv(db, &key_expr) {
            Ok(Some((value, timestamp))) => {
                Ok(vec![Sample::new(key_expr, value).with_timestamp(timestamp)])
            }
            Ok(None) => Err(format!("Entry not found for key `{}`", key_expr).into()),
            Err(e) => Err(format!("Error when getting key {} : {}", key_expr, e).into()),
        }
    }

    async fn get_all_entries(&self) -> ZResult<Vec<(OwnedKeyExpr, Timestamp)>> {
        let mut result = Vec::new();

        let db_cell = self.db.lock().await;
        let db = db_cell.as_ref().unwrap();

        // Iterate over DATA_INFO Column Family to avoid loading payloads
        let db_prefix = match &self.config.strip_prefix {
            Some(prefix) => prefix.as_str(),
            None => "",
        };
        for (key, buf) in db.prefix_iterator_cf(db.cf_handle(CF_DATA_INFO).unwrap(), db_prefix) {
            let key_str = String::from_utf8_lossy(&key);
            let res_ke = OwnedKeyExpr::new(key_str.as_ref());
            match res_ke {
                Ok(ke) => {
                    if let Ok((_, timestamp, _)) = decode_data_info(&buf) {
                        result.push((ke, timestamp))
                    } else {
                        bail!(
                            "Getting all entries : failed to decode data_info for key '{}'",
                            key_str
                        );
                    }
                }
                Err(e) => bail!("Invalid key in database: '{}' - {}", key_str, e),
            }
        }

        Ok(result)
    }
}

impl Drop for RocksdbStorage {
    fn drop(&mut self) {
        async_std::task::block_on(async move {
            // Get lock on DB and take DB so we can drop it before destroying it
            // (avoiding RocksDB lock to be taken twice)
            let mut db_cell = self.db.lock().await;
            let db = db_cell.take().unwrap();

            // Stop GC
            if let Some(t) = &mut self.timer {
                t.stop_async().await;
            }

            // Flush all
            if let Err(err) = db.flush() {
                warn!("Closing Rocksdb storage, flush failed: {}", err);
            }

            // copy path for later use after DB is dropped
            let path = db.path().to_path_buf();

            // drop DB, releasing RocksDB lock
            drop(db);

            match self.on_closure {
                OnClosure::DestroyDB => {
                    debug!(
                        "Close Rocksdb storage, destroying database {}",
                        path.display()
                    );
                    if let Err(err) = DB::destroy(&Options::default(), &path) {
                        error!(
                            "Failed to destroy Rocksdb database '{}' : {}",
                            path.display(),
                            err
                        );
                    }
                }
                OnClosure::DoNothing => {
                    debug!(
                        "Close Rocksdb storage, keeping database {} as it is",
                        path.display()
                    );
                }
            }
        });
    }
}

fn put_kv(
    db: &DB,
    key: &str,
    value: Value,
    timestamp: Timestamp,
) -> ZResult<StorageInsertionResult> {
    trace!("Put key {} in {:?}", key, db);
    let data_info = encode_data_info(&value.encoding, &timestamp, false)?;

    // Write content and encoding+timestamp in different Column Families
    let mut batch = WriteBatch::default();
    batch.put_cf(
        db.cf_handle(CF_PAYLOADS).unwrap(),
        key,
        value.payload.contiguous(),
    );
    batch.put_cf(db.cf_handle(CF_DATA_INFO).unwrap(), key, data_info);
    match db.write(batch) {
        Ok(()) => Ok(StorageInsertionResult::Inserted),
        Err(e) => Err(rocksdb_err_to_zerr(e)),
    }
}

fn delete_kv(db: &DB, key: &str) -> ZResult<StorageInsertionResult> {
    trace!("Delete key {} from {:?}", key, db);
    // Delete key from CF_PAYLOADS Column Family
    // Delete key from  CF_DATA_INFO Column Family (to remove metadata information)
    let mut batch = WriteBatch::default();
    batch.delete_cf(db.cf_handle(CF_PAYLOADS).unwrap(), key);
    batch.delete_cf(db.cf_handle(CF_DATA_INFO).unwrap(), key);
    match db.write(batch) {
        Ok(()) => Ok(StorageInsertionResult::Deleted),
        Err(e) => Err(rocksdb_err_to_zerr(e)),
    }
}

fn get_kv(db: &DB, key: &str) -> ZResult<Option<(Value, Timestamp)>> {
    trace!("Get key {} from {:?}", key, db);
    // TODO: use MultiGet when available (see https://github.com/rust-rocksdb/rust-rocksdb/issues/485)
    match (
        db.get_cf(db.cf_handle(CF_PAYLOADS).unwrap(), key),
        db.get_cf(db.cf_handle(CF_DATA_INFO).unwrap(), key),
    ) {
        (Ok(Some(payload)), Ok(Some(info))) => {
            trace!("first ok");
            let (encoding, timestamp, deleted) = decode_data_info(&info)?;
            if deleted {
                Ok(None)
            } else {
                Ok(Some((
                    Value::new(payload.into()).encoding(encoding),
                    timestamp,
                )))
            }
        }
        (Ok(Some(payload)), Ok(None)) => {
            trace!("second ok");
            // Only the payload is present in DB!
            // Possibly legacy data. Consider as encoding as APP_OCTET_STREAM and create timestamp from now()
            Ok(Some((
                Value::new(payload.into()).encoding(KnownEncoding::AppOctetStream.into()),
                new_reception_timestamp(),
            )))
        }
        (Ok(None), _) => Ok(None),
        (Err(err), _) | (_, Err(err)) => Err(rocksdb_err_to_zerr(err)),
    }
}

fn encode_data_info(encoding: &Encoding, timestamp: &Timestamp, deleted: bool) -> ZResult<Vec<u8>> {
    let codec = Zenoh060::default();
    let mut result = vec![];
    let mut writer = result.writer();

    // note: encode timestamp at first for faster decoding when only this one is required
    codec
        .write(&mut writer, timestamp)
        .map_err(|_| zerror!("Failed to encode data-info (timestamp)"))?;
    codec
        .write(&mut writer, deleted as u8)
        .map_err(|_| zerror!("Failed to encode data-info (deleted)"))?;
    codec
        .write(&mut writer, encoding)
        .map_err(|_| zerror!("Failed to encode data-info (encoding)"))?;
    Ok(result)
}

fn decode_data_info(buf: &[u8]) -> ZResult<(Encoding, Timestamp, bool)> {
    let codec = Zenoh060::default();
    let mut reader = buf.reader();
    let timestamp: Timestamp = codec
        .read(&mut reader)
        .map_err(|_| zerror!("Failed to decode data-info (timestamp)"))?;
    let deleted: u8 = codec
        .read(&mut reader)
        .map_err(|_| zerror!("Failed to decode data-info (deleted)"))?;
    let encoding: Encoding = codec
        .read(&mut reader)
        .map_err(|_| zerror!("Failed to decode data-info (timestamp)"))?;
    let deleted = deleted != 0;

    Ok((encoding, timestamp, deleted))
}

// decode the timestamp only
fn decode_timestamp(buf: &[u8]) -> ZResult<Timestamp> {
    let codec = Zenoh060::default();
    let mut reader = buf.reader();
    let timestamp: Timestamp = codec
        .read(&mut reader)
        .map_err(|_| zerror!("Failed to decode data-info (timestamp)"))?;
    Ok(timestamp)
}

// decode the deleted flag only
fn decode_deleted_flag(buf: &[u8]) -> ZResult<bool> {
    let codec = Zenoh060::default();
    let mut reader = buf.reader();
    let _timestamp: Timestamp = codec
        .read(&mut reader)
        .map_err(|_| zerror!("Failed to decode data-info (timestamp)"))?;
    let deleted: u8 = codec
        .read(&mut reader)
        .map_err(|_| zerror!("Failed to decode data-info (deleted)"))?;
    let deleted = deleted != 0;

    Ok(deleted)
}

fn rocksdb_err_to_zerr(err: rocksdb::Error) -> zenoh_core::Error {
    zerror!("Rocksdb error: {}", err.into_string()).into()
}

// Periodic event cleaning-up data info for no-longer existing files
struct GarbageCollectionEvent {
    db: Arc<Mutex<Option<DB>>>,
}

#[async_trait]
impl Timed for GarbageCollectionEvent {
    async fn run(&mut self) {
        trace!("Start garbage collection of obsolete data-infos");
        let time_limit = NTP64::from(SystemTime::now().duration_since(UNIX_EPOCH).unwrap())
            - *MIN_DELAY_BEFORE_REMOVAL;

        // Get lock on DB
        let db_cell = self.db.lock().await;
        let db = db_cell.as_ref().unwrap();

        // prepare a batch with all keys to delete
        let cf_handle = db.cf_handle(CF_DATA_INFO).unwrap();
        let mut batch = WriteBatch::default();
        let mut count = 0;
        for (key, buf) in db.iterator_cf(cf_handle, IteratorMode::Start) {
            if let Ok(true) = decode_deleted_flag(&buf) {
                if let Ok(timestamp) = decode_timestamp(&buf) {
                    if timestamp.get_time() < &time_limit {
                        batch.delete_cf(cf_handle, key);
                        count += 1;
                    }
                }
            }
        }

        // write batch
        if count > 0 {
            trace!("Garbage collect {} old data-info", count);
            if let Err(err) = db.write(batch).map_err(rocksdb_err_to_zerr) {
                warn!("Failed to clean-up old data-info : {}", err);
            }
        }

        trace!("End garbage collection of obsolete data-infos");
    }
}
