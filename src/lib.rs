//
// Copyright (c) 2017, 2020 ADLINK Technology Inc.
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ADLINK zenoh team, <zenoh@adlink-labs.tech>
//

use async_std::sync::{Arc, Mutex};
use async_trait::async_trait;
use log::{debug, error, trace, warn};
use rocksdb::{ColumnFamilyDescriptor, IteratorMode, Options, WriteBatch, DB};
use std::convert::TryFrom;
use std::io::prelude::*;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use uhlc::NTP64;
use zenoh::net::utils::resource_name;
use zenoh::net::{DataInfo, Sample, ZBuf, ZInt};
use zenoh::{
    Change, ChangeKind, Properties, Selector, Timestamp, Value, ZError, ZErrorKind, ZResult,
};
use zenoh_backend_traits::*;
use zenoh_util::collections::{Timed, TimedEvent, Timer};
use zenoh_util::{zenoh_home, zerror, zerror2};

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

// maximum size of serialized data-info: deleted (u8) + encoding (u64) + timestamp (u64 + ID at max size)
const MAX_VAL_LEN: usize = 1 + 8 + 8 + uhlc::ID::MAX_SIZE;
// minimum size of serialized data-info: deleted (u8) + encoding (u64) + timestamp (u64 + ID at 1 byte)
const MIN_VAL_LEN: usize = 1 + 8 + 8 + 1;

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

#[no_mangle]
pub fn create_backend(_unused: &Properties) -> ZResult<Box<dyn Backend>> {
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

    let admin_status = zenoh::utils::properties_to_json_value(&properties);
    Ok(Box::new(RocksdbBackend { admin_status, root }))
}

pub struct RocksdbBackend {
    admin_status: Value,
    root: PathBuf,
}

#[async_trait]
impl Backend for RocksdbBackend {
    async fn get_admin_status(&self) -> Value {
        self.admin_status.clone()
    }

    async fn create_storage(&mut self, props: Properties) -> ZResult<Box<dyn Storage>> {
        let path_expr = props.get(PROP_STORAGE_PATH_EXPR).unwrap();
        let path_prefix = props
            .get(PROP_STORAGE_PATH_PREFIX)
            .ok_or_else(|| {
                zerror2!(ZErrorKind::Other {
                    descr: format!(
                        r#"Missing required property for File System Storage: "{}""#,
                        PROP_STORAGE_PATH_PREFIX
                    )
                })
            })?
            .clone();
        if !path_expr.starts_with(&path_prefix) {
            return zerror!(ZErrorKind::Other {
                descr: format!(
                    r#"The specified "{}={}" is not a prefix of "{}={}""#,
                    PROP_STORAGE_PATH_PREFIX, path_prefix, PROP_STORAGE_PATH_EXPR, path_expr
                )
            });
        }

        let read_only = props.contains_key(PROP_STORAGE_READ_ONLY);

        let on_closure = match props.get(PROP_STORAGE_ON_CLOSURE) {
            Some(s) => {
                if s == "destroy_db" {
                    OnClosure::DestroyDB
                } else {
                    return zerror!(ZErrorKind::Other {
                        descr: format!("Unsupported value for 'on_closure' property: {}", s)
                    });
                }
            }
            None => OnClosure::DoNothing,
        };

        let db_path = props
            .get(PROP_STORAGE_DIR)
            .map(|dir| {
                // prepend db_path with self.root
                let mut db_path = self.root.clone();
                for segment in dir.split(std::path::MAIN_SEPARATOR) {
                    if !segment.is_empty() {
                        db_path.push(segment);
                    }
                }
                db_path
            })
            .ok_or_else(|| {
                zerror2!(ZErrorKind::Other {
                    descr: format!(
                        r#"Missing required property for File System Storage: "{}""#,
                        PROP_STORAGE_DIR
                    )
                })
            })?;

        let mut opts = Options::default();
        if props.contains_key(PROP_STORAGE_CREATE_DB) {
            opts.create_if_missing(true);
        }
        opts.create_missing_column_families(true);
        let db = if read_only {
            DB::open_cf_for_read_only(&opts, &db_path, &[CF_PAYLOADS, CF_DATA_INFO], true)
        } else {
            let cf_payloads = ColumnFamilyDescriptor::new(CF_PAYLOADS, opts.clone());
            let cf_data_info = ColumnFamilyDescriptor::new(CF_DATA_INFO, opts.clone());
            DB::open_cf_descriptors(&opts, &db_path, vec![cf_payloads, cf_data_info])
        }
        .map_err(|e| {
            zerror2!(ZErrorKind::Other {
                descr: format!(
                    "Failed to open data-info database from {:?}: {}",
                    db_path, e
                )
            })
        })?;
        let db = Arc::new(Mutex::new(Some(db)));

        let timer = if read_only {
            None
        } else {
            // start periodic GC event
            let t = Timer::new();
            let gc = TimedEvent::periodic(*GC_PERIOD, GarbageCollectionEvent { db: db.clone() });
            let _ = t.add(gc).await;
            Some(t)
        };

        let admin_status = zenoh::utils::properties_to_json_value(&props);
        Ok(Box::new(RocksdbStorage {
            admin_status,
            path_prefix,
            on_closure,
            read_only,
            db,
            timer,
        }))
    }

    fn incoming_data_interceptor(&self) -> Option<Box<dyn IncomingDataInterceptor>> {
        None
    }

    fn outgoing_data_interceptor(&self) -> Option<Box<dyn OutgoingDataInterceptor>> {
        None
    }
}

struct RocksdbStorage {
    admin_status: Value,
    path_prefix: String,
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
    async fn get_admin_status(&self) -> Value {
        self.admin_status.clone()
    }

    // When receiving a Sample (i.e. on PUT or DELETE operations)
    async fn on_sample(&mut self, sample: Sample) -> ZResult<()> {
        // transform the Sample into a Change to get kind, encoding and timestamp (not decoding => RawValue)
        let change = Change::from_sample(sample, false)?;

        // the key in rocksdb is the path stripped from "path_prefix"
        let key = change
            .path
            .as_str()
            .strip_prefix(&self.path_prefix)
            .ok_or_else(|| {
                zerror2!(ZErrorKind::Other {
                    descr: format!(
                        "Received a Sample not starting with path_prefix '{}'",
                        self.path_prefix
                    )
                })
            })?;

        // Get lock on DB
        let db_cell = self.db.lock().await;
        let db = db_cell.as_ref().unwrap();

        // get latest timestamp for this key (if already exists in db)
        // and drop incoming sample if older
        if let Some(old_ts) = get_timestamp(&db, key)? {
            if change.timestamp < old_ts {
                debug!("{} on {} dropped: out-of-date", change.kind, change.path);
                return Ok(());
            }
        }

        // Store or delete the sample depending the ChangeKind
        match change.kind {
            ChangeKind::Put => {
                if !self.read_only {
                    // check that there is a value for this PUT sample
                    if change.value.is_none() {
                        return zerror!(ZErrorKind::Other {
                            descr: format!(
                                "Received a PUT Sample without value for {}",
                                change.path
                            )
                        });
                    }

                    // get the encoding and buffer from the value (RawValue => direct access to inner ZBuf)
                    let (encoding, payload) = change.value.unwrap().encode();

                    // put payload and data_info in DB
                    put_kv(&db, key, payload, encoding, change.timestamp)
                } else {
                    warn!("Received PUT for read-only DB on {:?} - ignored", key);
                    Ok(())
                }
            }
            ChangeKind::Delete => {
                if !self.read_only {
                    // delete file
                    delete_kv(&db, key, change.timestamp)
                } else {
                    warn!("Received DELETE for read-only DB on {:?} - ignored", key);
                    Ok(())
                }
            }
            ChangeKind::Patch => {
                warn!("Received PATCH for {}: not yet supported", change.path);
                Ok(())
            }
        }
    }

    // When receiving a Query (i.e. on GET operations)
    async fn on_query(&mut self, query: Query) -> ZResult<()> {
        // get the query's Selector
        let selector = Selector::try_from(&query)?;

        // get the list of sub-path expressions that will match the same stored keys than
        // the selector, if those keys had the path_prefix.
        let path_exprs = utils::get_sub_path_exprs(selector.path_expr.as_str(), &self.path_prefix);
        debug!(
            "Query on {} with path_prefix={} => sub_path_exprs = {:?}",
            selector.path_expr, self.path_prefix, path_exprs
        );

        // Get lock on DB
        let db_cell = self.db.lock().await;
        let db = db_cell.as_ref().unwrap();

        // Get all matching keys/values
        let mut kvs: Vec<(String, Vec<u8>, ZInt, Timestamp)> = Vec::with_capacity(path_exprs.len());
        for path_expr in path_exprs {
            if path_expr.contains('*') {
                find_matching_kv(&db, &path_expr, &mut kvs);
            } else {
                // path_expr correspond to 1 key. Get it.
                match get_kv(&db, &path_expr) {
                    Ok(Some((payload, encoding, timestamp))) => {
                        kvs.push((path_expr.into(), payload, encoding, timestamp))
                    }
                    Ok(None) => (), // key not found, do nothing
                    Err(e) => warn!(
                        "Replying to query on {} : failed get key {} : {}",
                        query.res_name(),
                        path_expr,
                        e
                    ),
                }
            }
        }

        // Release lock on DB
        drop(db_cell);

        // Send replies
        for (key, payload, encoding, timestamp) in kvs {
            // append path_prefix to the key
            let path = concat_str(&self.path_prefix, &key);
            let mut info = DataInfo::new();
            info.encoding = Some(encoding);
            info.timestamp = Some(timestamp);
            query
                .reply(Sample {
                    res_name: path,
                    payload: payload.into(),
                    data_info: Some(info),
                })
                .await;
        }

        Ok(())
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
                t.stop().await;
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

fn put_kv(db: &DB, key: &str, content: ZBuf, encoding: ZInt, timestamp: Timestamp) -> ZResult<()> {
    trace!("Put key {} in {:?}", key, db);
    let data_info = encode_data_info(encoding, timestamp, false)?;

    // Write content and encoding+timestamp in different Column Families
    let mut batch = WriteBatch::default();
    batch.put_cf(db.cf_handle(CF_PAYLOADS).unwrap(), key, content.to_vec());
    batch.put_cf(db.cf_handle(CF_DATA_INFO).unwrap(), key, data_info);
    db.write(batch).map_err(rocksdb_err_to_zerr)
}

fn delete_kv(db: &DB, key: &str, timestamp: Timestamp) -> ZResult<()> {
    trace!("Delete key {} from {:?}", key, db);
    let data_info = encode_data_info(zenoh::net::encoding::NONE, timestamp, true)?;

    // Delete key from CF_PAYLOADS Column Family
    // Put deletion timestamp into CF_DATA_INFO Column Family (to avoid re-insertion of older value)
    let mut batch = WriteBatch::default();
    batch.delete_cf(db.cf_handle(CF_PAYLOADS).unwrap(), key);
    batch.put_cf(db.cf_handle(CF_DATA_INFO).unwrap(), key, data_info);
    db.write(batch).map_err(rocksdb_err_to_zerr)
}

fn get_kv(db: &DB, key: &str) -> ZResult<Option<(Vec<u8>, ZInt, Timestamp)>> {
    trace!("Get key {} from {:?}", key, db);
    // TODO: use MultiGet when available (see https://github.com/rust-rocksdb/rust-rocksdb/issues/485)
    match (
        db.get_cf(db.cf_handle(CF_PAYLOADS).unwrap(), key),
        db.get_cf(db.cf_handle(CF_DATA_INFO).unwrap(), key),
    ) {
        (Ok(Some(payload)), Ok(Some(info))) => {
            let (encoding, timestamp, deleted) = decode_data_info(&info)?;
            if deleted {
                Ok(None)
            } else {
                Ok(Some((payload, encoding, timestamp)))
            }
        }
        (Ok(Some(payload)), Ok(None)) => {
            // Only the payload is present in DB!
            // Possibly legacy data. Consider as encoding as NONE and create timestamp from now()
            let timestamp = zenoh::utils::new_reception_timestamp();
            Ok(Some((payload, zenoh::net::encoding::NONE, timestamp)))
        }
        (Ok(None), _) => Ok(None),
        (Err(err), _) | (_, Err(err)) => Err(rocksdb_err_to_zerr(err)),
    }
}

fn find_matching_kv(
    db: &DB,
    path_expr: &str,
    results: &mut Vec<(String, Vec<u8>, ZInt, Timestamp)>,
) {
    // Use Rocksdb prefix seek for faster search
    let prefix = &path_expr[..path_expr.find('*').unwrap()];
    trace!(
        "Find keys matching {} from {:?} using prefix seek with '{}'",
        path_expr,
        db,
        prefix
    );

    // Iterate over DATA_INFO Column Family to avoid loading payloads possibly for nothing if not matching
    for (key, buf) in db.prefix_iterator_cf(db.cf_handle(CF_DATA_INFO).unwrap(), prefix) {
        if let Ok(false) = decode_deleted_flag(&buf) {
            let key_str = String::from_utf8_lossy(&key);
            if resource_name::intersect(&key_str, path_expr) {
                match db.get_cf(db.cf_handle(CF_PAYLOADS).unwrap(), &key) {
                    Ok(Some(payload)) => {
                        if let Ok((encoding, timestamp, _)) = decode_data_info(&buf) {
                            results.push((key_str.into_owned(), payload, encoding, timestamp))
                        } else {
                            warn!(
                                "Replying to query on {} : failed to decode data_info for key {}",
                                path_expr, key_str
                            )
                        }
                    }
                    Ok(None) => (), // data_info exists, but not payload: key was probably deleted
                    Err(err) => warn!(
                        "Replying to query on {} : failed get key {} : {}",
                        path_expr, key_str, err
                    ),
                }
            }
        }
    }
}

fn get_timestamp(db: &DB, key: &str) -> ZResult<Option<Timestamp>> {
    match db.get_pinned_cf(db.cf_handle(CF_DATA_INFO).unwrap(), key) {
        Ok(Some(pin_val)) => decode_timestamp(pin_val.as_ref()).map(Some),
        Ok(None) => {
            trace!("timestamp for {:?} not found", key);
            Ok(None)
        }
        Err(e) => zerror!(ZErrorKind::Other {
            descr: format!("Failed to get data-info for {:?}: {}", key, e)
        }),
    }
}

fn encode_data_info(encoding: ZInt, timestamp: Timestamp, deleted: bool) -> ZResult<Vec<u8>> {
    // Encoding format is the following:
    //  - bytes[0]:     the "deleted" boolean as u8
    //  - bytes[1..9]:  the encoding as u64
    //  - bytes[9..17]: the timestamp's time as u64
    //  - bytes[17..]:  the timestamp's ID
    let mut buf: Vec<u8> = Vec::with_capacity(MAX_VAL_LEN);
    buf.push(if deleted { 1u8 } else { 0u8 });
    buf.write_all(&encoding.to_ne_bytes())
        .and_then(|()| buf.write_all(&timestamp.get_time().as_u64().to_ne_bytes()))
        .and_then(|()| buf.write_all(timestamp.get_id().as_slice()))
        .map_err(|e| {
            zerror2!(ZErrorKind::Other {
                descr: format!("Failed to encode data-info for: {}", e)
            })
        })?;
    Ok(buf)
}

fn decode_data_info(buf: &[u8]) -> ZResult<(ZInt, Timestamp, bool)> {
    if buf.len() < MIN_VAL_LEN {
        return zerror!(ZErrorKind::Other {
            descr: "Failed to decode data-info (buffer too small)".to_string()
        });
    }
    let deleted = buf[0] > 0;
    let mut encoding_bytes = [0u8; 8];
    encoding_bytes.clone_from_slice(&buf[1..9]);
    let encoding = ZInt::from_ne_bytes(encoding_bytes);
    let mut time_bytes = [0u8; 8];
    time_bytes.clone_from_slice(&buf[9..17]);
    let time = u64::from_ne_bytes(time_bytes);
    let id = uhlc::ID::try_from(&buf[17..]).unwrap();
    let timestamp = Timestamp::new(NTP64(time), id);
    Ok((encoding, timestamp, deleted))
}

// decode the timestamp only
fn decode_timestamp(buf: &[u8]) -> ZResult<Timestamp> {
    if buf.len() < MIN_VAL_LEN {
        return zerror!(ZErrorKind::Other {
            descr: "Failed to decode data-info (buffer too small)".to_string()
        });
    }
    let mut time_bytes = [0u8; 8];
    time_bytes.clone_from_slice(&buf[9..17]);
    let time = u64::from_ne_bytes(time_bytes);
    let id = uhlc::ID::try_from(&buf[17..]).unwrap();
    Ok(Timestamp::new(NTP64(time), id))
}

// decode the deleted flag only
fn decode_deleted_flag(buf: &[u8]) -> ZResult<bool> {
    if buf.len() < MIN_VAL_LEN {
        return zerror!(ZErrorKind::Other {
            descr: "Failed to decode data-info (buffer too small)".to_string()
        });
    }
    Ok(buf[0] > 0)
}

fn rocksdb_err_to_zerr(err: rocksdb::Error) -> ZError {
    zerror2!(ZErrorKind::Other {
        descr: format!("Rocksdb error: {}", err.into_string())
    })
}

pub(crate) fn concat_str<S1: AsRef<str>, S2: AsRef<str>>(s1: S1, s2: S2) -> String {
    let mut result = String::with_capacity(s1.as_ref().len() + s2.as_ref().len());
    result.push_str(s1.as_ref());
    result.push_str(s2.as_ref());
    result
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
