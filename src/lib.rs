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

use std::{
    borrow::Cow, collections::HashMap, future::Future, path::PathBuf, sync::Arc, time::Duration,
};

use async_trait::async_trait;
use rocksdb::{ColumnFamilyDescriptor, Options, WriteBatch, DB};
use tokio::sync::Mutex;
use tracing::{debug, error, trace, warn};
use uhlc::NTP64;
use zenoh::{
    bytes::{Encoding, ZBytes},
    internal::{bail, zenoh_home, zerror, Value},
    key_expr::OwnedKeyExpr,
    query::Parameters,
    time::Timestamp,
    try_init_log_from_env, Error, Result as ZResult,
};
use zenoh_backend_traits::{
    config::{StorageConfig, VolumeConfig},
    *,
};
use zenoh_plugin_trait::{plugin_long_version, plugin_version, Plugin};

const WORKER_THREAD_NUM: usize = 2;
const MAX_BLOCK_THREAD_NUM: usize = 50;
lazy_static::lazy_static! {
    // The global runtime is used in the dynamic plugins, which we can't get the current runtime
    static ref TOKIO_RUNTIME: tokio::runtime::Runtime = tokio::runtime::Builder::new_multi_thread()
               .worker_threads(WORKER_THREAD_NUM)
               .max_blocking_threads(MAX_BLOCK_THREAD_NUM)
               .enable_all()
               .build()
               .expect("Unable to create runtime");
}
#[inline(always)]
fn blockon_runtime<F: Future>(task: F) -> F::Output {
    // Check whether able to get the current runtime
    match tokio::runtime::Handle::try_current() {
        Ok(rt) => {
            // Able to get the current runtime (standalone binary), spawn on the current runtime
            tokio::task::block_in_place(|| rt.block_on(task))
        }
        Err(_) => {
            // Unable to get the current runtime (dynamic plugins), spawn on the global runtime
            tokio::task::block_in_place(|| TOKIO_RUNTIME.block_on(task))
        }
    }
}

/// The environement variable used to configure the root of all storages managed by this RocksdbBackend.
pub const SCOPE_ENV_VAR: &str = "ZENOH_BACKEND_ROCKSDB_ROOT";

/// The default root (whithin zenoh's home directory) if the ZENOH_BACKEND_ROCKSDB_ROOT environment variable is not specified.
pub const DEFAULT_ROOT_DIR: &str = "zenoh_backend_rocksdb";

// Properties used by the Backend
//  - None

// Properties used by the Storage
pub const PROP_STORAGE_DIR: &str = "dir";
pub const PROP_STORAGE_CREATE_DB: &str = "create_db";
pub const PROP_STORAGE_READ_ONLY: &str = "read_only";
pub const PROP_STORAGE_ON_CLOSURE: &str = "on_closure";

// Special key for None (when the prefix being stripped exactly matches the key)
pub const NONE_KEY: &str = "@@none_key@@";

// Column family names
const CF_PAYLOADS: &str = rocksdb::DEFAULT_COLUMN_FAMILY_NAME;
const CF_DATA_INFO: &str = "data_info";

lazy_static::lazy_static! {
    static ref GC_PERIOD: Duration = Duration::new(5, 0);
    static ref MIN_DELAY_BEFORE_REMOVAL: NTP64 = NTP64::from(Duration::new(5, 0));
}

pub(crate) enum OnClosure {
    DestroyDB,
    DoNothing,
}

pub struct RocksDbBackend {}

#[cfg(feature = "dynamic_plugin")]
zenoh_plugin_trait::declare_plugin!(RocksDbBackend);

impl Plugin for RocksDbBackend {
    type StartArgs = VolumeConfig;
    type Instance = VolumeInstance;

    const DEFAULT_NAME: &'static str = "rocks_backend";
    const PLUGIN_VERSION: &'static str = plugin_version!();
    const PLUGIN_LONG_VERSION: &'static str = plugin_long_version!();

    fn start(_name: &str, _config: &Self::StartArgs) -> ZResult<Self::Instance> {
        try_init_log_from_env();
        debug!("RocksDB backend {}", Self::PLUGIN_LONG_VERSION);

        let root = if let Some(dir) = std::env::var_os(SCOPE_ENV_VAR) {
            PathBuf::from(dir)
        } else {
            let mut dir = PathBuf::from(zenoh_home());
            dir.push(DEFAULT_ROOT_DIR);
            dir
        };
        let mut properties = Parameters::default();
        properties.insert::<String, String>("root".into(), root.to_string_lossy().into());
        properties.insert::<String, String>("version".into(), Self::PLUGIN_VERSION.into());

        let admin_status = HashMap::from(properties)
            .into_iter()
            .map(|(k, v)| (k, serde_json::Value::String(v)))
            .collect();
        Ok(Box::new(RocksdbVolume { admin_status, root }))
    }
}

pub struct RocksdbVolume {
    admin_status: serde_json::Value,
    root: PathBuf,
}

#[async_trait]
impl Volume for RocksdbVolume {
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

    async fn create_storage(&self, config: StorageConfig) -> ZResult<Box<dyn Storage>> {
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

        Ok(Box::new(RocksdbStorage {
            config,
            on_closure,
            read_only,
            db,
        }))
    }
}

struct RocksdbStorage {
    config: StorageConfig,
    on_closure: OnClosure,
    read_only: bool,
    // Note: rocksdb isn't thread-safe. See https://github.com/rust-rocksdb/rust-rocksdb/issues/404
    db: Arc<Mutex<Option<DB>>>,
}

#[async_trait]
impl Storage for RocksdbStorage {
    fn get_admin_status(&self) -> serde_json::Value {
        self.config.to_json_value()
    }

    // When receiving a PUT operation
    async fn put(
        &mut self,
        key: Option<OwnedKeyExpr>,
        value: Value,
        timestamp: Timestamp,
    ) -> ZResult<StorageInsertionResult> {
        // Get lock on DB
        let db_cell = self.db.lock().await;

        let db = db_cell
            .as_ref()
            .ok_or(zerror!("Could not get DB ref in put"))?;

        // Store the sample
        debug!(
            "Storing key, and value with timestamp: {:?} : {}",
            key, timestamp
        );
        if !self.read_only {
            // put payload and data_info in DB
            put_kv(db, key, value, timestamp)
        } else {
            warn!("Received PUT for read-only DB on {:?} - ignored", key);
            Err("Received update for read-only DB".into())
        }
    }

    // When receiving a DEL operation
    async fn delete(
        &mut self,
        key: Option<OwnedKeyExpr>,
        _timestamp: Timestamp,
    ) -> ZResult<StorageInsertionResult> {
        // Get lock on DB
        let db_cell = self.db.lock().await;
        let db = db_cell
            .as_ref()
            .ok_or(zerror!("Could not get DB ref in delete"))?;

        debug!("Deleting key: {:?}", key);
        if !self.read_only {
            // delete file
            delete_kv(db, key)
        } else {
            warn!("Received DELETE for read-only DB on {:?} - ignored", key);
            Err("Received update for read-only DB".into())
        }
    }

    // When receiving a GET operation
    async fn get(
        &mut self,
        key: Option<OwnedKeyExpr>,
        _parameters: &str,
    ) -> ZResult<Vec<StoredData>> {
        // Get lock on DB
        let db_cell = self.db.lock().await;
        let db = db_cell
            .as_ref()
            .ok_or(zerror!("Could not get DB ref in get"))?;

        // Get the matching key/value
        debug!("getting key `{:?}` with parameters `{}`", key, _parameters);
        match get_kv(db, key.clone()) {
            Ok(Some((value, timestamp))) => Ok(vec![StoredData { value, timestamp }]),
            Ok(None) => Ok(vec![]),
            Err(e) => Err(format!("Error when getting key {:?} : {}", key, e).into()),
        }
    }

    async fn get_all_entries(&self) -> ZResult<Vec<(Option<OwnedKeyExpr>, Timestamp)>> {
        let mut result = Vec::new();

        let db_cell = self.db.lock().await;
        let db = db_cell
            .as_ref()
            .ok_or(zerror!("Could not get DB ref in get_all_entries"))?;

        let cf_handle = db.cf_handle(CF_DATA_INFO).ok_or(zerror!(
            "Option for ColumFamily {CF_DATA_INFO} was None, cancel get_all_entries"
        ))?;

        // Iterate over DATA_INFO Column Family to avoid loading payloads
        for item in db.prefix_iterator_cf(cf_handle, "") {
            let (key, buf) = item.map_err(|e| zerror!("{}", e))?;
            let key_str = String::from_utf8_lossy(&key);
            let res_ke = if key_str == NONE_KEY {
                None
            } else {
                match OwnedKeyExpr::new(key_str.as_ref()) {
                    Ok(ke) => Some(ke),
                    Err(e) => bail!("Invalid key in database: '{}' - {}", key_str, e),
                }
            };
            if let Ok((_, timestamp, _)) = decode_data_info(&buf) {
                result.push((res_ke, timestamp))
            } else {
                bail!(
                    "Getting all entries : failed to decode data_info for key '{}'",
                    key_str
                );
            }
        }

        Ok(result)
    }
}

impl Drop for RocksdbStorage {
    fn drop(&mut self) {
        blockon_runtime(async move {
            // Get lock on DB and take DB so we can drop it before destroying it
            // (avoiding RocksDB lock to be taken twice)
            let mut db_cell = self.db.lock().await;

            if let Some(db) = db_cell.take() {
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
            } else {
                warn!("Tried Dropping DB connection, however D Connection internally was None, Continuing");
            };
        });
    }
}

fn put_kv(
    db: &DB,
    key: Option<OwnedKeyExpr>,
    value: Value,
    timestamp: Timestamp,
) -> ZResult<StorageInsertionResult> {
    trace!("Put key {:?} in {:?}", key, db);

    let data_info = encode_data_info(value.encoding().clone(), &timestamp, false)?;

    let key = match key {
        Some(k) => k.to_string(),
        None => NONE_KEY.to_string(),
    };
    // Write content and encoding+timestamp in different Column Families
    let mut batch = WriteBatch::default();
    batch.put_cf(
        db.cf_handle(CF_PAYLOADS).ok_or(zerror!(
            "Option for ColumFamily {CF_PAYLOADS} was None, cancel put_kv"
        ))?,
        &key,
        value.payload().into::<Cow<[u8]>>(),
    );
    batch.put_cf(
        db.cf_handle(CF_DATA_INFO).ok_or(zerror!(
            "Option for ColumFamily {CF_DATA_INFO} was None, cancel put_kv"
        ))?,
        &key,
        data_info,
    );

    match db.write(batch) {
        Ok(()) => Ok(StorageInsertionResult::Inserted),
        Err(e) => Err(rocksdb_err_to_zerr(e)),
    }
}

fn delete_kv(db: &DB, key: Option<OwnedKeyExpr>) -> ZResult<StorageInsertionResult> {
    trace!("Delete key {:?} from {:?}", key, db);
    // Delete key from CF_PAYLOADS Column Family
    // Delete key from  CF_DATA_INFO Column Family (to remove metadata information)
    let mut batch = WriteBatch::default();
    let key = match key {
        Some(k) => k.to_string(),
        None => NONE_KEY.to_string(),
    };

    if let Some(cf_payloads) = db.cf_handle(CF_PAYLOADS) {
        batch.delete_cf(cf_payloads, &key);
    } else {
        warn!("Option for ColumFamily {CF_PAYLOADS} was None, continue with delete_kv, ignoring CF_PAYLOADS");
    }

    if let Some(cf_data_info) = db.cf_handle(CF_DATA_INFO) {
        batch.delete_cf(cf_data_info, &key);
    } else {
        warn!("Option for ColumFamily {CF_DATA_INFO} was None, continue with delete_kv, ignoring CF_DATA_INFO");
    }

    match db.write(batch) {
        Ok(()) => Ok(StorageInsertionResult::Deleted),
        Err(e) => Err(rocksdb_err_to_zerr(e)),
    }
}

fn get_kv(db: &DB, key: Option<OwnedKeyExpr>) -> ZResult<Option<(Value, Timestamp)>> {
    trace!("Get key {:?} from {:?}", key, db);
    // TODO: use MultiGet when available (see https://github.com/rust-rocksdb/rust-rocksdb/issues/485)
    let key = match key {
        Some(k) => k.to_string(),
        None => NONE_KEY.to_string(),
    };

    let (cf_payloads, cf_data_info) = match (db.cf_handle(CF_PAYLOADS), db.cf_handle(CF_DATA_INFO))
    {
        (None, None) => {
            bail!("Option for ColumFamily {CF_PAYLOADS} and {CF_DATA_INFO} were both 'None'")
        }
        (None, Some(_)) => bail!("Option for ColumFamily {CF_DATA_INFO} is 'None'"),
        (Some(_), None) => bail!("Option for ColumFamily {CF_PAYLOADS} is 'None'"),
        (Some(cf_payloads), Some(cf_data_info)) => (cf_payloads, cf_data_info),
    };

    match (db.get_cf(cf_payloads, &key), db.get_cf(cf_data_info, &key)) {
        (Ok(Some(payload)), Ok(Some(info))) => {
            trace!("first ok");
            let (encoding, timestamp, deleted) = decode_data_info(&info)?;

            if deleted {
                Ok(None)
            } else {
                Ok(Some((Value::new(payload, encoding), timestamp)))
            }
        }
        (Ok(_), Ok(None)) => {
            trace!("second ok");
            warn!("no {CF_DATA_INFO} in database, however payload data exists. Possible Legacy / Dirty state of Database. Unsupported in case of Replication");
            Ok(None)
        }
        (Ok(None), _) => Ok(None),
        (Err(err), _) | (_, Err(err)) => Err(rocksdb_err_to_zerr(err)),
    }
}

fn encode_data_info(encoding: Encoding, timestamp: &Timestamp, deleted: bool) -> ZResult<Vec<u8>> {
    let mut bytes = ZBytes::empty();
    let mut writer = bytes.writer();

    writer.serialize(timestamp);
    writer.serialize(deleted as u8);
    writer.serialize(&encoding);

    Ok(bytes.into())
}

fn decode_data_info(buf: &[u8]) -> ZResult<(Encoding, Timestamp, bool)> {
    let bytes = ZBytes::from(buf);
    let mut reader = bytes.reader();

    let timestamp: Timestamp = reader
        .deserialize()
        .map_err(|_| zerror!("Failed to decode data-info (timestamp)"))?;

    let deleted: u8 = reader
        .deserialize()
        .map_err(|_| zerror!("Failed to decode data-info (deleted)"))?;

    let encoding: Encoding = reader
        .deserialize()
        .map_err(|_| zerror!("Failed to decode data-info (Encoding)"))?;

    let deleted = deleted != 0;

    Ok((encoding, timestamp, deleted))
}

fn rocksdb_err_to_zerr(err: rocksdb::Error) -> Error {
    zerror!("Rocksdb error: {}", err.into_string()).into()
}
