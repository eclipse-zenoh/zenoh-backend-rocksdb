<img src="http://zenoh.io/img/zenoh-dragon-small.png" width="150">

[![CI](https://github.com/eclipse-zenoh/zenoh-backend-rocksdb/workflows/CI/badge.svg)](https://github.com/eclipse-zenoh/zenoh-backend-rocksdb/actions?query=workflow%3A%22CI%22)
[![Gitter](https://badges.gitter.im/atolab/zenoh.svg)](https://gitter.im/atolab/zenoh?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)
[![License](https://img.shields.io/badge/License-EPL%202.0-blue)](https://choosealicense.com/licenses/epl-2.0/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

# RocksDB backend for Eclipse zenoh

In zenoh a backend is a storage technology (such as DBMS, time-series database, file system...) alowing to store the
keys/values publications made via zenoh and return them on queries.
See the [zenoh documentation](http://zenoh.io/docs/manual/backends/) for more details.

This backend relies on [RocksDB](https://rocksdb.org/) to implement the storages.
Its library name (without OS specific prefix and extension) that zenoh will rely on to find it and load it is **`zbackend_rocksdb`**.

:point_right: **Download:** https://download.eclipse.org/zenoh/zenoh-backend-rocksdb/

-------------------------------
## **Examples of usage**

Prerequisites:
 - You have a zenoh router running, and the `zbackend_rocksdb` library file is available in `~/.zenoh/lib`.
 - Declare the `ZBACKEND_ROCKSDB_ROOT` environment variable to the directory where you want the RocksDB databases
   to be stored. If you don't declare it, the `~/.zenoh/zbackend_rocksdb` directory will be used.

Using `curl` on the zenoh router to add backend and storages:
```bash
# Add a backend that will have all its storages storing data RocksDB databases under the ${ZBACKEND_ROCKSDB_ROOT} directory.
curl -X PUT -H 'content-type:application/properties' http://localhost:8000/@/router/local/plugin/storages/backend/rocksdb

# Add a storage on /demo/example/** storing data in a ${ZBACKEND_ROCKSDB_ROOT}/test RocksDB database.
# We use 'path_prefix=/demo/example' thus a zenoh path "/demo/example/a/b" will be stored using "a/b" as key in RocksDB
curl -X PUT -H 'content-type:application/properties' -d "path_expr=/demo/example/**;path_prefix=/demo/example;dir=test;create_db" http://localhost:8000/@/router/local/plugin/storages/backend/rocksdb/storage/example

# Put values that will be stored in the RocksDB database
curl -X PUT -d "TEST-1" http://localhost:8000/demo/example/test-1
curl -X PUT -d "B" http://localhost:8000/demo/example/a/b

# Retrive the values
curl http://localhost:8000/demo/example/**
```

-------------------------------
## **Properties for Backend creation**

- **`"lib"`** (optional) : the path to the backend library file. If not speficied, the Backend identifier in admin space must be `rocksdb` (i.e. zenoh will automatically search for a library named `zbackend_rocksdb`).

-------------------------------
## **Properties for Storage creation**

- **`"path_expr"`** (**required**) : the Storage's [Path Expression](../abstractions#path-expression)

- **`"path_prefix"`** (optional) : a prefix of the `"path_expr"` that will be stripped from each path to store.  
  _Example: with `"path_expr"="/demo/example/**"` and `"path_prefix"="/demo/example/"` the path `"/demo/example/foo/bar"` will be stored as key: `"foo/bar"`. But replying to a get on `"/demo/**"`, the key `"foo/bar"` will be transformed back to the original path (`"/demo/example/foo/bar"`)._

- **`"dir"`** (**required**) : The name of directory where the RocksDB database is stored.
  The absolute path will be `${ZBACKEND_ROCKSDB_ROOT}/<dir>`.

- **`"create_db"`** (optional) : create the RocksDB database if not already existing. Not set by default.
  *(the value doesn't matter, only the property existence is checked)*

- **`"read_only"`** (optional) : the storage will only answer to GET queries. It will not accept any PUT or DELETE message, and won't put anything in RocksDB database. Not set by default. *(the value doesn't matter, only the property existence is checked)*

- **`"on_closure"`** (optional) : the strategy to use when the Storage is removed. There are 2 options:
  - *unset*: the database remains untouched (this is the default behaviour)
  - `"destroy_db"`: the database is destroyed (i.e. removed)

-------------------------------
## **Behaviour of the backend**

### Mapping to RocksDB database
Each **storage** will map to a RocksDB database stored in directory: `${ZBACKEND_ROCKSDB_ROOT}/<dir>`, where:
  * `${ZBACKEND_ROCKSDB_ROOT}` is an environment variable that could be specified before zenoh router startup.
     If this variable is not specified `${ZENOH_HOME}/zbackend_rocksdb` will be used
     (where the default value of `${ZENOH_HOME}` is `~/.zenoh`).
  * `<dir>` is the `"dir"` property specified at storage creation.
Each zenoh **path/value** put into the storage will map to 2 **key/values** in the database:
  * For both, the key is the zenoh path, stripped from the `"path_prefix"` property specified at storage creation.
  * In the `"default"` [Column Family](https://github.com/facebook/rocksdb/wiki/Column-Families) the key is
    put with the zenoh encoded value as a value.
  * In the `"data_info"` [Column Family](https://github.com/facebook/rocksdb/wiki/Column-Families) the key is
    put with a value encoded as following:
      - bytes[0]:     the "deleted" flag as a u8 (1=true, 0=false)
      - bytes[1..9]:  the zenoh encoding flag as a u64
      - bytes[9..17]: the timestamp's time as a u64
      - bytes[17..]:  the timestamp's ID

### Behaviour on deletion
On deletion of a path, the corresponding key is removed from the `"default"` Column Family. An entry with the
"deletion" flag set to true and the deletion timestamp is inserted in the `"data-info"` Column Family
(to avoid re-insertion of points with an older timestamp in case of un-ordered messages).  
At regular interval, a task cleans-up the `"data-info"` Column Family from entries with old timestamps and
the "deletion" flag set to true

### Behaviour on GET
On GET operations:
  * if the selector is a path (i.e. not containing any `'*'`): the value and its encoding and timestamp
    for the corresponding key are directly retrieved from the 2 Column Families using `get` RocksDB operation.
  * otherwise: the storage searches for matching keys, leveraging RocksDB's [Prefix Seek](https://github.com/facebook/rocksdb/wiki/Prefix-Seek) if possible to minimize the number of entries to check.


-------------------------------
## How to build it

At first, install [Cargo and Rust](https://doc.rust-lang.org/cargo/getting-started/installation.html). 

:warning: **WARNING** :warning: : As Rust doesn't have a stable ABI, the backend library should be
built with the exact same Rust version than `zenohd`. Otherwise, incompatibilities in memory mapping
of shared types between `zenohd` and the library can lead to a `"SIGSEV"` crash.

To know the Rust version you're `zenohd` has been built with, use the `--version` option.  
Example:
```bash
$ zenohd --version
The zenoh router v0.5.0-beta.5-134-g81e85d7 built with rustc 1.51.0-nightly (2987785df 2020-12-28)
```
Here, `zenohd` has been built with the Rust toolchain version **nightly-2020-12-28**.  
Install and use this toolchain with the following command:

```bash
$ rustup default nightly-2020-12-28
```

And then build the backend with:

```bash
$ cargo build --release --all-targets
```
