<img src="https://raw.githubusercontent.com/eclipse-zenoh/zenoh/main/zenoh-dragon.png" height="150">

[![CI](https://github.com/eclipse-zenoh/zenoh-backend-rocksdb/workflows/CI/badge.svg)](https://github.com/eclipse-zenoh/zenoh-backend-rocksdb/actions?query=workflow%3A%22CI%22)
[![Discussion](https://img.shields.io/badge/discussion-on%20github-blue)](https://github.com/eclipse-zenoh/roadmap/discussions)
[![Discord](https://img.shields.io/badge/chat-on%20discord-blue)](https://discord.gg/2GJ958VuHs)
[![License](https://img.shields.io/badge/License-EPL%202.0-blue)](https://choosealicense.com/licenses/epl-2.0/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

# Eclipse Zenoh
The Eclipse Zenoh: Zero Overhead Pub/sub, Store/Query and Compute.

Zenoh (pronounce _/zeno/_) unifies data in motion, data at rest and computations. It carefully blends traditional pub/sub with geo-distributed storages, queries and computations, while retaining a level of time and space efficiency that is well beyond any of the mainstream stacks.

Check the website [zenoh.io](http://zenoh.io) and the [roadmap](https://github.com/eclipse-zenoh/roadmap) for more detailed information.

-------------------------------
# RocksDB backend

In zenoh a backend is a storage technology (such as DBMS, time-series database, file system...) alowing to store the
keys/values publications made via zenoh and return them on queries.
See the [zenoh documentation](http://zenoh.io/docs/manual/backends/) for more details.

This backend relies on [RocksDB](https://rocksdb.org/) to implement the storages.
Its library name (without OS specific prefix and extension) that zenoh will rely on to find it and load it is **`zenoh_backend_rocksdb`**.

:point_right: **Install latest release:** see [below](#How-to-install-it)

:point_right: **Build "main" branch:** see [below](#How-to-build-it)

-------------------------------
## **Examples of usage**

Prerequisites:
 - You have a zenoh router (`zenohd`) installed, and the `zenoh_backend_rocksdb` library file is available in `~/.zenoh/lib`.
 - Declare the `ZENOH_BACKEND_ROCKSDB_ROOT` environment variable to the directory where you want the RocksDB databases
   to be stored. If you don't declare it, the `~/.zenoh/zenoh_backend_rocksdb` directory will be used.

You can setup storages either at zenoh router startup via a configuration file, either at runtime via the zenoh admin space, using for instance the REST API.

### **Setup via a JSON5 configuration file**

  - Create a `zenoh.json5` configuration file containing:
    ```json5
    {
      plugins: {
        // configuration of "storages" plugin:
        storage_manager: {
          volumes: {
            // configuration of a "rocksdb" volume (the "zenoh_backend_rocksdb" backend library will be loaded at startup)
            rocksdb: {}
          },
          storages: {
            // configuration of a "demo" storage using the "rocksdb" volume
            demo: {
              // the key expression this storage will subscribes to
              key_expr: "demo/example/**",
              // this prefix will be stripped from the received key when converting to database key.
              // i.e.: "demo/example/a/b" will be stored as "a/b"
              strip_prefix: "demo/example",
              volume: {
                id: "rocksdb",
                // the RocksDB database will be stored in this directory (relative to ${ZENOH_BACKEND_ROCKSDB_ROOT})
                dir: "example",
                // create the RocksDB database if not already existing
                create_db: true
              }
            }
          }
        },
        // Optionally, add the REST plugin
        rest: { http_port: 8000 }
      }
    }
    ```
  - Run the zenoh router with:  
    `zenohd -c zenoh.json5`

### **Setup at runtime via `curl` commands on the admin space**

  - Run the zenoh router, with write permissions to its admin space and with the REST plugin:  
    `zenohd --adminspace-permissions=rw --rest-http-port=8000`
  - Add the "rocksdb" backend (the "zenoh_backend_rocksdb" library will be loaded):  
   `curl -X PUT -H 'content-type:application/json' -d '{}' http://localhost:8000/@/router/local/config/plugins/storage_manager/volumes/rocksdb`
  - Add the "demo" storage using the "rocksdb" backend:  
   `curl -X PUT -H 'content-type:application/json' -d '{key_expr:"demo/example/**",strip_prefix:"demo/example",volume: {id: "rocksdb",dir: "example",create_db: true}}' http://localhost:8000/@/router/local/config/plugins/storage_manager/storages/demo`

### **Tests using the REST API**

Using `curl` to publish and query keys/values, you can:
```bash
# Put values that will be stored in the RocksDB database
curl -X PUT -d "TEST-1" http://localhost:8000/demo/example/test-1
curl -X PUT -d "B" http://localhost:8000/demo/example/a/b

# Retrive the values
curl http://localhost:8000/demo/example/**
```

-------------------------------
## Volume-specific storage configuration
Storages relying on a RocksDB-backed volume must specify some additional configuration as shown [above](#setup-via-a-json5-configuration-file):
- **`"dir"`** (**required**, string) : The name of directory where the RocksDB database is stored.
  The absolute path will be `${ZENOH_BACKEND_ROCKSDB_ROOT}/<dir>`.

- **`"create_db"`** (optional, boolean) : create the RocksDB database if not already existing. Not set by default.
  *(the value doesn't matter, only the property existence is checked)*

- **`"read_only"`** (optional, boolean) : the storage will only answer to GET queries. It will not accept any PUT or DELETE message, and won't put anything in RocksDB database. Not set by default. *(the value doesn't matter, only the property existence is checked)*

- **`"on_closure"`** (optional, string) : the strategy to use when the Storage is removed. There are 2 options:
  - *unset*: the database remains untouched (this is the default behaviour)
  - `"destroy_db"`: the database is destroyed (i.e. removed)

-------------------------------
## **Behaviour of the backend**

### Mapping to RocksDB database
Each **storage** will map to a RocksDB database stored in directory: `${ZENOH_BACKEND_ROCKSDB_ROOT}/<dir>`, where:
  * `${ZENOH_BACKEND_ROCKSDB_ROOT}` is an environment variable that could be specified before zenoh router startup.
     If this variable is not specified `${ZENOH_HOME}/zenoh_backend_rocksdb` will be used
     (where the default value of `${ZENOH_HOME}` is `~/.zenoh`).
  * `<dir>` is the `"dir"` property specified at storage creation.
Each zenoh **key/value** put into the storage will map to 2 **key/values** in the database:
  * For both, the database key is the zenoh key, stripped from the `"strip_prefix"` property specified at storage creation.
  * In the `"default"` [Column Family](https://github.com/facebook/rocksdb/wiki/Column-Families) the key is
    put with the zenoh encoded value as a value.
  * In the `"data_info"` [Column Family](https://github.com/facebook/rocksdb/wiki/Column-Families) the key is
    put with a bytes buffer encoded in this order:
      - the Timestamp encoded as: 8 bytes for the time + 16 bytes for the HLC ID
      - a "is deleted" flag encoded as a boolean on 1 byte
      - the encoding prefix flag encoded as a ZInt (variable length)
      - the encoding suffix encoded as a String (string length as a ZInt + string bytes without ending `\0`)

### Behaviour on deletion
On deletion of a key, the corresponding key is removed from the `"default"` Column Family. An entry with the
"deletion" flag set to true and the deletion timestamp is inserted in the `"data-info"` Column Family
(to avoid re-insertion of points with an older timestamp in case of un-ordered messages).  
At regular interval, a task cleans-up the `"data-info"` Column Family from entries with old timestamps and
the "deletion" flag set to true

### Behaviour on GET
On GET operations:
  * if the selector is a unique key (i.e. not containing any `'*'`): the value and its encoding and timestamp
    for the corresponding key are directly retrieved from the 2 Column Families using `get` RocksDB operation.
  * if the selector is a key expression: the storage searches for matching keys, leveraging RocksDB's [Prefix Seek](https://github.com/facebook/rocksdb/wiki/Prefix-Seek) if possible to minimize the number of entries to check.

-------------------------------
## How to install it

To install the latest release of this backend library, you can do as follows:

### Manual installation (all platforms)

All release packages can be downloaded from:  
 - https://download.eclipse.org/zenoh/zenoh-backend-rocksdb/latest/   

Each subdirectory has the name of the Rust target. See the platforms each target corresponds to on https://doc.rust-lang.org/stable/rustc/platform-support.html

Choose your platform and download the `.zip` file.  
Unzip it in the same directory than `zenohd` or to any directory where it can find the backend library (e.g. /usr/lib or ~/.zenoh/lib)

### Linux Debian

Add Eclipse Zenoh private repository to the sources list, and install the `zenoh-backend-rocksdb` package:

```bash
echo "deb [trusted=yes] https://download.eclipse.org/zenoh/debian-repo/ /" | sudo tee -a /etc/apt/sources.list.d/zenoh.list > /dev/null
sudo apt update
sudo apt install zenoh-backend-rocksdb
```

-------------------------------
## How to build it

> :warning: **WARNING** :warning: : Zenoh and its ecosystem are under active development. When you build from git, make sure you also build from git any other Zenoh repository you plan to use (e.g. binding, plugin, backend, etc.). It may happen that some changes in git are not compatible with the most recent packaged Zenoh release (e.g. deb, docker, pip). We put particular effort in mantaining compatibility between the various git repositories in the Zenoh project.

At first, install [Clang](https://clang.llvm.org/) and [Cargo and Rust](https://doc.rust-lang.org/cargo/getting-started/installation.html). If you already have the Rust toolchain installed, make sure it is up-to-date with:

```bash
$ rustup update
``` 

> :warning: **WARNING** :warning: : As Rust doesn't have a stable ABI, the backend library should be
built with the exact same Rust version than `zenohd`, and using for `zenoh` dependency the same version (or commit number) than 'zenohd'.
Otherwise, incompatibilities in memory mapping of shared types between `zenohd` and the library can lead to a `"SIGSEV"` crash.

To know the Rust version you're `zenohd` has been built with, use the `--version` option.  
Example:
```bash
$ zenohd --version
The zenoh router v0.6.0-beta.1 built with rustc 1.64.0 (a55dd71d5 2022-09-19)
```
Here, `zenohd` has been built with the rustc version `1.64.0`.  
Install and use this toolchain with the following command:

```bash
$ rustup default 1.64.0
```

And `zenohd` version corresponds to an un-released commit with id `1f20c86`. Update the `zenoh` dependency in Cargo.lock with this command:
```bash
$ cargo update -p zenoh --precise 1f20c86
```

Then build the backend with:

```bash
$ cargo build --release --all-targets
```
