# Senzing fork of the DataStax C/C++ driver

Branch `sz-2.17.1` = upstream DataStax cpp-driver **2.17.1** plus the commits below. Senzing's G2 builds this
driver statically into its YugabyteDB (YCQL) store-port plugin (`libszstoreport_ycql`) from a **pinned commit
archive** (sha256-verified); see `dev/scripts/setup_ycql_client.sh` in G2. CI and developer builds use the same pin.

## Changes vs upstream 2.17.1

| commit | change | why |
|---|---|---|
| `7174d9af` | **CASSCPP-3 backport** (upstream trunk `58da4b30bb`, unreleased): `ConnectionListener::on_close` is no longer pure virtual; `PrepareHostHandler::on_close` no longer calls `callback_(this)` | "pure virtual function called" crash while a `Session` is destroyed |
| `5013b507` | **`Async::handle_` teardown race**: one `std::mutex` guards `handle_` in `send()` and `close_handle()` | data race / use-after-free at session teardown (found by TSan); not fixed upstream |
| `cda6d8b7` | **YugabyteDB Jenkins hash**, verbatim port of YugabyteDB's own `jenkins_hash` (license header kept) | needed to compute a row's YB partition hash client-side |
| `163a0740` | **YugabyteDB partition-aware routing, default OFF** (see below) | send each statement straight to its tablet leader instead of a proxy hop |
| `4302a62d` | `#define CASS_YB_PARTITION_AWARE_ROUTING 1` in `cassandra.h` | lets callers compile against stock DataStax too |

## Partition-aware routing (`163a0740`)

- **API:** `cass_cluster_set_yb_partition_aware_routing(cluster, cass_true)` and
  `cass_cluster_set_yb_partition_aware_routing_refresh_interval(cluster, secs)` (default 60 s; 0 rejected).
  Off means no partitions query and no extra policy in the chain (stock behaviour).
- **Map:** the cluster reads `system.partitions` on the control connection after connect/reconnect, every refresh
  interval, 1 s after a keyspace/table schema change, and after a host is removed. A failed query keeps the previous
  map and the driver degrades to the normal plan (the server proxies); it never fails the connection.
- **Request path (no lock):** hash the bound partition-key columns of a prepared statement (YB Jenkins fold), look the
  table up in a hash map, binary-search the tablet range in the TokenMap snapshot each processor already holds.
- **Policy:** `YbPartitionAwarePolicy` wraps the whole chain (outside latency-aware) and returns the leader first,
  then the child plan minus the leader. Its own per-processor down-host set decides "leader up" (no rwlock).
- **Limits:** only INT, BIGINT, TEXT/VARCHAR and BLOB partition-key columns are routed (each verified against a live
  `partition_hash()`); anything else uses the child plan. Requires token-aware routing and schema metadata (both on
  by default); a WARN is logged otherwise. Followers are not used.
- **Tests:** 14 unit tests in `tests/src/unit/tests/test_yb_partition_aware.cpp`, including live-oracle hash vectors;
  dropping one term of the hash fold fails 3 of them.
- **Measured in G2 (2026-09-30):** reads served on the leader's node rose to 99.5%. Writes inside a distributed
  transaction still fan out to every tablet the transaction touches, so routing alone did not move G2's load rate.
