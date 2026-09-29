/*
  Copyright (c) Senzing, Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

#ifndef DATASTAX_INTERNAL_YB_PARTITION_MAP_HPP
#define DATASTAX_INTERNAL_YB_PARTITION_MAP_HPP

#include "cassandra.h"
#include "dense_hash_map.hpp"
#include "host.hpp"
#include "ref_counted.hpp"
#include "string.hpp"
#include "string_ref.hpp"
#include "vector.hpp"

#include <stdint.h>

// The query that feeds YbPartitionMap::build(). YugabyteDB only (the table does not exist on
// Apache Cassandra); it is only ever issued when YB partition-aware routing is enabled.
#define SELECT_YB_PARTITIONS \
  "SELECT keyspace_name, table_name, start_key, end_key, replica_addresses FROM system.partitions"

namespace datastax { namespace internal { namespace core {

class ResultResponse;

/**
 * The 16-bit YugabyteDB partition hash of an already-encoded hash key, i.e.
 * YBPartition::HashColumnCompoundValue(): Jenkins Hash64StringWithSeed(key, len, 97) folded to
 * 16 bits as ((h >> 48) ^ 3 * (h >> 32) ^ 5 * (h >> 16) ^ 7 * (h & 0xffff)) & 0xffff.
 *
 * The key is the concatenation, in partition-key column order, of each hash column's encoded
 * value with no framing. For the column types accepted by yb_hash_type_supported() that encoding
 * is byte-identical to the CQL wire value (INT: 4 bytes big-endian, BIGINT: 8 bytes big-endian,
 * TEXT/VARCHAR/BLOB: the raw bytes).
 */
uint16_t yb_partition_hash(const char* key_bytes, size_t length);

/**
 * True when a partition-key column of this type hashes, on the server, over exactly its CQL wire
 * bytes. Only these types are routed; anything else falls back to the child policy's plan.
 * Verified against a live YugabyteDB `partition_hash()`: INT, BIGINT, TEXT/VARCHAR, BLOB.
 */
bool yb_hash_type_supported(CassValueType type);

/**
 * One tablet: the hash range [start, end) and its current leader. `end` is 0x10000 for the last
 * tablet (an empty end_key in system.partitions).
 */
struct YbTabletRange {
  YbTabletRange()
      : start(0)
      , end(0) {}
  YbTabletRange(uint32_t start, uint32_t end, const Host::Ptr& leader)
      : start(start)
      , end(end)
      , leader(leader) {}

  uint32_t start;
  uint32_t end;
  Host::Ptr leader;
};

/**
 * An immutable snapshot of YugabyteDB tablet leaders, keyed by "keyspace.table". It is built on
 * the cluster's event loop from system.partitions and handed to every request processor inside
 * its TokenMap snapshot, so lookups on the request path take no lock. A new snapshot replaces the
 * old one; an existing snapshot is never modified.
 */
class YbPartitionMap : public RefCounted<YbPartitionMap> {
public:
  typedef SharedRefPtr<const YbPartitionMap> ConstPtr;
  typedef Vector<YbTabletRange> TabletVec;

  /**
   * Incremental construction (used by build() and by tests). Tablets may be added in any order;
   * finish() sorts each table's tablets by start.
   */
  class Builder {
  public:
    Builder();
    void add(const String& ks_table, uint32_t start, uint32_t end, const Host::Ptr& leader);
    ConstPtr finish();

  private:
    SharedRefPtr<YbPartitionMap> map_;
  };

  /**
   * Build from a system.partitions result. Each replica inet is resolved to a Host ONCE, here,
   * against `hosts` (by address with `port`, then by rpc_address). A tablet whose leader is
   * missing, or whose leader inet has no Host (topology race), or whose key bounds are malformed,
   * is skipped: lookups in its range miss and the request falls back to the child plan.
   */
  static ConstPtr build(const ResultResponse* partitions, const HostMap& hosts, int port);

  /**
   * Decode a system.partitions start_key/end_key blob: empty -> `empty_value`, 2 bytes ->
   * big-endian uint16. Returns false for any other length.
   */
  static bool decode_key(StringRef blob, uint32_t empty_value, uint32_t* output);

  /**
   * The leader for `code` in table `ks_table`, or NULL when the table is unknown or no tablet
   * with a resolved leader covers the code. O(log tablets); no allocation, no lock.
   */
  const Host::Ptr* leader_for(const String& ks_table, uint16_t code) const;

  size_t table_count() const { return tables_.size(); }
  size_t tablet_count() const { return tablet_count_; }

private:
  YbPartitionMap();

  typedef DenseHashMap<String, TabletVec> TableMap;
  TableMap tables_;
  size_t tablet_count_;
};

}}} // namespace datastax::internal::core

#endif
