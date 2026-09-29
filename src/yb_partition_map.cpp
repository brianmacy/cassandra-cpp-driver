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

#include "yb_partition_map.hpp"

#include "jenkins_hash.hpp"
#include "logger.hpp"
#include "map_iterator.hpp"
#include "result_iterator.hpp"
#include "result_response.hpp"
#include "row.hpp"

#include <algorithm>

using namespace datastax;
using namespace datastax::internal;
using namespace datastax::internal::core;

namespace {

// YBPartition::HashColumnCompoundValue() seeds the Jenkins hash with 97.
const uint64_t YB_HASH_SEED = 97;

// Tablet range bound for an empty end_key: the open end of the 16-bit hash space.
const uint32_t YB_HASH_SPACE_END = 0x10000;

struct TabletStartLess {
  bool operator()(const YbTabletRange& lhs, const YbTabletRange& rhs) const {
    return lhs.start < rhs.start;
  }
};

struct CodeBeforeTabletStart {
  bool operator()(uint32_t code, const YbTabletRange& range) const { return code < range.start; }
};

Host::Ptr resolve_host(const Address& address, const HostMap& hosts) {
  HostMap::const_iterator it = hosts.find(address);
  if (it != hosts.end()) return it->second;
  // Fallback for address translation / SNI: match the host's rpc_address instead.
  for (HostMap::const_iterator i = hosts.begin(), end = hosts.end(); i != end; ++i) {
    if (i->second->rpc_address().equals(address, false)) return i->second;
  }
  return Host::Ptr();
}

} // namespace

uint16_t datastax::internal::core::yb_partition_hash(const char* key_bytes, size_t length) {
  const uint64_t h =
      cass::Hash64StringWithSeed(key_bytes, static_cast<uint32_t>(length), YB_HASH_SEED);
  const uint64_t h1 = h >> 48;
  const uint64_t h2 = 3 * (h >> 32);
  const uint64_t h3 = 5 * (h >> 16);
  const uint64_t h4 = 7 * (h & 0xffff);
  return static_cast<uint16_t>((h1 ^ h2 ^ h3 ^ h4) & 0xffff);
}

bool datastax::internal::core::yb_hash_type_supported(CassValueType type) {
  switch (type) {
    case CASS_VALUE_TYPE_INT:
    case CASS_VALUE_TYPE_BIGINT:
    case CASS_VALUE_TYPE_TEXT:
    case CASS_VALUE_TYPE_VARCHAR:
    case CASS_VALUE_TYPE_BLOB:
      return true;
    default:
      return false;
  }
}

YbPartitionMap::YbPartitionMap()
    : tablet_count_(0) {
  tables_.set_empty_key(String());
}

YbPartitionMap::Builder::Builder()
    : map_(new YbPartitionMap()) {}

void YbPartitionMap::Builder::add(const String& ks_table, uint32_t start, uint32_t end,
                                  const Host::Ptr& leader) {
  map_->tables_[ks_table].push_back(YbTabletRange(start, end, leader));
  ++map_->tablet_count_;
}

YbPartitionMap::ConstPtr YbPartitionMap::Builder::finish() {
  for (TableMap::iterator it = map_->tables_.begin(), end = map_->tables_.end(); it != end; ++it) {
    std::sort(it->second.begin(), it->second.end(), TabletStartLess());
  }
  ConstPtr result(map_);
  map_.reset(new YbPartitionMap());
  return result;
}

bool YbPartitionMap::decode_key(StringRef blob, uint32_t empty_value, uint32_t* output) {
  if (blob.empty()) {
    *output = empty_value;
    return true;
  }
  if (blob.size() != 2) return false;
  const unsigned char* bytes = reinterpret_cast<const unsigned char*>(blob.data());
  *output = (static_cast<uint32_t>(bytes[0]) << 8) | static_cast<uint32_t>(bytes[1]);
  return true;
}

YbPartitionMap::ConstPtr YbPartitionMap::build(const ResultResponse* partitions,
                                               const HostMap& hosts, int port) {
  Builder builder;
  if (partitions == NULL) return builder.finish();

  size_t skipped = 0;
  ResultIterator rows(partitions);
  while (rows.next()) {
    const Row* row = rows.row();
    const Value* keyspace_name = row->get_by_name("keyspace_name");
    const Value* table_name = row->get_by_name("table_name");
    const Value* start_key = row->get_by_name("start_key");
    const Value* end_key = row->get_by_name("end_key");
    const Value* replicas = row->get_by_name("replica_addresses");
    if (keyspace_name == NULL || table_name == NULL || start_key == NULL || end_key == NULL ||
        replicas == NULL || keyspace_name->is_null() || table_name->is_null() ||
        replicas->is_null() || !replicas->is_map()) {
      ++skipped;
      continue;
    }

    uint32_t start = 0;
    uint32_t end = 0;
    if (!decode_key(start_key->to_string_ref(), 0, &start) ||
        !decode_key(end_key->to_string_ref(), YB_HASH_SPACE_END, &end) || start >= end) {
      ++skipped;
      continue;
    }

    Host::Ptr leader;
    MapIterator iterator(replicas);
    while (iterator.next()) {
      if (iterator.value()->to_string_ref() != "LEADER") continue;
      const Value* inet = iterator.key();
      Address address;
      if (inet->decoder().as_inet(inet->size(), port, &address)) {
        leader = resolve_host(address, hosts);
      }
      break;
    }
    if (!leader) {
      ++skipped;
      continue;
    }

    String ks_table(keyspace_name->to_string());
    ks_table.push_back('.');
    ks_table.append(table_name->to_string());
    builder.add(ks_table, start, end, leader);
  }

  ConstPtr map(builder.finish());
  LOG_DEBUG("Built YB partition map: %u tables, %u tablets (%u rows skipped: no resolvable leader "
            "or malformed bounds)",
            static_cast<unsigned int>(map->table_count()),
            static_cast<unsigned int>(map->tablet_count()), static_cast<unsigned int>(skipped));
  return map;
}

const Host::Ptr* YbPartitionMap::leader_for(const String& ks_table, uint16_t code) const {
  TableMap::const_iterator table = tables_.find(ks_table);
  if (table == tables_.end()) return NULL;
  const TabletVec& tablets = table->second;
  // The last tablet whose start <= code.
  TabletVec::const_iterator it = std::upper_bound(
      tablets.begin(), tablets.end(), static_cast<uint32_t>(code), CodeBeforeTabletStart());
  if (it == tablets.begin()) return NULL;
  --it;
  if (static_cast<uint32_t>(code) >= it->end || !it->leader) return NULL;
  return &it->leader;
}
