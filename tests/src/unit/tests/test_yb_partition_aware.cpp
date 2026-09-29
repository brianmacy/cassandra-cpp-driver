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

// YugabyteDB partition-aware routing: hash, tablet-leader map and policy.

#include <gtest/gtest.h>

#include "batch_request.hpp"
#include "config.hpp"
#include "control_connection.hpp"
#include "execute_request.hpp"
#include "jenkins_hash.hpp"
#include "query_request.hpp"
#include "request_handler.hpp"
#include "round_robin_policy.hpp"
#include "serialization.hpp"
#include "token_map.hpp"
#include "token_map_impl.hpp"
#include "yb_partition_aware_policy.hpp"
#include "yb_partition_map.hpp"

#include <string.h>

using namespace datastax;
using namespace datastax::internal;
using namespace datastax::internal::core;

namespace {

// ---------------------------------------------------------------------------------------------
// Wire-format helpers for building ResultResponses in-process.

class Bytes {
public:
  const String& str() const { return buf_; }

  void i32(int32_t v) {
    char b[4];
    encode_int32(b, v);
    buf_.append(b, 4);
  }
  void u16(uint16_t v) {
    char b[2];
    encode_uint16(b, v);
    buf_.append(b, 2);
  }
  void raw(const String& s) { buf_.append(s); }
  void string(const String& s) { // [string]
    u16(static_cast<uint16_t>(s.size()));
    raw(s);
  }
  void value(const String& s) { // [bytes]
    i32(static_cast<int32_t>(s.size()));
    raw(s);
  }
  void null_value() { i32(-1); }

private:
  String buf_;
};

String hex(const char* h) {
  String out;
  for (size_t i = 0; h[i] && h[i + 1]; i += 2) {
    char byte[3] = { h[i], h[i + 1], 0 };
    out.push_back(static_cast<char>(strtoul(byte, NULL, 16)));
  }
  return out;
}

String be32(int32_t v) {
  char b[4];
  encode_int32(b, v);
  return String(b, 4);
}

String be64(int64_t v) {
  char b[8];
  encode_int64(b, v);
  return String(b, 8);
}

String ipv4(uint8_t a, uint8_t b, uint8_t c, uint8_t d) {
  char out[4] = { static_cast<char>(a), static_cast<char>(b), static_cast<char>(c),
                  static_cast<char>(d) };
  return String(out, 4);
}

uint16_t hash_of(const String& key) { return yb_partition_hash(key.data(), key.size()); }

// A decoded ResultResponse plus the wire bytes it references (it keeps StringRefs into them).
struct OwnedResult {
  String bytes;
  ResultResponse::Ptr result;

  void decode(const String& wire, int protocol_version) {
    bytes = wire;
    result.reset(new ResultResponse());
    Decoder decoder(bytes.data(), bytes.size(), ProtocolVersion(protocol_version));
    ASSERT_TRUE(result->decode(decoder));
  }
};

// ---------------------------------------------------------------------------------------------
// system.partitions rows

struct PartitionRow {
  PartitionRow(const String& ks, const String& table, const String& start, const String& end)
      : ks(ks)
      , table(table)
      , start(start)
      , end(end) {}
  PartitionRow& replica(const String& inet, const String& role) {
    replicas.push_back(std::make_pair(inet, role));
    return *this;
  }
  String ks, table, start, end;
  Vector<std::pair<String, String> > replicas;
};

void build_partitions_result(const Vector<PartitionRow>& rows, OwnedResult* out) {
  Bytes b;
  b.i32(CASS_RESULT_KIND_ROWS);
  b.i32(CASS_RESULT_FLAG_GLOBAL_TABLESPEC);
  b.i32(5);
  b.string("system");
  b.string("partitions");
  b.string("keyspace_name");
  b.u16(CASS_VALUE_TYPE_VARCHAR);
  b.string("table_name");
  b.u16(CASS_VALUE_TYPE_VARCHAR);
  b.string("start_key");
  b.u16(CASS_VALUE_TYPE_BLOB);
  b.string("end_key");
  b.u16(CASS_VALUE_TYPE_BLOB);
  b.string("replica_addresses");
  b.u16(CASS_VALUE_TYPE_MAP);
  b.u16(CASS_VALUE_TYPE_INET);
  b.u16(CASS_VALUE_TYPE_VARCHAR);
  b.i32(static_cast<int32_t>(rows.size()));
  for (size_t i = 0; i < rows.size(); ++i) {
    const PartitionRow& r = rows[i];
    b.value(r.ks);
    b.value(r.table);
    b.value(r.start);
    b.value(r.end);
    Bytes map; // protocol v3+: [int n] then n x ([bytes] key, [bytes] value)
    map.i32(static_cast<int32_t>(r.replicas.size()));
    for (size_t j = 0; j < r.replicas.size(); ++j) {
      map.value(r.replicas[j].first);
      map.value(r.replicas[j].second);
    }
    b.value(map.str());
  }
  out->decode(b.str(), CASS_PROTOCOL_VERSION_V4);
}

// ---------------------------------------------------------------------------------------------
// A prepared statement with partition key columns (bind indices 0 and 1) and one other column.

struct PreparedFixture {
  PreparedFixture(const String& ks, const String& table, CassValueType k0, CassValueType k1) {
    Bytes b;
    b.i32(CASS_RESULT_KIND_PREPARED);
    b.string("yb-test-id");
    // Bind metadata (v4 carries pk indices)
    b.i32(CASS_RESULT_FLAG_GLOBAL_TABLESPEC);
    b.i32(3);
    b.i32(2);
    b.u16(0);
    b.u16(1);
    b.string(ks);
    b.string(table);
    b.string("k0");
    b.u16(k0);
    b.string("k1");
    b.u16(k1);
    b.string("ck");
    b.u16(CASS_VALUE_TYPE_BLOB);
    // Result metadata: none
    b.i32(CASS_RESULT_FLAG_NO_METADATA);
    b.i32(0);
    owned.decode(b.str(), CASS_PROTOCOL_VERSION_V4);
    Metadata metadata;
    prepared.reset(new Prepared(owned.result, PrepareRequest::ConstPtr(new PrepareRequest("q")),
                                metadata.schema_snapshot()));
  }

  OwnedResult owned;
  Prepared::ConstPtr prepared;
};

ExecuteRequest* bind_int_blob(const Prepared::ConstPtr& prepared, int32_t sub, const String& root) {
  ExecuteRequest* execute = new ExecuteRequest(prepared.get());
  EXPECT_EQ(CASS_OK, execute->set(0, static_cast<cass_int32_t>(sub)));
  EXPECT_EQ(CASS_OK, execute->set(1, CassBytes(reinterpret_cast<const cass_byte_t*>(root.data()),
                                               root.size())));
  EXPECT_EQ(CASS_OK, execute->set(2, CassBytes(reinterpret_cast<const cass_byte_t*>("c"), 1)));
  return execute;
}

// ---------------------------------------------------------------------------------------------
// Live-oracle vectors: `SELECT sub, root, partition_hash(sub, root)` against YugabyteDB
// 192.168.6.201:9042 (release_version 3.9-SNAPSHOT), table ((sub int, root blob), ck blob), and
// `SELECT b, t, partition_hash(b, t)` for ((b bigint, t text), ck int). partition_hash() is the
// server's own YBPartition::HashColumnCompoundValue, so it is the oracle for the client hash.

struct IntBlobVector {
  int32_t sub;
  const char* root_hex;
  uint16_t code;
};

const IntBlobVector INT_BLOB_VECTORS[] = {
  { 7, "a94a8fe5ccb19ba61c4c0873d391e987982fbbd3", 25484 },
  { 0, "00", 38972 },
  { 1, "0102030405", 43708 },
  { 2147483647, "ff", 48239 },
  { -2147483647 - 1, "6b6579", 52240 },
  { -1, "deadbeef", 59218 },
  { 123456, "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f", 61389 },
};

struct BigintTextVector {
  int64_t b;
  const char* t;
  uint16_t code;
};

const BigintTextVector BIGINT_TEXT_VECTORS[] = {
  { -5, "senzing", 4373 },
  { 9223372036854775807LL, "kv", 6310 },
  { 0, "a", 54981 },
};

} // namespace

// =============================================================================================
// 1. Jenkins + fold

// Ported verbatim from yugabyte/cassandra-cpp-driver gtests/src/unit/tests/test_jenkins.cpp.
TEST(JenkinsUnitTest, TestHash64) {
  const uint64_t seed = 97;

  const uint8_t b1[] = { 0xc7, 0x25, 0x1d, 0x5d, 0x75, 0x3a, 0x4e, 0x46, 0x22,
                         0x29, 0x4d, 0x6c, 0x67, 0x7a, 0xa8, 0x25, 0x71 };

  const uint8_t b2[] = { 0x83, 0x8e, 0x7e, 0xf0, 0x71, 0xef, 0x9b, 0x3e, 0x4a, 0xe6,
                         0x12, 0x60, 0xc0, 0xa1, 0xf9, 0x94, 0x5a, 0x85, 0x9b, 0xb1,
                         0xf6, 0x86, 0x97, 0xe1, 0xab, 0x87, 0xc8, 0xab, 0xc1, 0x28,
                         0xd1, 0x72, 0x73, 0x0b, 0xda, 0x50, 0xe3, 0xe6, 0xf9, 0x42 };

  const uint8_t b3[] = {
    0xad, 0xe3, 0xaa, 0xb7, 0xd2, 0xbc, 0x3a, 0xe6, 0x60, 0xe4, 0xc6, 0xc1, 0x02, 0x0a, 0x3a, 0x50,
    0x66, 0xb2, 0x26, 0x6c, 0x1d, 0x1b, 0x16, 0xb1, 0x1b, 0x51, 0x74, 0x9c, 0xa7, 0xbb, 0xad, 0x46,
    0x25, 0x54, 0xca, 0x30, 0x3a, 0x31, 0xd0, 0x34, 0x56, 0xac, 0xb1, 0xca, 0xaf, 0x7f, 0x5c, 0xf3,
    0x9e, 0x16, 0x94, 0x78, 0x84, 0xca, 0x60, 0x66, 0x27, 0x59, 0xe1, 0x99, 0xb4, 0xc4, 0xbd, 0x50,
    0x48, 0x50, 0xcb, 0xa6, 0x0b, 0xe1, 0x71, 0x31, 0x49, 0x27, 0x11, 0x9e, 0xcc, 0xcd, 0xd8, 0x19,
    0x09, 0xc6, 0xdf, 0x15, 0x64, 0x0d, 0xf7, 0x25, 0x5c, 0x48, 0x19, 0xc7, 0x6b, 0x10, 0x02, 0x7e,
    0x31, 0x54, 0x2a, 0xd8, 0x92, 0xe5, 0xc5, 0xab, 0xe9, 0x3d, 0x57, 0x99, 0x9a, 0x93, 0x4f, 0x48,
    0x3f, 0xfa, 0x73, 0x36, 0x03, 0xe1, 0xbd, 0x27, 0xe5, 0x06, 0x8a, 0x21, 0x33, 0xff, 0x91, 0x80,
    0x36, 0x4d, 0x2d, 0x04, 0xc7, 0x11, 0xcc, 0x2a, 0xc0, 0xa9, 0x17, 0x18, 0x73, 0xff, 0xd5, 0x0e,
    0x0d, 0x8b, 0x6f, 0x8b, 0xba, 0x8c, 0x37, 0x49, 0xb1, 0x31, 0x5b, 0xf4, 0x4d, 0xd7, 0x19, 0x10,
    0x40, 0x6e, 0x61, 0x41, 0xf1, 0x55, 0xaa, 0x44, 0x79, 0x13, 0x57, 0x3b, 0x72, 0xac, 0xfe, 0xce,
    0xf8, 0xd7, 0x07, 0x82, 0x05, 0xef, 0x0f, 0x53, 0x6c, 0xfe, 0x7d, 0x94, 0x48, 0xa5, 0x48, 0x42,
    0x47, 0x70, 0x29, 0xe7, 0x7e, 0x53, 0xca, 0x88, 0x89, 0x8a, 0xec, 0xe5, 0x01, 0x44, 0xf5, 0xc5,
    0xc9, 0x89, 0x6d, 0x6a, 0xf1, 0x26, 0x61, 0xae, 0x30, 0x50, 0x61, 0x68, 0x41, 0xac, 0x82, 0x40,
    0xdb, 0x12, 0x00, 0x68, 0xad, 0x34, 0x52, 0xb2, 0xbb, 0xc5, 0x74, 0xf1, 0x3e, 0x00, 0x98, 0x6e,
    0x1d, 0xc2, 0xd7, 0x7d, 0xc6, 0xc7, 0x10, 0xb2, 0xac, 0xcf, 0x8b, 0x25, 0xd9, 0x7d, 0xd5, 0x20
  };

  EXPECT_EQ(1789751740810280356ull,
            cass::Hash64StringWithSeed(reinterpret_cast<const char*>(b1), sizeof(b1), seed));
  EXPECT_EQ(4001818822847464429ull,
            cass::Hash64StringWithSeed(reinterpret_cast<const char*>(b2), sizeof(b2), seed));
  EXPECT_EQ(15240025333683105143ull,
            cass::Hash64StringWithSeed(reinterpret_cast<const char*>(b3), sizeof(b3), seed));
}

// BE32(sub) || root must reproduce the server's partition_hash(sub, root).
TEST(YbPartitionHashUnitTest, LiveOracleIntBlob) {
  for (size_t i = 0; i < sizeof(INT_BLOB_VECTORS) / sizeof(INT_BLOB_VECTORS[0]); ++i) {
    const IntBlobVector& v = INT_BLOB_VECTORS[i];
    EXPECT_EQ(v.code, hash_of(be32(v.sub) + hex(v.root_hex)))
        << "sub=" << v.sub << " root=0x" << v.root_hex;
  }
}

// BE64(b) || t must reproduce the server's partition_hash(b, t).
TEST(YbPartitionHashUnitTest, LiveOracleBigintText) {
  for (size_t i = 0; i < sizeof(BIGINT_TEXT_VECTORS) / sizeof(BIGINT_TEXT_VECTORS[0]); ++i) {
    const BigintTextVector& v = BIGINT_TEXT_VECTORS[i];
    EXPECT_EQ(v.code, hash_of(be64(v.b) + String(v.t))) << "b=" << v.b << " t=" << v.t;
  }
}

// The same vectors through the request path: bound CQL wire values of a prepared EXECUTE.
TEST(YbPartitionHashUnitTest, LiveOracleThroughExecuteRequest) {
  PreparedFixture int_blob("ks", "kv", CASS_VALUE_TYPE_INT, CASS_VALUE_TYPE_BLOB);
  ASSERT_TRUE(int_blob.prepared->yb_routable());
  EXPECT_EQ("ks.kv", int_blob.prepared->yb_ks_table());
  for (size_t i = 0; i < sizeof(INT_BLOB_VECTORS) / sizeof(INT_BLOB_VECTORS[0]); ++i) {
    const IntBlobVector& v = INT_BLOB_VECTORS[i];
    Request::ConstPtr execute(bind_int_blob(int_blob.prepared, v.sub, hex(v.root_hex)));
    uint16_t code = 0;
    const String* ks_table = NULL;
    ASSERT_TRUE(YbPartitionAwarePolicy::yb_hash_code(execute.get(), &code, &ks_table));
    EXPECT_EQ(v.code, code) << "sub=" << v.sub;
    EXPECT_EQ("ks.kv", *ks_table);
  }

  PreparedFixture bigint_text("ks", "kt", CASS_VALUE_TYPE_BIGINT, CASS_VALUE_TYPE_VARCHAR);
  ASSERT_TRUE(bigint_text.prepared->yb_routable());
  for (size_t i = 0; i < sizeof(BIGINT_TEXT_VECTORS) / sizeof(BIGINT_TEXT_VECTORS[0]); ++i) {
    const BigintTextVector& v = BIGINT_TEXT_VECTORS[i];
    SharedRefPtr<ExecuteRequest> execute(new ExecuteRequest(bigint_text.prepared.get()));
    ASSERT_EQ(CASS_OK, execute->set(0, static_cast<cass_int64_t>(v.b)));
    ASSERT_EQ(CASS_OK, execute->set(1, CassString(v.t, strlen(v.t))));
    ASSERT_EQ(CASS_OK, execute->set(2, CassBytes(reinterpret_cast<const cass_byte_t*>("c"), 1)));
    uint16_t code = 0;
    const String* ks_table = NULL;
    ASSERT_TRUE(YbPartitionAwarePolicy::yb_hash_code(execute.get(), &code, &ks_table));
    EXPECT_EQ(v.code, code) << "b=" << v.b;
  }
}

TEST(YbPartitionHashUnitTest, UnsupportedKeyTypeIsNotRoutable) {
  PreparedFixture uuid_key("ks", "t", CASS_VALUE_TYPE_UUID, CASS_VALUE_TYPE_BLOB);
  EXPECT_FALSE(uuid_key.prepared->yb_routable());
  PreparedFixture double_key("ks", "t", CASS_VALUE_TYPE_INT, CASS_VALUE_TYPE_DOUBLE);
  EXPECT_FALSE(double_key.prepared->yb_routable());
}

// =============================================================================================
// 2. YbPartitionMap::build + leader_for

class YbPartitionMapUnitTest : public testing::Test {
public:
  YbPartitionMapUnitTest()
      : a(new Host(Address("127.0.0.1", 9042)))
      , b(new Host(Address("127.0.0.2", 9042)))
      , c(new Host(Address("127.0.0.3", 9042))) {
    hosts[a->address()] = a;
    hosts[b->address()] = b;
    hosts[c->address()] = c;
  }

  Host::Ptr a, b, c;
  HostMap hosts;
};

TEST_F(YbPartitionMapUnitTest, DecodeKey) {
  uint32_t v = 12345;
  EXPECT_TRUE(YbPartitionMap::decode_key(StringRef(), 0x10000, &v));
  EXPECT_EQ(0x10000u, v);
  EXPECT_TRUE(YbPartitionMap::decode_key(StringRef(hex("0800")), 0, &v));
  EXPECT_EQ(0x0800u, v);
  EXPECT_TRUE(YbPartitionMap::decode_key(StringRef(hex("ffff")), 0, &v));
  EXPECT_EQ(0xffffu, v);
  EXPECT_FALSE(YbPartitionMap::decode_key(StringRef(hex("01")), 0, &v));
  EXPECT_FALSE(YbPartitionMap::decode_key(StringRef(hex("010203")), 0, &v));
}

TEST_F(YbPartitionMapUnitTest, BuildAndLookup) {
  Vector<PartitionRow> rows;
  // Out of order on purpose: build() must sort by start.
  rows.push_back(PartitionRow("ks", "t", hex("4000"), hex("8000"))
                     .replica(ipv4(127, 0, 0, 1), "FOLLOWER")
                     .replica(ipv4(127, 0, 0, 2), "LEADER"));
  rows.push_back(PartitionRow("ks", "t", "", hex("4000"))
                     .replica(ipv4(127, 0, 0, 1), "LEADER")
                     .replica(ipv4(127, 0, 0, 3), "FOLLOWER"));
  // Missing leader (election in progress): skipped.
  rows.push_back(PartitionRow("ks", "t", hex("8000"), hex("c000"))
                     .replica(ipv4(127, 0, 0, 1), "FOLLOWER")
                     .replica(ipv4(127, 0, 0, 2), "FOLLOWER"));
  // Leader inet with no Host (topology race): skipped.
  rows.push_back(PartitionRow("ks", "t", hex("c000"), "")
                     .replica(ipv4(10, 9, 9, 9), "LEADER")
                     .replica(ipv4(127, 0, 0, 3), "FOLLOWER"));
  // Malformed bound: skipped.
  rows.push_back(
      PartitionRow("ks", "bad", hex("000000"), "").replica(ipv4(127, 0, 0, 1), "LEADER"));
  // Single-tablet table covering the whole hash space.
  rows.push_back(PartitionRow("ks", "one", "", "").replica(ipv4(127, 0, 0, 3), "LEADER"));

  OwnedResult owned;
  build_partitions_result(rows, &owned);
  YbPartitionMap::ConstPtr map(YbPartitionMap::build(owned.result.get(), hosts, 9042));

  EXPECT_EQ(2u, map->table_count());
  EXPECT_EQ(3u, map->tablet_count());

  const String t("ks.t");
  ASSERT_TRUE(map->leader_for(t, 0) != NULL);
  EXPECT_EQ(a, *map->leader_for(t, 0));
  EXPECT_EQ(a, *map->leader_for(t, 0x3fff));
  EXPECT_EQ(b, *map->leader_for(t, 0x4000)); // start boundary belongs to the next tablet
  EXPECT_EQ(b, *map->leader_for(t, 0x7fff));
  EXPECT_TRUE(map->leader_for(t, 0x8000) == NULL); // missing leader
  EXPECT_TRUE(map->leader_for(t, 0xbfff) == NULL);
  EXPECT_TRUE(map->leader_for(t, 0xc000) == NULL); // unknown inet
  EXPECT_TRUE(map->leader_for(t, 0xffff) == NULL);

  const String one("ks.one");
  EXPECT_EQ(c, *map->leader_for(one, 0));
  EXPECT_EQ(c, *map->leader_for(one, 0xffff));

  EXPECT_TRUE(map->leader_for("ks.bad", 0) == NULL);
  EXPECT_TRUE(map->leader_for("ks.missing", 0) == NULL);
}

TEST_F(YbPartitionMapUnitTest, BuilderGapAndNullResult) {
  YbPartitionMap::Builder builder;
  builder.add("ks.t", 0x1000, 0x2000, b);
  YbPartitionMap::ConstPtr map(builder.finish());
  EXPECT_TRUE(map->leader_for("ks.t", 0x0fff) == NULL); // before the first tablet
  EXPECT_EQ(b, *map->leader_for("ks.t", 0x1000));
  EXPECT_EQ(b, *map->leader_for("ks.t", 0x1fff));
  EXPECT_TRUE(map->leader_for("ks.t", 0x2000) == NULL); // past the last tablet's end

  YbPartitionMap::ConstPtr empty(YbPartitionMap::build(NULL, hosts, 9042));
  EXPECT_EQ(0u, empty->table_count());
}

// =============================================================================================
// 3. Policy

class YbPartitionAwarePolicyUnitTest : public YbPartitionMapUnitTest {
public:
  YbPartitionAwarePolicyUnitTest()
      : fixture("ks", "kv", CASS_VALUE_TYPE_INT, CASS_VALUE_TYPE_BLOB)
      , token_map(TokenMap::from_partitioner(Murmur3Partitioner::name())) {
    // [0, 0x8000) -> b, [0x8000, 0x10000) -> c
    YbPartitionMap::Builder builder;
    builder.add("ks.kv", 0, 0x8000, b);
    builder.add("ks.kv", 0x8000, 0x10000, c);
    token_map->set_yb_partitions(builder.finish());
  }

  static Vector<Address> drain(QueryPlan* plan) {
    ScopedPtr<QueryPlan> owned(plan);
    Vector<Address> out;
    Address address;
    while (owned->compute_next(&address))
      out.push_back(address);
    return out;
  }

  Vector<Address> plan_for(YbPartitionAwarePolicy& policy, const Request::ConstPtr& request,
                           const TokenMap* map) {
    SharedRefPtr<RequestHandler> handler(new RequestHandler(request, ResponseFuture::Ptr()));
    return drain(policy.new_query_plan("ks", handler.get(), map));
  }

  // The plan the wrapped policy alone yields for the same call sequence (fallback == this).
  static Vector<Address> child_plan(RoundRobinPolicy& reference) {
    return drain(reference.new_query_plan("ks", NULL, NULL));
  }

  PreparedFixture fixture;
  TokenMap::Ptr token_map;
};

// sub=7 root=a94a.. hashes to 25484 (< 0x8000) -> b; sub=0 root=00 hashes to 38972 -> c.
TEST_F(YbPartitionAwarePolicyUnitTest, RoutesToLeaderFirst) {
  YbPartitionAwarePolicy policy(new RoundRobinPolicy());
  policy.init(a, hosts, NULL, "");

  Request::ConstPtr to_b(
      bind_int_blob(fixture.prepared, 7, hex("a94a8fe5ccb19ba61c4c0873d391e987982fbbd3")));
  Vector<Address> plan = plan_for(policy, to_b, token_map.get());
  ASSERT_EQ(3u, plan.size());
  EXPECT_EQ(b->address(), plan[0]);
  EXPECT_NE(b->address(), plan[1]); // leader is not repeated
  EXPECT_NE(b->address(), plan[2]);

  Request::ConstPtr to_c(bind_int_blob(fixture.prepared, 0, hex("00")));
  plan = plan_for(policy, to_c, token_map.get());
  ASSERT_EQ(3u, plan.size());
  EXPECT_EQ(c->address(), plan[0]);
  EXPECT_NE(c->address(), plan[1]);
  EXPECT_NE(c->address(), plan[2]);
}

TEST_F(YbPartitionAwarePolicyUnitTest, RoutesBatchByFirstRoutableStatement) {
  YbPartitionAwarePolicy policy(new RoundRobinPolicy());
  policy.init(a, hosts, NULL, "");

  SharedRefPtr<BatchRequest> batch(new BatchRequest(CASS_BATCH_TYPE_LOGGED));
  batch->add_statement(new QueryRequest("SELECT 1", 0)); // not routable
  batch->add_statement(bind_int_blob(fixture.prepared, 0, hex("00")));
  Vector<Address> plan = plan_for(policy, batch, token_map.get());
  ASSERT_EQ(3u, plan.size());
  EXPECT_EQ(c->address(), plan[0]);
}

TEST_F(YbPartitionAwarePolicyUnitTest, FallsBackToChildPlan) {
  YbPartitionAwarePolicy policy(new RoundRobinPolicy());
  RoundRobinPolicy reference;
  policy.init(a, hosts, NULL, "");
  reference.init(a, hosts, NULL, "");

  Request::ConstPtr routable(
      bind_int_blob(fixture.prepared, 7, hex("a94a8fe5ccb19ba61c4c0873d391e987982fbbd3")));

  // No token map at all.
  EXPECT_EQ(child_plan(reference), plan_for(policy, routable, NULL));

  // A token map without a YB partition map (routing never refreshed / not YugabyteDB).
  TokenMap::Ptr plain(TokenMap::from_partitioner(Murmur3Partitioner::name()));
  EXPECT_EQ(child_plan(reference), plan_for(policy, routable, plain.get()));

  // No hash: a simple query.
  EXPECT_EQ(child_plan(reference),
            plan_for(policy, Request::ConstPtr(new QueryRequest("SELECT 1", 0)), token_map.get()));

  // No hash: a partition-key value is unset, or NULL.
  SharedRefPtr<ExecuteRequest> unset(new ExecuteRequest(fixture.prepared.get()));
  ASSERT_EQ(CASS_OK, unset->set(0, static_cast<cass_int32_t>(7)));
  EXPECT_EQ(child_plan(reference), plan_for(policy, unset, token_map.get()));
  SharedRefPtr<ExecuteRequest> null_key(new ExecuteRequest(fixture.prepared.get()));
  ASSERT_EQ(CASS_OK, null_key->set(0, static_cast<cass_int32_t>(7)));
  ASSERT_EQ(CASS_OK, null_key->set(1, CassNull()));
  EXPECT_EQ(child_plan(reference), plan_for(policy, null_key, token_map.get()));

  // Table not in the map (created after the last refresh).
  PreparedFixture other("ks", "not_in_map", CASS_VALUE_TYPE_INT, CASS_VALUE_TYPE_BLOB);
  EXPECT_EQ(child_plan(reference),
            plan_for(policy, Request::ConstPtr(bind_int_blob(other.prepared, 7, hex("00"))),
                     token_map.get()));
}

TEST_F(YbPartitionAwarePolicyUnitTest, SkipsDownLeaderAndResumesWhenUp) {
  YbPartitionAwarePolicy policy(new RoundRobinPolicy());
  RoundRobinPolicy reference;
  policy.init(a, hosts, NULL, "");
  reference.init(a, hosts, NULL, "");

  Request::ConstPtr to_b(
      bind_int_blob(fixture.prepared, 7, hex("a94a8fe5ccb19ba61c4c0873d391e987982fbbd3")));

  policy.on_host_down(b->address());
  reference.on_host_down(b->address());
  Vector<Address> plan = plan_for(policy, to_b, token_map.get());
  EXPECT_EQ(child_plan(reference), plan);
  for (size_t i = 0; i < plan.size(); ++i)
    EXPECT_NE(b->address(), plan[i]);

  policy.on_host_up(b);
  reference.on_host_up(b);
  plan = plan_for(policy, to_b, token_map.get());
  ASSERT_FALSE(plan.empty());
  EXPECT_EQ(b->address(), plan[0]);

  // A removed leader is not routed to either.
  policy.on_host_removed(b);
  plan = plan_for(policy, to_b, token_map.get());
  for (size_t i = 0; i < plan.size(); ++i)
    EXPECT_NE(b->address(), plan[i]);
}

TEST_F(YbPartitionAwarePolicyUnitTest, TokenMapCopyCarriesPartitionMap) {
  TokenMap::Ptr copy(token_map->copy());
  ASSERT_TRUE(copy->yb_partitions().get() != NULL);
  EXPECT_EQ(token_map->yb_partitions().get(), copy->yb_partitions().get());
}

TEST(YbPartitionAwareConfigUnitTest, DefaultOffAndKnobs) {
  // Default OFF, and a default Config's control-connection settings do not query partitions.
  Config config;
  EXPECT_FALSE(config.yb_partition_aware_routing());
  EXPECT_EQ(60u, config.yb_partitions_refresh_interval_secs());
  EXPECT_FALSE(ControlConnectionSettings(config).use_yb_partition_aware_routing);

  CassCluster* cluster = cass_cluster_new();
  EXPECT_EQ(CASS_ERROR_LIB_BAD_PARAMS,
            cass_cluster_set_yb_partition_aware_routing_refresh_interval(cluster, 0));
  EXPECT_EQ(CASS_OK, cass_cluster_set_yb_partition_aware_routing_refresh_interval(cluster, 5));
  cass_cluster_free(cluster);

  ExecutionProfile off;
  off.set_load_balancing_policy(new RoundRobinPolicy());
  off.build_load_balancing_policy();
  EXPECT_TRUE(dynamic_cast<YbPartitionAwarePolicy*>(off.load_balancing_policy().get()) == NULL);

  ExecutionProfile on;
  on.set_load_balancing_policy(new RoundRobinPolicy());
  on.set_yb_partition_aware_routing(true);
  on.build_load_balancing_policy();
  EXPECT_TRUE(dynamic_cast<YbPartitionAwarePolicy*>(on.load_balancing_policy().get()) != NULL);
}
