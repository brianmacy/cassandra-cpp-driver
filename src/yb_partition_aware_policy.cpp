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

#include "yb_partition_aware_policy.hpp"

#include "batch_request.hpp"
#include "execute_request.hpp"
#include "request_handler.hpp"
#include "token_map.hpp"
#include "yb_partition_map.hpp"

#include <string.h>

using namespace datastax;
using namespace datastax::internal;
using namespace datastax::internal::core;

namespace {

// Hash keys up to this size (the common case: a 4-byte int plus a short blob) are assembled on
// the stack; longer keys use one heap buffer.
const size_t YB_STACK_KEY_SIZE = 256;

bool execute_hash_code(const ExecuteRequest* execute, uint16_t* code, const String** ks_table) {
  const Prepared* prepared = execute->prepared().get();
  if (prepared == NULL || !prepared->yb_routable()) return false;

  const ResultResponse::PKIndexVec& key_indices = prepared->key_indices();
  const AbstractData::ElementVec& elements = execute->elements();
  const size_t header_size = sizeof(int32_t); // Every bound value is [int32 length][bytes]

  size_t total = 0;
  for (ResultResponse::PKIndexVec::const_iterator it = key_indices.begin(), end = key_indices.end();
       it != end; ++it) {
    if (*it >= elements.size()) return false;
    const AbstractData::Element& element = elements[*it];
    if (element.is_unset() || element.is_null()) return false;
    total += element.get_size() - header_size;
  }

  char stack_buffer[YB_STACK_KEY_SIZE];
  String heap_buffer;
  char* key = stack_buffer;
  if (total > sizeof(stack_buffer)) {
    heap_buffer.resize(total);
    key = &heap_buffer[0];
  }

  size_t offset = 0;
  for (ResultResponse::PKIndexVec::const_iterator it = key_indices.begin(), end = key_indices.end();
       it != end; ++it) {
    const Buffer buffer(elements[*it].get_buffer());
    const size_t size = buffer.size() - header_size;
    memcpy(key + offset, buffer.data() + header_size, size);
    offset += size;
  }

  *code = yb_partition_hash(key, offset);
  *ks_table = &prepared->yb_ks_table();
  return true;
}

} // namespace

YbPartitionAwarePolicy::YbPartitionAwarePolicy(LoadBalancingPolicy* child_policy)
    : ChainedLoadBalancingPolicy(child_policy) {}

void YbPartitionAwarePolicy::init(const Host::Ptr& connected_host, const HostMap& hosts,
                                  Random* random, const String& local_dc) {
  down_hosts_.clear();
  ChainedLoadBalancingPolicy::init(connected_host, hosts, random, local_dc);
}

bool YbPartitionAwarePolicy::yb_hash_code(const Request* request, uint16_t* code,
                                          const String** ks_table) {
  if (request == NULL) return false;
  switch (request->opcode()) {
    case CQL_OPCODE_EXECUTE:
      return execute_hash_code(static_cast<const ExecuteRequest*>(request), code, ks_table);
    case CQL_OPCODE_BATCH: {
      const BatchRequest::StatementVec& statements =
          static_cast<const BatchRequest*>(request)->statements();
      for (BatchRequest::StatementVec::const_iterator it = statements.begin(),
                                                      end = statements.end();
           it != end; ++it) {
        if ((*it)->opcode() == CQL_OPCODE_EXECUTE &&
            execute_hash_code(static_cast<const ExecuteRequest*>(it->get()), code, ks_table)) {
          return true;
        }
      }
      return false;
    }
    default:
      return false;
  }
}

QueryPlan* YbPartitionAwarePolicy::new_query_plan(const String& keyspace,
                                                  RequestHandler* request_handler,
                                                  const TokenMap* token_map) {
  QueryPlan* child_plan = child_policy_->new_query_plan(keyspace, request_handler, token_map);
  if (request_handler == NULL || token_map == NULL) return child_plan;

  const YbPartitionMap* partitions = token_map->yb_partitions().get();
  if (partitions == NULL) return child_plan;

  uint16_t code = 0;
  const String* ks_table = NULL;
  if (!yb_hash_code(request_handler->request(), &code, &ks_table)) return child_plan;

  const Host::Ptr* leader = partitions->leader_for(*ks_table, code);
  if (leader == NULL || down_hosts_.count((*leader)->address()) > 0 ||
      child_policy_->distance(*leader) != CASS_HOST_DISTANCE_LOCAL) {
    return child_plan;
  }

  return new YbQueryPlan(*leader, child_plan);
}

void YbPartitionAwarePolicy::on_host_added(const Host::Ptr& host) {
  down_hosts_.erase(host->address());
  ChainedLoadBalancingPolicy::on_host_added(host);
}

void YbPartitionAwarePolicy::on_host_removed(const Host::Ptr& host) {
  down_hosts_.insert(host->address());
  ChainedLoadBalancingPolicy::on_host_removed(host);
}

void YbPartitionAwarePolicy::on_host_up(const Host::Ptr& host) {
  down_hosts_.erase(host->address());
  ChainedLoadBalancingPolicy::on_host_up(host);
}

void YbPartitionAwarePolicy::on_host_down(const Address& address) {
  down_hosts_.insert(address);
  ChainedLoadBalancingPolicy::on_host_down(address);
}

Host::Ptr YbPartitionAwarePolicy::YbQueryPlan::compute_next() {
  if (!leader_returned_) {
    leader_returned_ = true;
    return leader_;
  }
  Host::Ptr host;
  while ((host = child_plan_->compute_next())) {
    if (host->address() != leader_->address()) return host;
  }
  return Host::Ptr();
}
