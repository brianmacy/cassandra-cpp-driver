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

#ifndef DATASTAX_INTERNAL_YB_PARTITION_AWARE_POLICY_HPP
#define DATASTAX_INTERNAL_YB_PARTITION_AWARE_POLICY_HPP

#include "address.hpp"
#include "load_balancing.hpp"
#include "scoped_ptr.hpp"

namespace datastax { namespace internal { namespace core {

class Request;

/**
 * YugabyteDB partition-aware routing: send a bound statement to the leader of the tablet that
 * owns its partition hash first, then fall through to the child policy's plan.
 *
 * The tablet-leader map rides the TokenMap snapshot each request processor already receives on
 * its own event loop (TokenMap::yb_partitions()), so new_query_plan() takes no lock: one hash of
 * the bound key bytes, one hash-map find and one binary search.
 *
 * Threading: like every chained policy, an instance is owned by a single request processor (or
 * the cluster), and its host up/down callbacks and new_query_plan() run on that owner's event
 * loop, so the down-host set below needs no lock.
 */
class YbPartitionAwarePolicy : public ChainedLoadBalancingPolicy {
public:
  YbPartitionAwarePolicy(LoadBalancingPolicy* child_policy);

  virtual ~YbPartitionAwarePolicy() {}

  virtual void init(const Host::Ptr& connected_host, const HostMap& hosts, Random* random,
                    const String& local_dc);

  virtual QueryPlan* new_query_plan(const String& keyspace, RequestHandler* request_handler,
                                    const TokenMap* token_map);

  virtual LoadBalancingPolicy* new_instance() {
    return new YbPartitionAwarePolicy(child_policy_->new_instance());
  }

  virtual void on_host_added(const Host::Ptr& host);
  virtual void on_host_removed(const Host::Ptr& host);
  virtual void on_host_up(const Host::Ptr& host);
  virtual void on_host_down(const Address& address);

  /**
   * The YB partition hash and "keyspace.table" of a routable request: an EXECUTE of a prepared
   * statement whose partition-key values are all bound, or the first such statement of a BATCH.
   * Returns false (no routing) for anything else, including unset or NULL key values.
   */
  static bool yb_hash_code(const Request* request, uint16_t* code, const String** ks_table);

private:
  class YbQueryPlan : public QueryPlan {
  public:
    YbQueryPlan(const Host::Ptr& leader, QueryPlan* child_plan)
        : leader_(leader)
        , child_plan_(child_plan)
        , leader_returned_(false) {}

    Host::Ptr compute_next();

  private:
    Host::Ptr leader_;
    ScopedPtr<QueryPlan> child_plan_;
    bool leader_returned_;
  };

  AddressSet down_hosts_;

private:
  DISALLOW_COPY_AND_ASSIGN(YbPartitionAwarePolicy);
};

}}} // namespace datastax::internal::core

#endif
