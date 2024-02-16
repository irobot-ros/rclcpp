// Copyright 2021 Open Source Robotics Foundation, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "rclcpp/detail/add_guard_condition_to_rcl_wait_set.hpp"
#include "rclcpp/experimental/service_intra_process_base.hpp"

using rclcpp::experimental::ServiceIntraProcessBase;

void
ServiceIntraProcessBase::add_to_wait_set(rcl_wait_set_t * wait_set)
{
  detail::add_guard_condition_to_rcl_wait_set(*wait_set, gc_);
}

const char *
ServiceIntraProcessBase::get_service_name() const
{
  return service_name_.c_str();
}

rclcpp::QoS
ServiceIntraProcessBase::get_actual_qos() const
{
  return qos_profile_;
}

void
ServiceIntraProcessBase::add_intra_process_client(
  rclcpp::experimental::ClientIntraProcessBase::SharedPtr client,
  uint64_t client_id)
{
  std::unique_lock<std::recursive_mutex> lock(reentrant_mutex_);
  clients_[client_id] = client;
}

uint64_t
ServiceIntraProcessBase::get_unique_request_id()
{
  static std::atomic<uint64_t> _next_unique_id {1};

  auto next_id = _next_unique_id.fetch_add(1, std::memory_order_relaxed);
  // Check for rollover (we started at 1).
  if (0 == next_id) {
    // This puts a technical limit on the number of times you can add a publisher or subscriber.
    // But even if you could add (and remove) them at 1 kHz (very optimistic rate)
    // it would still be a very long time before you could exhaust the pool of id's:
    //   2^64 / 1000 times per sec / 60 sec / 60 min / 24 hours / 365 days = 584,942,417 years
    // So around 585 million years. Even at 1 GHz, it would take 585 years.
    // I think it's safe to avoid trying to handle overflow.
    // If we roll over then it's most likely a bug.
    // *INDENT-OFF* (prevent uncrustify from making unnecessary indents here)
    throw std::overflow_error(
      "exhausted the unique id's for publishers and subscribers in this process "
      "(congratulations your computer is either extremely fast or extremely old)");
    // *INDENT-ON*
  }
  return next_id;
}
