// Copyright 2020 Open Source Robotics Foundation, Inc.
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

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "rclcpp/executors/events_executor.hpp"
#include "rclcpp/executors/events_executor_entities_collector.hpp"

using rclcpp::executors::ExecutorEvent;
using rclcpp::executors::EventsExecutorEntitiesCollector;

EventsExecutorEntitiesCollector::EventsExecutorEntitiesCollector(
  EventsExecutor * executor)
{
  if (executor == nullptr) {
    throw std::runtime_error("Received NULL executor in EventsExecutorEntitiesCollector.");
  }

  associated_executor_ = executor;
  timers_manager_ = associated_executor_->timers_manager_;
}

EventsExecutorEntitiesCollector::~EventsExecutorEntitiesCollector()
{
  // Disassociate all callback groups
  for (const auto & pair : weak_groups_associated_with_executor_to_nodes_) {
    auto group = pair.first.lock();
    if (group) {
      std::atomic_bool & has_executor = group->get_associated_with_executor_atomic();
      has_executor.store(false);
      unset_callback_group_entities_callbacks(group);
    }
  }
  for (const auto & pair : weak_groups_to_nodes_associated_with_executor_) {
    auto group = pair.first.lock();
    if (group) {
      std::atomic_bool & has_executor = group->get_associated_with_executor_atomic();
      has_executor.store(false);
      unset_callback_group_entities_callbacks(group);
    }
  }

  // Disassociate all nodes
  for (const auto & weak_node : weak_nodes_) {
    auto node = weak_node.lock();
    if (node) {
      std::atomic_bool & has_executor = node->get_associated_with_executor_atomic();
      has_executor.store(false);
    }
  }

  // Unset nodes notify guard condition executor callback
  for (const auto & pair : weak_nodes_to_guard_conditions_) {
    auto node = pair.first.lock();
    if (node) {
      auto & node_gc = pair.second;
      unset_guard_condition_callback(node_gc);
    }
  }

  // Clear all containers
  weak_nodes_.clear();
  weak_clients_map_.clear();
  weak_services_map_.clear();
  weak_waitables_map_.clear();
  weak_subscriptions_map_.clear();
  weak_nodes_to_guard_conditions_.clear();
  weak_groups_associated_with_executor_to_nodes_.clear();
  weak_groups_to_nodes_associated_with_executor_.clear();
}

void
EventsExecutorEntitiesCollector::init()
{
  // Add the EventsExecutorEntitiesCollector shared_ptr to waitables map
  weak_waitables_map_.emplace(this, this->shared_from_this());
}

void
EventsExecutorEntitiesCollector::add_node(
  rclcpp::node_interfaces::NodeBaseInterface::SharedPtr node_ptr)
{
  // Check if the node already has an executor and if not, set this to true
  std::atomic_bool & has_executor = node_ptr->get_associated_with_executor_atomic();
  if (has_executor.exchange(true)) {
    throw std::runtime_error("Node has already been added to an executor.");
  }

  // Get node callback groups, add them to weak_groups_to_nodes_associated_with_executor_
  for (const auto & weak_group : node_ptr->get_callback_groups()) {
    auto group_ptr = weak_group.lock();
    if (group_ptr != nullptr && !group_ptr->get_associated_with_executor_atomic().load() &&
      group_ptr->automatically_add_to_executor_with_node())
    {
      add_callback_group(group_ptr, node_ptr, weak_groups_to_nodes_associated_with_executor_);
    }
  }

  // Add node to weak_nodes_
  weak_nodes_.push_back(node_ptr);
}

void
EventsExecutorEntitiesCollector::add_callback_group(
  rclcpp::CallbackGroup::SharedPtr group_ptr,
  rclcpp::node_interfaces::NodeBaseInterface::SharedPtr node_ptr)
{
  add_callback_group(group_ptr, node_ptr, weak_groups_associated_with_executor_to_nodes_);
}

void
EventsExecutorEntitiesCollector::add_callback_group(
  rclcpp::CallbackGroup::SharedPtr group_ptr,
  rclcpp::node_interfaces::NodeBaseInterface::SharedPtr node_ptr,
  WeakCallbackGroupsToNodesMap & weak_groups_to_nodes)
{
  // If the callback_group already has an executor, throw error
  std::atomic_bool & has_executor = group_ptr->get_associated_with_executor_atomic();
  if (has_executor.exchange(true)) {
    throw std::runtime_error("Callback group has already been added to an executor.");
  }

  bool is_new_node = !has_node(node_ptr, weak_groups_associated_with_executor_to_nodes_) &&
    !has_node(node_ptr, weak_groups_to_nodes_associated_with_executor_);

  if (is_new_node) {

    auto notify_guard_condition = &(node_ptr->get_notify_guard_condition());
    // Set an event callback for the node's notify guard condition, so if new entities are added
    // or removed to this node we will receive an event.
    set_guard_condition_callback(notify_guard_condition);

    // Store node's notify guard condition
    weak_nodes_to_guard_conditions_[node_ptr] = notify_guard_condition;
  }

  // Add callback group to weak_groups_to_node
  rclcpp::CallbackGroup::WeakPtr weak_group_ptr = group_ptr;

  auto insert_info = weak_groups_to_nodes.insert(std::make_pair(weak_group_ptr, node_ptr));

  // Throw error if the group was already registered in the executor
  bool was_inserted = insert_info.second;
  if (!was_inserted) {
    throw std::runtime_error("Callback group was already added to executor.");
  }

  // For all entities in the callback group, set their event callback
  set_callback_group_entities_callbacks(group_ptr);
}

void
EventsExecutorEntitiesCollector::execute(std::shared_ptr<void> & data)
{
  (void)data;
  // This function is called when the associated executor is notified that something changed.
  // We do not know if an entity has been added or removed so we have to rebuild everything.

  timers_manager_->clear();

  // If a registered node has a new callback group, register the group.
  add_callback_groups_from_nodes_associated_to_executor();

  // For all groups registered in the executor, set their event callbacks.
  set_entities_event_callbacks_from_map(weak_groups_associated_with_executor_to_nodes_);
  set_entities_event_callbacks_from_map(weak_groups_to_nodes_associated_with_executor_);
}

void
EventsExecutorEntitiesCollector::add_callback_groups_from_nodes_associated_to_executor()
{
  // Register new callback groups added to a node while already spinning
  for (const auto & weak_node : weak_nodes_) {
    auto node = weak_node.lock();
    if (node) {
      auto group_ptrs = node->get_callback_groups();
      std::for_each(
        group_ptrs.begin(), group_ptrs.end(),
        [this, node](rclcpp::CallbackGroup::WeakPtr group_ptr)
        {
          auto shared_group_ptr = group_ptr.lock();
          if (shared_group_ptr && shared_group_ptr->automatically_add_to_executor_with_node() &&
          !shared_group_ptr->get_associated_with_executor_atomic().load())
          {
            add_callback_group(
              shared_group_ptr,
              node,
              weak_groups_to_nodes_associated_with_executor_);
          }
        });
    }
  }
}

void
EventsExecutorEntitiesCollector::set_entities_event_callbacks_from_map(
  const WeakCallbackGroupsToNodesMap & weak_groups_to_nodes)
{
  for (const auto & pair : weak_groups_to_nodes) {
    auto group = pair.first.lock();
    auto node = pair.second.lock();
    if (!node || !group || !group->can_be_taken_from().load()) {
      continue;
    }
    set_callback_group_entities_callbacks(group);
  }
}

void
EventsExecutorEntitiesCollector::set_callback_group_entities_callbacks(
  rclcpp::CallbackGroup::SharedPtr group)
{
  // Timers are handled by the timers manager
  group->find_timer_ptrs_if(
    [this](const rclcpp::TimerBase::SharedPtr & timer) {
      if (timer) {
        timers_manager_->add_timer(timer);
      }
      return false;
    });

  // Set callbacks for all other entity types
  group->find_subscription_ptrs_if(
    [this](const rclcpp::SubscriptionBase::SharedPtr & subscription) {
      if (subscription) {
        weak_subscriptions_map_.emplace(subscription.get(), subscription);

        subscription->set_on_new_message_callback(
          create_entity_callback(subscription.get(), SUBSCRIPTION_EVENT));
      }
      return false;
    });
  group->find_service_ptrs_if(
    [this](const rclcpp::ServiceBase::SharedPtr & service) {
      if (service) {
        weak_services_map_.emplace(service.get(), service);

        service->set_on_new_request_callback(
          create_entity_callback(service.get(), SERVICE_EVENT));
      }
      return false;
    });
  group->find_client_ptrs_if(
    [this](const rclcpp::ClientBase::SharedPtr & client) {
      if (client) {
        weak_clients_map_.emplace(client.get(), client);

        client->set_on_new_response_callback(
          create_entity_callback(client.get(), CLIENT_EVENT));
      }
      return false;
    });
  group->find_waitable_ptrs_if(
    [this](const rclcpp::Waitable::SharedPtr & waitable) {
      if (waitable) {
        weak_waitables_map_.emplace(waitable.get(), waitable);

        waitable->set_on_ready_callback(
          create_waitable_callback(waitable.get()));
      }
      return false;
    });
}

void
EventsExecutorEntitiesCollector::unset_callback_group_entities_callbacks(
  rclcpp::CallbackGroup::SharedPtr group)
{
  // Timers are handled by the timers manager
  group->find_timer_ptrs_if(
    [this](const rclcpp::TimerBase::SharedPtr & timer) {
      if (timer) {
        timers_manager_->remove_timer(timer);
      }
      return false;
    });

  // Unset callbacks for all other entity types
  group->find_subscription_ptrs_if(
    [this](const rclcpp::SubscriptionBase::SharedPtr & subscription) {
      if (subscription) {
        subscription->clear_on_new_message_callback();
        weak_subscriptions_map_.erase(subscription.get());
      }
      return false;
    });
  group->find_service_ptrs_if(
    [this](const rclcpp::ServiceBase::SharedPtr & service) {
      if (service) {
        service->clear_on_new_request_callback();
        weak_services_map_.erase(service.get());
      }
      return false;
    });
  group->find_client_ptrs_if(
    [this](const rclcpp::ClientBase::SharedPtr & client) {
      if (client) {
        client->clear_on_new_response_callback();
        weak_clients_map_.erase(client.get());
      }
      return false;
    });
  group->find_waitable_ptrs_if(
    [this](const rclcpp::Waitable::SharedPtr & waitable) {
      if (waitable) {
        waitable->clear_on_ready_callback();
        weak_waitables_map_.erase(waitable.get());
      }
      return false;
    });
}

void
EventsExecutorEntitiesCollector::remove_callback_group(
  rclcpp::CallbackGroup::SharedPtr group_ptr)
{
  this->remove_callback_group_from_map(
    group_ptr,
    weak_groups_associated_with_executor_to_nodes_);
}

void
EventsExecutorEntitiesCollector::remove_callback_group_from_map(
  rclcpp::CallbackGroup::SharedPtr group_ptr,
  WeakCallbackGroupsToNodesMap & weak_groups_to_nodes)
{
  rclcpp::node_interfaces::NodeBaseInterface::SharedPtr node_ptr;
  rclcpp::CallbackGroup::WeakPtr weak_group_ptr = group_ptr;

  // Look for the group to remove in the map
  auto iter = weak_groups_to_nodes.find(weak_group_ptr);
  if (iter == weak_groups_to_nodes.end()) {
    // Group not found.
    throw std::runtime_error("Callback group needs to be associated with this executor.");
  }

  // Group found, get its associated node.
  node_ptr = iter->second.lock();
  if (node_ptr == nullptr) {
    throw std::runtime_error("Node must not be deleted before its callback group(s).");
  }
  // Remove group from map
  weak_groups_to_nodes.erase(iter);

  // For all the entities in the group, unset their callbacks
  unset_callback_group_entities_callbacks(group_ptr);

  // Mark this callback group as not associated with an executor anymore
  std::atomic_bool & has_executor = group_ptr->get_associated_with_executor_atomic();
  has_executor.store(false);

  // Check if this node still has other callback groups associated with the executor
  bool node_has_associated_callback_groups =
    has_node(node_ptr, weak_groups_associated_with_executor_to_nodes_) ||
    has_node(node_ptr, weak_groups_to_nodes_associated_with_executor_);

  if (!node_has_associated_callback_groups) {
    // Node doesn't have more callback groups associated to the executor.
    // Unset the event callback for the node's notify guard condition, to stop
    // receiving events if entities are added or removed to this node.
    unset_guard_condition_callback(&(node_ptr->get_notify_guard_condition()));

    // Remove guard condition from list
    rclcpp::node_interfaces::NodeBaseInterface::WeakPtr weak_node_ptr(node_ptr);
    weak_nodes_to_guard_conditions_.erase(weak_node_ptr);
  }
}

void
EventsExecutorEntitiesCollector::remove_node(
  rclcpp::node_interfaces::NodeBaseInterface::SharedPtr node_ptr)
{
  if (!node_ptr->get_associated_with_executor_atomic().load()) {
    throw std::runtime_error("Node needs to be associated with an executor.");
    return;
  }
  // Check if this node is currently stored here
  auto node_it = weak_nodes_.begin();
  while (node_it != weak_nodes_.end()) {
    bool matched = (node_it->lock() == node_ptr);
    if (matched) {
      weak_nodes_.erase(node_it);
      break;
    }
    ++node_it;
  }
  if (node_it == weak_nodes_.end()) {
    // The node is not stored here
    throw std::runtime_error("Tried to remove node not stored in this executor.");
    return;
  }

  // Find callback groups belonging to the node to remove
  std::vector<rclcpp::CallbackGroup::SharedPtr> found_group_ptrs;
  std::for_each(
    weak_groups_to_nodes_associated_with_executor_.begin(),
    weak_groups_to_nodes_associated_with_executor_.end(),
    [&found_group_ptrs, node_ptr](std::pair<rclcpp::CallbackGroup::WeakPtr,
    rclcpp::node_interfaces::NodeBaseInterface::WeakPtr> key_value_pair) {
      auto & weak_node_ptr = key_value_pair.second;
      auto shared_node_ptr = weak_node_ptr.lock();
      auto group_ptr = key_value_pair.first.lock();
      if (shared_node_ptr == node_ptr) {
        found_group_ptrs.push_back(group_ptr);
      }
    });
  // Remove those callback groups
  std::for_each(
    found_group_ptrs.begin(), found_group_ptrs.end(), [this]
      (rclcpp::CallbackGroup::SharedPtr group_ptr) {
      this->remove_callback_group_from_map(
        group_ptr,
        weak_groups_to_nodes_associated_with_executor_);
    });

  // Set that the node does not have an executor anymore
  std::atomic_bool & has_executor = node_ptr->get_associated_with_executor_atomic();
  has_executor.store(false);
}

// Returns true if the map has the node_ptr
bool
EventsExecutorEntitiesCollector::has_node(
  const rclcpp::node_interfaces::NodeBaseInterface::SharedPtr node_ptr,
  const WeakCallbackGroupsToNodesMap & weak_groups_to_nodes) const
{
  return std::find_if(
    weak_groups_to_nodes.begin(),
    weak_groups_to_nodes.end(),
    [&](const WeakCallbackGroupsToNodesMap::value_type & other) -> bool {
      auto other_ptr = other.second.lock();
      return other_ptr == node_ptr;
    }) != weak_groups_to_nodes.end();
}

std::vector<rclcpp::CallbackGroup::WeakPtr>
EventsExecutorEntitiesCollector::get_all_callback_groups()
{
  std::vector<rclcpp::CallbackGroup::WeakPtr> groups;
  for (const auto & group_node_ptr : weak_groups_associated_with_executor_to_nodes_) {
    groups.push_back(group_node_ptr.first);
  }
  for (const auto & group_node_ptr : weak_groups_to_nodes_associated_with_executor_) {
    groups.push_back(group_node_ptr.first);
  }
  return groups;
}

std::vector<rclcpp::CallbackGroup::WeakPtr>
EventsExecutorEntitiesCollector::get_manually_added_callback_groups()
{
  std::vector<rclcpp::CallbackGroup::WeakPtr> groups;
  for (const auto & group_node_ptr : weak_groups_associated_with_executor_to_nodes_) {
    groups.push_back(group_node_ptr.first);
  }
  return groups;
}

std::vector<rclcpp::CallbackGroup::WeakPtr>
EventsExecutorEntitiesCollector::get_automatically_added_callback_groups_from_nodes()
{
  std::vector<rclcpp::CallbackGroup::WeakPtr> groups;
  for (const auto & group_node_ptr : weak_groups_to_nodes_associated_with_executor_) {
    groups.push_back(group_node_ptr.first);
  }
  return groups;
}

void
EventsExecutorEntitiesCollector::set_guard_condition_callback(
  rclcpp::GuardCondition * guard_condition)
{
  auto gc_callback = [this](size_t num_events) {
    // Override num events (we don't care more than a single event)
    num_events = 1;
    int gc_id = -1;
    ExecutorEvent event = {this, gc_id, WAITABLE_EVENT, num_events};
    associated_executor_->events_queue_->enqueue(event);
  };

  guard_condition->set_on_trigger_callback(gc_callback);
}

void
EventsExecutorEntitiesCollector::unset_guard_condition_callback(
  rclcpp::GuardCondition * guard_condition)
{
   guard_condition->set_on_trigger_callback(nullptr);
}

rclcpp::SubscriptionBase::SharedPtr
EventsExecutorEntitiesCollector::get_subscription(const void * subscription_id)
{
  auto it = weak_subscriptions_map_.find(subscription_id);

  if (it != weak_subscriptions_map_.end()) {
    auto subscription_weak_ptr = it->second;
    auto subscription_shared_ptr = subscription_weak_ptr.lock();

    if (subscription_shared_ptr) {
      return subscription_shared_ptr;
    }

    // The subscription expired, remove from map
    weak_subscriptions_map_.erase(it);
  }
  return nullptr;
}

rclcpp::ClientBase::SharedPtr
EventsExecutorEntitiesCollector::get_client(const void * client_id)
{
  auto it = weak_clients_map_.find(client_id);

  if (it != weak_clients_map_.end()) {
    auto client_weak_ptr = it->second;
    auto client_shared_ptr = client_weak_ptr.lock();

    if (client_shared_ptr) {
      return client_shared_ptr;
    }

    // The client expired, remove from map
    weak_clients_map_.erase(it);
  }
  return nullptr;
}

rclcpp::ServiceBase::SharedPtr
EventsExecutorEntitiesCollector::get_service(const void * service_id)
{
  auto it = weak_services_map_.find(service_id);

  if (it != weak_services_map_.end()) {
    auto service_weak_ptr = it->second;
    auto service_shared_ptr = service_weak_ptr.lock();

    if (service_shared_ptr) {
      return service_shared_ptr;
    }

    // The service expired, remove from map
    weak_services_map_.erase(it);
  }
  return nullptr;
}

rclcpp::Waitable::SharedPtr
EventsExecutorEntitiesCollector::get_waitable(const void * waitable_id)
{
  auto it = weak_waitables_map_.find(waitable_id);

  if (it != weak_waitables_map_.end()) {
    auto waitable_weak_ptr = it->second;
    auto waitable_shared_ptr = waitable_weak_ptr.lock();

    if (waitable_shared_ptr) {
      return waitable_shared_ptr;
    }

    // The waitable expired, remove from map
    weak_waitables_map_.erase(it);
  }
  return nullptr;
}

void
EventsExecutorEntitiesCollector::add_waitable(rclcpp::Waitable::SharedPtr waitable)
{
  weak_waitables_map_.emplace(waitable.get(), waitable);

  waitable->set_on_ready_callback(
    create_waitable_callback(waitable.get()));
}

std::function<void(size_t)>
EventsExecutorEntitiesCollector::create_entity_callback(
  void * exec_entity_id, ExecutorEventType event_type)
{
  return [this, exec_entity_id, event_type](size_t num_events) {
    ExecutorEvent event = {exec_entity_id, -1, event_type, num_events};
    associated_executor_->events_queue_->enqueue(event);
  };
}

std::function<void(size_t, int)>
EventsExecutorEntitiesCollector::create_waitable_callback(void * exec_entity_id)
{
  return [this, exec_entity_id](size_t num_events, int gen_entity_id) {
    ExecutorEvent event = {exec_entity_id, gen_entity_id, WAITABLE_EVENT, num_events};
    associated_executor_->events_queue_->enqueue(event);
  };
}

size_t
EventsExecutorEntitiesCollector::get_entity_qos_depth(
  const rclcpp::executors::ExecutorEvent & event)
{
  switch (event.type) {
    case TIMER_EVENT:
      {
        return 1;
      }
    case SUBSCRIPTION_EVENT:
      {
        auto subscription = get_subscription(event.exec_entity_id);

        if (subscription) {
          return subscription->get_actual_qos().depth();
        }
        break;
      }

    case SERVICE_EVENT:
      {
        auto service = get_service(event.exec_entity_id);

        if (service) {
          return service->get_actual_qos().depth();
        }
        break;
      }

    case CLIENT_EVENT:
      {
        auto client = get_client(event.exec_entity_id);

        if (client) {
          return client->get_actual_qos().depth();
        }
        break;
      }

    case WAITABLE_EVENT:
      {
        auto waitable = get_waitable(event.exec_entity_id);
        if (waitable) {
          using rclcpp::experimental::SubscriptionIntraProcessBase;
          if (auto sub_ipc = dynamic_cast<SubscriptionIntraProcessBase *>(waitable.get())) {
            return sub_ipc->get_actual_qos().depth;
          } else {
            // For waitables other than intra-process subscriptions that
            // don't have QoS or queued events, we return 1.
            return 1;
          }
        }
        break;
      }
  }

  return 0;
}
