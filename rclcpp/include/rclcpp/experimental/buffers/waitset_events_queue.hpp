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

#ifndef RCLCPP__EXPERIMENTAL__BUFFERS__WAITSET_EVENTS_QUEUE_HPP_
#define RCLCPP__EXPERIMENTAL__BUFFERS__WAITSET_EVENTS_QUEUE_HPP_

#include <list>
#include <mutex>
#include <utility>

#include "rclcpp/experimental/buffers/events_queue.hpp"
#include "rclcpp/executors/events_executor_entities_collector.hpp"

namespace rclcpp
{
namespace experimental
{
namespace buffers
{

/**
 * @brief This class implements an EventsQueue as a waitset
 */
class WaitSetEventsQueue : public EventsQueue
{
public:
  /**
   * @brief enqueue event into the wait set
   * Thread safe
   * @param event The event to enqueue into the wait set
   */
  RCLCPP_PUBLIC
  void
  enqueue(const rclcpp::executors::ExecutorEvent & event) override
  {
    {
      std::unique_lock<std::mutex> lock(wait_set_mutex_);
      update_wait_set(event);
    }
    wait_set_cv_.notify_one();
  }

  /**
   * @brief waits for an event until timeout and gets a single event
   * Thread safe
   * @return true if event, false if timeout
   */
  RCLCPP_PUBLIC
  bool
  dequeue(
    rclcpp::executors::ExecutorEvent & event,
    std::chrono::nanoseconds timeout = std::chrono::nanoseconds::max()) override
  {
    std::unique_lock<std::mutex> lock(wait_set_mutex_);

    // Initialize to true because it's only needed if we have a valid timeout
    bool has_data = true;
    if (timeout != std::chrono::nanoseconds::max()) {
      has_data =
        wait_set_cv_.wait_for(lock, timeout, [this]() {return !this->is_empty_unsafe();});
    } else {
      wait_set_cv_.wait(lock, [this]() {return !this->is_empty_unsafe();});
    }

    if (has_data) {
      event = get_next_event();
      return true;
    }

    return false;
  }

  /**
   * @brief Test whether wait set is empty
   * Thread safe
   * @return true if the wait set size is 0, false otherwise.
   */
  RCLCPP_PUBLIC
  bool
  empty() const override
  {
    std::unique_lock<std::mutex> lock(wait_set_mutex_);
    return is_empty_unsafe();
  }

  /**
   * @brief Returns the number of elements in the wait set.
   * Thread safe
   * @return the number of elements in the wait set.
   */
  RCLCPP_PUBLIC
  size_t size() const override
  {
    std::unique_lock<std::mutex> lock(wait_set_mutex_);
    return count_events(timers_wait_set_) +
           count_events(subscriptions_wait_set_) +
           count_events(services_wait_set_) +
           count_events(clients_wait_set_) +
           count_events(waitables_wait_set_);
  }

  /**
   * @brief Initializes the entities collector
   */
  RCLCPP_PUBLIC
  virtual
  void
  init(rclcpp::executors::EventsExecutorEntitiesCollector::SharedPtr entities_collector)
  {
    entities_collector_ = entities_collector;
  }

private:
  // Struct combining an entity event with its maximum allowed number of them,
  // usually set by the entity QoS depth.
  struct EntityEvent {
    rclcpp::executors::ExecutorEvent event;
    size_t num_events;
    size_t max_events;

    EntityEvent(
      rclcpp::executors::ExecutorEvent e, size_t num_evs, size_t max_evs)
        : event(e), num_events(num_evs), max_events(max_evs) {}

    bool operator==(const rclcpp::executors::ExecutorEvent & rhs) const
    {
      return (event.exec_entity_id == rhs.exec_entity_id) &&
             (event.gen_entity_id == rhs.gen_entity_id);
    }
  };

  /**
   * @brief Adds event to the corresponding wait set.
   */
  void update_wait_set(const rclcpp::executors::ExecutorEvent & event)
  {
    switch (event.type) {
      case rclcpp::executors::TIMER_EVENT:
        add_to_wait_set(event, timers_wait_set_);
        break;

      case rclcpp::executors::SUBSCRIPTION_EVENT:
        add_to_wait_set(event, subscriptions_wait_set_);
        break;

      case rclcpp::executors::SERVICE_EVENT:
        add_to_wait_set(event, services_wait_set_);
        break;

      case rclcpp::executors::CLIENT_EVENT:
        add_to_wait_set(event, clients_wait_set_);
        break;

      case rclcpp::executors::WAITABLE_EVENT:
        add_to_wait_set(event, waitables_wait_set_);
        break;
    }
  }

  /**
   * @brief Adds event to the the wait set if it is new, otherwise increase counter
   */
  void add_to_wait_set(
    const rclcpp::executors::ExecutorEvent & event,
    std::list<EntityEvent> & wait_set)
  {
    // Lets look for existing events like this one
    auto it = std::find(wait_set.begin(), wait_set.end(), event);

    if (it != wait_set.end()) {
      // Not the first event from this entity in waitset,
      // so lets compute number of events for this kind.
      if (it->num_events < it->max_events) {
        // We haven't reached the maximum amounts of events for this
        // entity. Increment its counter, limited by max events.
        size_t num_events = it->num_events + event.num_events;
        it->num_events = std::min(num_events, it->max_events);
        return;
      } else {
        // We reached the maximum amounts of events for this entity.
        return;
      }
    }

    // If we are at this point, it means no elements were found in the wait set
    // for the entity which generated the event. Lets keep track of it.
    size_t max_events = entities_collector_->get_entity_qos_depth(event);

    if (event.type != currently_executing_) {
      // Just push the new event, we're not using the iterator from this waitset
      wait_set.emplace_back(event, event.num_events, max_events);
    } else {
      // Push the new event and update the iterator if needed
      if (wait_set.empty()) {
        wait_set.emplace_back(event, event.num_events, max_events);
        event_iterator_ = wait_set.begin();
      } else {
        wait_set.emplace_back(event, event.num_events, max_events);
      }
    }
  }

  // Iterate the wait set getting events in order as followed in rcl/wait.c
  // i.e; timers, subscriptions, services, clients, waitables.
  // If this function is called, is because we know there's at least one
  // event in the wait set to retrieve. Otherwise will keep stuck in a loop
  rclcpp::executors::ExecutorEvent get_next_event()
  {
    while (true) {
      std::list<EntityEvent> * current_wait_set;
      std::list<EntityEvent> * next_wait_set;
      rclcpp::executors::ExecutorEventType next_to_execute;

      switch (currently_executing_) {
        case rclcpp::executors::TIMER_EVENT:
          current_wait_set = &timers_wait_set_;
          next_wait_set = &subscriptions_wait_set_;
          next_to_execute = rclcpp::executors::SUBSCRIPTION_EVENT;
          break;

        case rclcpp::executors::SUBSCRIPTION_EVENT:
          current_wait_set = &subscriptions_wait_set_;
          next_wait_set = &services_wait_set_;
          next_to_execute = rclcpp::executors::SERVICE_EVENT;
          break;

        case rclcpp::executors::SERVICE_EVENT:
          current_wait_set = &services_wait_set_;
          next_wait_set = &clients_wait_set_;
          next_to_execute = rclcpp::executors::CLIENT_EVENT;
          break;

        case rclcpp::executors::CLIENT_EVENT:
          current_wait_set = &clients_wait_set_;
          next_wait_set = &waitables_wait_set_;
          next_to_execute = rclcpp::executors::WAITABLE_EVENT;
          break;

        case rclcpp::executors::WAITABLE_EVENT:
          current_wait_set = &waitables_wait_set_;
          next_wait_set = &timers_wait_set_;
          next_to_execute = rclcpp::executors::TIMER_EVENT;
          break;
      }

      if (current_wait_set->empty()) {
        event_iterator_ = next_wait_set->begin();
        currently_executing_ = next_to_execute;
      } else {
        rclcpp::executors::ExecutorEvent next_event = event_iterator_->event;
        decrease_events_count(current_wait_set);
        if (event_iterator_ == current_wait_set->end()) {
          event_iterator_ = next_wait_set->begin();
          currently_executing_ = next_to_execute;
        }
        return next_event;
      }
    }
  }

  // Decrease events counter and update event iterator to point to
  // next element in wait set.
  // If the events counter becomes zero, remove event from wait set.
  //
  inline void decrease_events_count(std::list<EntityEvent> * wait_set)
  {
    // Decrease events counter
    event_iterator_->num_events--;

    // Update iterator to point to next element
    if (event_iterator_->num_events == 0) {
      event_iterator_ = wait_set->erase(event_iterator_);
    } else {
      event_iterator_++;
    }
  }

  inline bool is_empty_unsafe() const
  {
    if (!waitables_wait_set_.empty()) {
      return false;
    }
    if (!timers_wait_set_.empty()) {
      return false;
    }
    if (!subscriptions_wait_set_.empty()) {
      return false;
    }
    if (!services_wait_set_.empty()) {
      return false;
    }
    if (!clients_wait_set_.empty()) {
      return false;
    }

    return true;
  }

  inline size_t count_events(const std::list<EntityEvent> & wait_set) const
  {
    size_t num_events = 0;
    for (auto & entity : wait_set) {
      num_events += entity.num_events;
    }
    return num_events;
  }

  // Mutex to protect the waitset
  mutable std::mutex wait_set_mutex_;
  // Variable used to notify when the waitset has been updated
  std::condition_variable wait_set_cv_;
  // The entities collector associated with the executor, which provides
  // useful info about entities in the waiteset
  rclcpp::executors::EventsExecutorEntitiesCollector::SharedPtr
      entities_collector_;

  // Waitset maps splited by entity type, for convenience.
  // They map each entity with the number of events from them
  // in the wait set.
  std::list<EntityEvent> timers_wait_set_;
  std::list<EntityEvent> subscriptions_wait_set_;
  std::list<EntityEvent> services_wait_set_;
  std::list<EntityEvent> clients_wait_set_;
  std::list<EntityEvent> waitables_wait_set_;

  // Iterator pointing to next ready executable.
  std::list<EntityEvent>::iterator event_iterator_;

  // Event type used to know what entity to retrieve next when
  // calling dequeue
  rclcpp::executors::ExecutorEventType currently_executing_ =
      rclcpp::executors::TIMER_EVENT;
};

}  // namespace buffers
}  // namespace experimental
}  // namespace rclcpp

#endif  // RCLCPP__EXPERIMENTAL__BUFFERS__WAITSET_EVENTS_QUEUE_HPP_
