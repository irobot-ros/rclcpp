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

#ifndef RCLCPP__EXPERIMENTAL__BUFFERS__BOUNDED_EVENTS_QUEUE_HPP_
#define RCLCPP__EXPERIMENTAL__BUFFERS__BOUNDED_EVENTS_QUEUE_HPP_

#include <mutex>
#include <queue>
#include <unordered_map>
#include <utility>
#include <vector>

#include "rclcpp/experimental/buffers/events_queue.hpp"
#include "rclcpp/executors/events_executor_entities_collector.hpp"

namespace rclcpp
{
namespace experimental
{
namespace buffers
{

/**
 * @brief This class implements an EventsQueue as a wrapper around a std::vector.
 * It keeps track of amount of events in the queue for each entity.
 * Each entity has a limit of events which makes sense to have in the queue,
 * for example a subscription with QoS.depth = 5 shoud have as max 5 events in the queue.
 * If a 6th event for that subscription arrives, the 1st event in the queue is removed
 * and a new one is added at the back, to maintain correct ordering of events.
 * This way the queue doesn't grow un-bounded.
 */
class BoundedEventsQueue : public EventsQueue
{
public:
  enum class QueuePolicy
  {
    NotBounded,
    BoundedNoTimeOrdering,
    BoundedWithTimeOrdering,
  };

  RCLCPP_PUBLIC
  explicit BoundedEventsQueue(QueuePolicy policy = QueuePolicy::BoundedWithTimeOrdering)
  {
    queue_policy_ = policy;
  }

  RCLCPP_PUBLIC
  ~BoundedEventsQueue() override
  {
    event_queue_.clear();
    entity_events.clear();
  }

  /**
   * @brief enqueue event into the queue
   * If the limit of events per entity is surpassed, the 1st
   * event is removed and a new one is added at the back of the queue.
   * Thread safe
   * @param event The event to enqueue into the queue
   */
  RCLCPP_PUBLIC
  void
  enqueue(const rclcpp::executors::ExecutorEvent & event) override
  {
    rclcpp::executors::ExecutorEvent single_event = event;
    single_event.num_events = 1;
    {
      std::unique_lock<std::mutex> lock(mutex_);
      for (size_t ev = 0; ev < event.num_events; ev++) {
        if (queue_policy_ == QueuePolicy::NotBounded) {
          // Just push the event. No checks
          event_queue_.push_back(single_event);
        } else {
          if (!max_events_limit_reached(event)) {
            // Push the event only if the limit has not been reached
            event_queue_.push_back(single_event);
          } else if (queue_policy_ == QueuePolicy::BoundedWithTimeOrdering) {
            // Relocate the event, if we want to keep time ordering of events
            remove_first_and_push_back(event);
          }
        }
      }
    }
    events_queue_cv_.notify_one();
  }

  /**
   * @brief waits for an event until timeout, gets a single event
   * (if any) and decreases the event count for the event entity.
   * Thread safe
   * @return true if event, false if timeout
   */
  RCLCPP_PUBLIC
  bool
  dequeue(
    rclcpp::executors::ExecutorEvent & event,
    std::chrono::nanoseconds timeout = std::chrono::nanoseconds::max()) override
  {
    std::unique_lock<std::mutex> lock(mutex_);

    // Initialize to true because it's only needed if we have a valid timeout
    bool has_data = true;
    if (timeout != std::chrono::nanoseconds::max()) {
      has_data =
        events_queue_cv_.wait_for(lock, timeout, [this]() {return !event_queue_.empty();});
    } else {
      events_queue_cv_.wait(lock, [this]() {return !event_queue_.empty();});
    }

    if (has_data) {
      event = event_queue_.front();
      // Decrease the counter of events from this entity
      if(queue_policy_ != QueuePolicy::NotBounded) {
        decrease_entity_events_count(event);
      }
      // Remove first element from queue
      event_queue_.erase(event_queue_.begin());
      return true;
    }

    return false;
  }

  /**
   * @brief Test whether queue is empty
   * Thread safe
   * @return true if the queue's size is 0, false otherwise.
   */
  RCLCPP_PUBLIC
  bool
  empty() const override
  {
    std::unique_lock<std::mutex> lock(mutex_);
    return event_queue_.empty();
  }

  /**
   * @brief Returns the number of elements in the queue.
   * Thread safe
   * @return the number of elements in the queue.
   */
  RCLCPP_PUBLIC
  size_t
  size() const override
  {
    std::unique_lock<std::mutex> lock(mutex_);
    return event_queue_.size();
  }

  /**
   * @brief Initializes the queue
   */
  RCLCPP_PUBLIC
  virtual
  void
  init(rclcpp::executors::EventsExecutorEntitiesCollector::SharedPtr entities_collector)
  {
    entities_collector_ = entities_collector;
  }

private:
  /**
   * @brief Compares current amount of events in the queue of a particular entity
   * with the maximum allowed number of events for it.
   * This function is not thread safe.
   * @return true if the limit of events in the queue has been reached for
   * the entity.
  */
  bool max_events_limit_reached(const rclcpp::executors::ExecutorEvent & event)
  {
    // Lets look for existing events like this one
    for (auto & entity_event : entity_events) {
      if (entity_event.entity_id == event.exec_entity_id) {
        // Entity ID matched. Lets check about the sub-entity ID
        if (entity_event.sub_entity_id == event.gen_entity_id) {
          // Sub-entity ID also matched! Lets see how many events do we already have.
          if (entity_event.current_events >= entity_event.max_events) {
            // We reached the maximum amounts of events for this entity
            return true;
          } else {
            // We haven't reached the maximum amounts of events for this
            // entity. Increment its counter
            entity_event.current_events++;
            return false;
          }
        }
      }
    }

    // If we are at this point, it means no events were found in the queue
    // for the entity which generated the event. Lets keep track of it.
    EntityEvents new_event;

    new_event.entity_id = event.exec_entity_id;
    new_event.sub_entity_id = event.gen_entity_id;
    new_event.current_events = 1;

    // Get max amount of events allowed for this entity
    if (entities_collector_) {
      new_event.max_events = entities_collector_->get_entity_qos_depth(event);
    } else {
      throw std::runtime_error(
        "BoundedEventsQueue has to be initialized with entities collector.");
    }

    entity_events.push_back(new_event);
    return false;
  }

  /**
   * @brief Decrease the counter of events of the entity which
   * generated the event.
   * This function is not thread safe.
  */
  void decrease_entity_events_count(const rclcpp::executors::ExecutorEvent & event)
  {
    for (auto it = entity_events.begin(); it != entity_events.end(); ++it) {
      if (it->entity_id == event.exec_entity_id) {
        // Entity ID matched. Lets check about the sub-entity ID
        if (it->sub_entity_id == event.gen_entity_id) {
          // Sub-entity ID also matched! Let's decrease the counter.
          if (it->current_events == 1) {
            // If we only have one event, remove entity event from vector.
            entity_events.erase(it);
          } else {
            it->current_events--;
          }
          // We're done here. Return
          return;
        }
      }
    }

    throw std::runtime_error("Tried to decrease counter of non-existing event!");
  }

  /**
   * @brief To keep correct time ordering of events, remove
   * the first event (oldest, maybe expired) and add a new one in
   * the back were newer events are.
   * This function is not thread safe.
  */
  void
  remove_first_and_push_back(const rclcpp::executors::ExecutorEvent & event)
  {
    for (auto it = event_queue_.begin(); it != event_queue_.end(); ++it) {
      if (it->exec_entity_id == event.exec_entity_id) {
        if (it->gen_entity_id == event.gen_entity_id) {
          event_queue_.erase(it);
          break;
        }
      }
    }

    event_queue_.push_back(event);
  }

  // The underlying queue implementation
  std::vector<rclcpp::executors::ExecutorEvent> event_queue_;
  // Mutex to protect the insertion/extraction of events in the queue
  mutable std::mutex mutex_;
  // Variable used to notify when an event is added to the queue
  std::condition_variable events_queue_cv_;
  // The entities collector associated with the executor, which provides
  // useful info to bound the queue
  rclcpp::executors::EventsExecutorEntitiesCollector::SharedPtr entities_collector_;
  // Struct to count events in the queue from different entities
  struct EntityEvents
  {
    const void * entity_id;
    int sub_entity_id;
    size_t current_events;
    size_t max_events;
  };
  // Vector to hold info about events present in the queue
  std::vector<EntityEvents> entity_events;

  // Queue policy
  QueuePolicy queue_policy_;
};

}  // namespace buffers
}  // namespace experimental
}  // namespace rclcpp

#endif  // RCLCPP__EXPERIMENTAL__BUFFERS__BOUNDED_EVENTS_QUEUE_HPP_
