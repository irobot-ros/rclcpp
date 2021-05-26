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

#ifndef RCLCPP__EXECUTORS__EVENTS_EXECUTOR_NOTIFY_WAITABLE_HPP_
#define RCLCPP__EXECUTORS__EVENTS_EXECUTOR_NOTIFY_WAITABLE_HPP_

#include <list>
#include <memory>

#include "rcl/guard_condition.h"
#include "rclcpp/executors/event_waitable.hpp"

namespace rclcpp
{
namespace executors
{

/**
 * @brief This class provides an EventWaitable that allows to
 * wake up an EventsExecutor when a guard condition is notified.
 */
class EventsExecutorNotifyWaitable final : public EventWaitable
{
public:
  RCLCPP_SMART_PTR_DEFINITIONS(EventsExecutorNotifyWaitable)

  // Constructor
  RCLCPP_PUBLIC
  EventsExecutorNotifyWaitable() = default;

  // Destructor
  RCLCPP_PUBLIC
  virtual ~EventsExecutorNotifyWaitable() = default;

  // The function is a no-op, since we only care of waking up the executor
  RCLCPP_PUBLIC
  void
  execute(std::shared_ptr<void> & data) override
  {
    (void)data;
  }

  RCLCPP_PUBLIC
  void
  add_guard_condition(const rclcpp::GuardCondition * guard_condition)
  {
    notify_guard_conditions_.push_back(guard_condition);
  }

  RCLCPP_PUBLIC
  void
  set_on_ready_callback(std::function<void(size_t, int)> callback) override
  {
    (void)callback;
    // for (auto gc : notify_guard_conditions_) {
    //   gc->set_listener_callback();
    // }
  }

  RCLCPP_PUBLIC
  std::shared_ptr<void>
  take_data()
  {
    // This waitable doesn't handle any data
    return nullptr;
  }

private:
  std::list<const rclcpp::GuardCondition *> notify_guard_conditions_;
};

}  // namespace executors
}  // namespace rclcpp

#endif  // RCLCPP__EXECUTORS__EVENTS_EXECUTOR_NOTIFY_WAITABLE_HPP_
