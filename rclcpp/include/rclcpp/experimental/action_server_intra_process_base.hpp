// Copyright 2022 Open Source Robotics Foundation, Inc.
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

#ifndef RCLCPP__EXPERIMENTAL__ACTION_SERVER_INTRA_PROCESS_BASE_HPP_
#define RCLCPP__EXPERIMENTAL__ACTION_SERVER_INTRA_PROCESS_BASE_HPP_

#include <algorithm>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>

#include "rmw/impl/cpp/demangle.hpp"

#include "rclcpp/context.hpp"
#include "rclcpp/experimental/action_client_intra_process_base.hpp"
#include "rclcpp/guard_condition.hpp"
#include "rclcpp/logging.hpp"
#include "rclcpp/qos.hpp"
#include "rclcpp/type_support_decl.hpp"
#include "rclcpp/waitable.hpp"

namespace rclcpp
{
namespace experimental
{

class ActionServerIntraProcessBase : public rclcpp::Waitable
{
public:
  RCLCPP_SMART_PTR_ALIASES_ONLY(ActionServerIntraProcessBase)

  // The action server event types
  enum class EventType : std::size_t
  {
    GoalRequest,
    CancelGoal,
    ResultRequest,
  };

  RCLCPP_PUBLIC
  ActionServerIntraProcessBase(
    rclcpp::Context::SharedPtr context,
    const std::string & action_name,
    const rclcpp::QoS & qos_profile)
  : gc_(context),
    action_name_(action_name),
    qos_profile_(qos_profile)
  {}

  virtual ~ActionServerIntraProcessBase() = default;

  RCLCPP_PUBLIC
  size_t
  get_number_of_ready_guard_conditions() override {return 1;}

  RCLCPP_PUBLIC
  void
  add_to_wait_set(rcl_wait_set_t * wait_set) override;

  bool
  is_ready(rcl_wait_set_t * wait_set) override = 0;

  std::shared_ptr<void>
  take_data() override = 0;

  std::shared_ptr<void>
  take_data_by_entity_id(size_t id) override = 0;

  void
  execute(std::shared_ptr<void> & data) override = 0;

  RCLCPP_PUBLIC
  const char *
  get_action_name() const;

  RCLCPP_PUBLIC
  QoS
  get_actual_qos() const;

  /// Set a callback to be called when each new request arrives.
  /**
   * The callback receives a size_t which is the number of requests received
   * since the last time this callback was called.
   * Normally this is 1, but can be > 1 if requests were received before any
   * callback was set.
   *
   * The callback also receives an int identifier argument.
   * This is needed because a Waitable may be composed of several distinct entities,
   * such as subscriptions, services, etc. In this case they identify event types.
   * The application should provide a generic callback function that will be then
   * forwarded by the waitable to all of its entities.
   * Before forwarding, a different value for the identifier argument will be
   * bound to the function.
   * This implies that the provided callback can use the identifier to behave
   * differently depending on which entity triggered the waitable to become ready.
   *
   * Calling it again will clear any previously set callback.
   *
   * An exception will be thrown if the callback is not callable.
   *
   * This function is thread-safe.
   *
   * If you want more information available in the callback, like the subscription
   * or other information, you may use a lambda with captures or std::bind.
   *
   * \param[in] callback functor to be called when a new request is received.
   */
  void
  set_on_ready_callback(std::function<void(size_t, int)> callback) override
  {
    if (!callback) {
      throw std::invalid_argument(
              "The callback passed to set_on_ready_callback "
              "is not callable.");
    }

    std::lock_guard<std::recursive_mutex> lock(reentrant_mutex_);

    on_ready_callback_ = callback;

    for (auto& pair : event_type_to_unread_count_) {
      auto & event_type = pair.first;
      auto & unread_count = pair.second;
      if (unread_count) {
        on_ready_callback_(unread_count, static_cast<int>(event_type));
        unread_count = 0;
      }
    }
  }

  void
  clear_on_ready_callback() override
  {
    std::lock_guard<std::recursive_mutex> lock(reentrant_mutex_);
    on_ready_callback_ = nullptr;
  }

protected:
  rclcpp::GuardCondition gc_;
  std::string action_name_;
  QoS qos_profile_;
  std::recursive_mutex reentrant_mutex_;

  // Map the different action server event types to their unread count.
  std::unordered_map<EventType, size_t> event_type_to_unread_count_;

  // Generic events callback
  std::function<void(size_t, int)> on_ready_callback_{nullptr};

  // Invoke the callback to be called when the action server has a new event
  void
  invoke_on_ready_callback(EventType event_type)
  {
    std::lock_guard<std::recursive_mutex> lock(reentrant_mutex_);

    if (on_ready_callback_) {
      on_ready_callback_(1, static_cast<int>(event_type));
      return;
    }

    auto it = event_type_to_unread_count_.find(event_type);
    if (it != event_type_to_unread_count_.end()) {
        auto & unread_count = it->second;
        // Entry exists, increment unread counter
        unread_count++;
    } else {
        // Entry doesn't exist, create new with unread_count = 1
        event_type_to_unread_count_[event_type] = 1;
    }
  }
};

}  // namespace experimental
}  // namespace rclcpp

#endif  // RCLCPP__EXPERIMENTAL__ACTION_SERVER_INTRA_PROCESS_BASE_HPP_
