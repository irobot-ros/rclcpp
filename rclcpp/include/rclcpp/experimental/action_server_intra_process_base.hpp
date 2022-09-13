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
  get_number_of_ready_guard_conditions() {return 1;}

  RCLCPP_PUBLIC
  void
  add_to_wait_set(rcl_wait_set_t * wait_set);

  virtual bool
  is_ready(rcl_wait_set_t * wait_set) = 0;

  virtual
  std::shared_ptr<void>
  take_data() = 0;

  virtual
  std::shared_ptr<void>
  take_data_by_entity_id(size_t id) = 0;

  virtual void
  execute(std::shared_ptr<void> & data) = 0;

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

    set_callback_to_event_type(EventType::GoalRequest, callback);
    set_callback_to_event_type(EventType::CancelGoal, callback);
    set_callback_to_event_type(EventType::ResultRequest, callback);
  }

  void
  clear_on_ready_callback() override
  {
    std::lock_guard<std::recursive_mutex> lock(reentrant_mutex_);
    event_type_to_on_ready_callback_.clear();
  }

protected:
  std::recursive_mutex reentrant_mutex_;
  rclcpp::GuardCondition gc_;

  // Action server on ready callbacks and unread count.
  // These callbacks can be set by the user to be notified about new events
  // on the action server like a new goal request, result request or a cancel goal request.
  // These events have a counter associated with them, counting the amount of events
  // that happened before having assigned a callback for them.
  using EventTypeOnReadyCallback = std::function<void (size_t)>;
  using CallbackUnreadCountPair = std::pair<EventTypeOnReadyCallback, size_t>;

  // Map the different action server event types to their callbacks and unread count.
  std::unordered_map<EventType, CallbackUnreadCountPair> event_type_to_on_ready_callback_;

  // Invoke the callback to be called when the action server has a new event
  void
  invoke_on_ready_callback(EventType event_type)
  {
    std::lock_guard<std::recursive_mutex> lock(reentrant_mutex_);

    // Search for a callback for this event type
    auto it = event_type_to_on_ready_callback_.find(event_type);

    if (it != event_type_to_on_ready_callback_.end()) {
      auto & on_ready_callback = it->second.first;
      // If there's a callback associated with this event type, call it
      if (on_ready_callback) {
        on_ready_callback(1);
      } else {
        // We don't have a callback for this event type yet,
        // increase its event counter.
        auto & event_type_unread_count = it->second.second;
        event_type_unread_count++;
      }
    } else {
      // No entries found for this event type, create one
      // with an emtpy callback and one unread event.
      event_type_to_on_ready_callback_.emplace(event_type, std::make_pair(nullptr, 1));
    }
  }

private:
  std::string action_name_;
  QoS qos_profile_;

  void set_callback_to_event_type(
    EventType event_type,
    std::function<void(size_t, int)> callback)
  {
    auto new_callback = create_event_type_callback(callback, event_type);

    std::lock_guard<std::recursive_mutex> lock(reentrant_mutex_);

    // Check if we have already an entry for this event type
    auto it = event_type_to_on_ready_callback_.find(event_type);

    if (it != event_type_to_on_ready_callback_.end()) {
      // We have an entry for this event type, check how many
      // events of this event type happened so far.
      auto & event_type_unread_count = it->second.second;
      if (event_type_unread_count) {
        new_callback(event_type_unread_count);
      }
      event_type_unread_count = 0;
      // Set the new callback for this event type
      auto & event_type_on_ready_callback = it->second.first;
      event_type_on_ready_callback = new_callback;
    } else {
      // We had no entries for this event type, create one
      // with the new callback and zero as unread count.
      event_type_to_on_ready_callback_.emplace(event_type, std::make_pair(new_callback, 0));
    }
  }

  std::function<void(size_t)>
  create_event_type_callback(
    std::function<void(size_t, int)> callback,
    EventType event_type)
  {
    // Note: we bind the int identifier argument to this waitable's entity types
    auto new_callback =
      [callback, event_type, this](size_t number_of_events) {
        try {
          callback(number_of_events, static_cast<int>(event_type));
        } catch (const std::exception & exception) {
          RCLCPP_ERROR_STREAM(
            rclcpp::get_logger("rclcpp_action"),
            "rclcpp::experimental::ActionServerIntraProcessBase@" << this <<
              " caught " << rmw::impl::cpp::demangle(exception) <<
              " exception in user-provided callback for the 'on ready' callback: " <<
              exception.what());
        } catch (...) {
          RCLCPP_ERROR_STREAM(
            rclcpp::get_logger("rclcpp_action"),
            "rclcpp::experimental::ActionServerIntraProcessBase@" << this <<
              " caught unhandled exception in user-provided callback " <<
              "for the 'on ready' callback");
        }
      };

    return new_callback;
  }
};

}  // namespace experimental
}  // namespace rclcpp

#endif  // RCLCPP__EXPERIMENTAL__ACTION_SERVER_INTRA_PROCESS_BASE_HPP_
