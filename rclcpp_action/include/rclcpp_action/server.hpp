// Copyright 2018 Open Source Robotics Foundation, Inc.
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

#ifndef RCLCPP_ACTION__SERVER_HPP_
#define RCLCPP_ACTION__SERVER_HPP_

#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>

// Check what I need
#include <action_msgs/msg/goal_status_array.hpp>
#include <action_msgs/srv/cancel_goal.hpp>
#include <rclcpp/experimental/action_server_intra_process.hpp>
#include <rclcpp/intra_process_setting.hpp>

#include "rcl/event_callback.h"
#include "rcl_action/action_server.h"
#include "rosidl_runtime_c/action_type_support_struct.h"
#include "rosidl_typesupport_cpp/action_type_support.hpp"
#include "rclcpp/node_interfaces/node_base_interface.hpp"
#include "rclcpp/node_interfaces/node_clock_interface.hpp"
#include "rclcpp/node_interfaces/node_logging_interface.hpp"
#include "rclcpp/waitable.hpp"

#include "rclcpp_action/visibility_control.hpp"
#include "rclcpp_action/server_goal_handle.hpp"
#include "rclcpp_action/types.hpp"

namespace rclcpp_action
{
// Forward declaration
class ServerBaseImpl;

/// A response returned by an action server callback when a goal is requested.
enum class GoalResponse : int8_t
{
  /// The goal is rejected and will not be executed.
  REJECT = 1,
  /// The server accepts the goal, and is going to begin execution immediately.
  ACCEPT_AND_EXECUTE = 2,
  /// The server accepts the goal, and is going to execute it later.
  ACCEPT_AND_DEFER = 3,
};

/// A response returned by an action server callback when a goal has been asked to be canceled.
enum class CancelResponse : int8_t
{
  /// The server will not try to cancel the goal.
  REJECT = 1,
  /// The server has agreed to try to cancel the goal.
  ACCEPT = 2,
};

/// Base Action Server implementation
/// \internal
/**
 * This class should not be used directly by users writing an action server.
 * Instead users should use `rclcpp_action::Server`.
 *
 * Internally, this class is responsible for interfacing with the `rcl_action` API.
 */
class ServerBase : public rclcpp::Waitable
{
public:
  /// Enum to identify entities belonging to the action server
  enum class EntityType : std::size_t
  {
    GoalService,
    ResultService,
    CancelService,
  };

  RCLCPP_ACTION_PUBLIC
  virtual ~ServerBase();

  // -------------
  // Waitables API

  /// Return the number of subscriptions used to implement an action server
  /// \internal
  RCLCPP_ACTION_PUBLIC
  size_t
  get_number_of_ready_subscriptions() override;

  /// Return the number of timers used to implement an action server
  /// \internal
  RCLCPP_ACTION_PUBLIC
  size_t
  get_number_of_ready_timers() override;

  /// Return the number of service clients used to implement an action server
  /// \internal
  RCLCPP_ACTION_PUBLIC
  size_t
  get_number_of_ready_clients() override;

  /// Return the number of service servers used to implement an action server
  /// \internal
  RCLCPP_ACTION_PUBLIC
  size_t
  get_number_of_ready_services() override;

  /// Return the number of guard conditions used to implement an action server
  /// \internal
  RCLCPP_ACTION_PUBLIC
  size_t
  get_number_of_ready_guard_conditions() override;

  /// Add all entities to a wait set.
  /// \internal
  RCLCPP_ACTION_PUBLIC
  void
  add_to_wait_set(rcl_wait_set_t * wait_set) override;

  /// Return true if any entity belonging to the action server is ready to be executed.
  /// \internal
  RCLCPP_ACTION_PUBLIC
  bool
  is_ready(rcl_wait_set_t *) override;

  RCLCPP_ACTION_PUBLIC
  std::shared_ptr<void>
  take_data() override;

  RCLCPP_ACTION_PUBLIC
  std::shared_ptr<void>
  take_data_by_entity_id(size_t id) override;

  /// Act on entities in the wait set which are ready to be acted upon.
  /// \internal
  RCLCPP_ACTION_PUBLIC
  void
  execute(std::shared_ptr<void> & data) override;

  /// \internal
  /// Set a callback to be called when action server entities have an event
  /**
   * The callback receives a size_t which is the number of messages received
   * since the last time this callback was called.
   * Normally this is 1, but can be > 1 if messages were received before any
   * callback was set.
   *
   * The callback also receives an int identifier argument, which identifies
   * the action server entity which is ready.
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
   * \param[in] callback functor to be called when a new message is received.
   */
  RCLCPP_ACTION_PUBLIC
  void
  set_on_ready_callback(std::function<void(size_t, int)> callback) override;

  /// Unset the callback to be called whenever the waitable becomes ready.
  RCLCPP_ACTION_PUBLIC
  void
  clear_on_ready_callback() override;

  using IntraProcessManagerWeakPtr =
    std::weak_ptr<rclcpp::experimental::IntraProcessManager>;

  /// Implementation detail.
  RCLCPP_PUBLIC
  void
  setup_intra_process(
    uint64_t ipc_actionserver_id,
    IntraProcessManagerWeakPtr weak_ipm);

  /// Return the waitable for intra-process
  /**
   * \return the waitable sharedpointer for intra-process, or nullptr if intra-process is not setup.
   * \throws std::runtime_error if the intra process manager is destroyed
   */
  RCLCPP_PUBLIC
  rclcpp::Waitable::SharedPtr
  get_intra_process_waitable();

  std::shared_ptr<rclcpp::experimental::IntraProcessManager>
  lock_intra_process_manager()
  {
    auto ipm = weak_ipm_.lock();
    if (!ipm) {
      throw std::runtime_error(
              "Intra-process manager already destroyed");
    }
    return ipm;
  }

  // End Waitables API
  // -----------------

protected:
  RCLCPP_ACTION_PUBLIC
  ServerBase(
    rclcpp::node_interfaces::NodeBaseInterface::SharedPtr node_base,
    rclcpp::node_interfaces::NodeClockInterface::SharedPtr node_clock,
    rclcpp::node_interfaces::NodeLoggingInterface::SharedPtr node_logging,
    const std::string & name,
    const rosidl_action_type_support_t * type_support,
    const rcl_action_server_options_t & options);

  // -----------------------------------------------------
  // API for communication between ServerBase and Server<>

  // ServerBase will call this function when a goal request is received.
  // The subclass should convert to the real type and call a user's callback.
  /// \internal
  RCLCPP_ACTION_PUBLIC
  virtual
  std::pair<GoalResponse, std::shared_ptr<void>>
  call_handle_goal_callback(GoalUUID &, std::shared_ptr<void> request) = 0;

  // ServerBase will determine which goal ids are being cancelled, and then call this function for
  // each goal id.
  // The subclass should look up a goal handle and call the user's callback.
  /// \internal
  RCLCPP_ACTION_PUBLIC
  virtual
  CancelResponse
  call_handle_cancel_callback(const GoalUUID & uuid) = 0;

  /// Given a goal request message, return the UUID contained within.
  /// \internal
  RCLCPP_ACTION_PUBLIC
  virtual
  GoalUUID
  get_goal_id_from_goal_request(void * message) = 0;

  /// Create an empty goal request message so it can be taken from a lower layer.
  /// \internal
  RCLCPP_ACTION_PUBLIC
  virtual
  std::shared_ptr<void>
  create_goal_request() = 0;

  /// Call user callback to inform them a goal has been accepted.
  /// \internal
  RCLCPP_ACTION_PUBLIC
  virtual
  void
  call_goal_accepted_callback(
    std::shared_ptr<rcl_action_goal_handle_t> rcl_goal_handle,
    GoalUUID uuid, std::shared_ptr<void> goal_request_message) = 0;

  /// Given a result request message, return the UUID contained within.
  /// \internal
  RCLCPP_ACTION_PUBLIC
  virtual
  GoalUUID
  get_goal_id_from_result_request(void * message) = 0;

  /// Create an empty goal request message so it can be taken from a lower layer.
  /// \internal
  RCLCPP_ACTION_PUBLIC
  virtual
  std::shared_ptr<void>
  create_result_request() = 0;

  /// Create an empty goal result message so it can be sent as a reply in a lower layer
  /// \internal
  RCLCPP_ACTION_PUBLIC
  virtual
  std::shared_ptr<void>
  create_result_response(decltype(action_msgs::msg::GoalStatus::status) status) = 0;

  /// \internal
  RCLCPP_ACTION_PUBLIC
  std::shared_ptr<action_msgs::msg::GoalStatusArray>
  get_status_array();

  /// \internal
  RCLCPP_ACTION_PUBLIC
  void
  publish_status();

  /// \internal
  RCLCPP_ACTION_PUBLIC
  void
  notify_goal_terminal_state();

  /// \internal
  RCLCPP_ACTION_PUBLIC
  void
  publish_result(const GoalUUID & uuid, std::shared_ptr<void> result_msg);

  /// \internal
  RCLCPP_ACTION_PUBLIC
  void
  publish_feedback(std::shared_ptr<void> feedback_msg);

  /// Temporary workaround
  /// \internal
  RCLCPP_ACTION_PUBLIC
  std::shared_ptr<rcl_action_goal_handle_t>
  get_rcl_action_goal_handle(
    rcl_action_goal_info_t goal_info,
    GoalUUID uuid);

  /// Addition
  /// \internal
  RCLCPP_ACTION_PUBLIC
  std::shared_ptr<action_msgs::srv::CancelGoal::Response>
  process_cancel_request(rcl_action_cancel_request_t & cancel_request);

  // End API for communication between ServerBase and Server<>
  // ---------------------------------------------------------

private:
  /// Handle a request to add a new goal to the server
  /// \internal
  RCLCPP_ACTION_PUBLIC
  void
  execute_goal_request_received(std::shared_ptr<void> & data);

  /// Handle a request to cancel goals on the server
  /// \internal
  RCLCPP_ACTION_PUBLIC
  void
  execute_cancel_request_received(std::shared_ptr<void> & data);

  /// Handle a request to get the result of an action
  /// \internal
  RCLCPP_ACTION_PUBLIC
  void
  execute_result_request_received(std::shared_ptr<void> & data);

  /// Handle a timeout indicating a completed goal should be forgotten by the server
  /// \internal
  RCLCPP_ACTION_PUBLIC
  void
  execute_check_expired_goals();

  /// Private implementation
  /// \internal
  std::unique_ptr<ServerBaseImpl> pimpl_;

  /// Set a std::function callback to be called when the specified entity is ready
  RCLCPP_ACTION_PUBLIC
  void
  set_callback_to_entity(
    EntityType entity_type,
    std::function<void(size_t, int)> callback);

protected:
  // Mutex to protect the callbacks storage.
  std::recursive_mutex listener_mutex_;
  // Storage for std::function callbacks to keep them in scope
  std::unordered_map<EntityType, std::function<void(size_t)>> entity_type_to_on_ready_callback_;

  /// Set a callback to be called when the specified entity is ready
  RCLCPP_ACTION_PUBLIC
  void
  set_on_ready_callback(
    EntityType entity_type,
    rcl_event_callback_t callback,
    const void * user_data);

  bool on_ready_callback_set_{false};

  // Intra-process action server data fields
  bool use_intra_process_{false};
  IntraProcessManagerWeakPtr weak_ipm_;
  uint64_t ipc_action_server_id_;
};

/// Action Server
/**
 * This class creates an action server.
 *
 * Create an instance of this server using `rclcpp_action::create_server()`.
 *
 * Internally, this class is responsible for:
 *  - coverting between the C++ action type and generic types for `rclcpp_action::ServerBase`, and
 *  - calling user callbacks.
 */
template<typename ActionT>
class Server : public ServerBase, public std::enable_shared_from_this<Server<ActionT>>
{
public:
  RCLCPP_SMART_PTR_DEFINITIONS_NOT_COPYABLE(Server)

  /// Signature of a callback that accepts or rejects goal requests.
  using GoalCallback = std::function<GoalResponse(
        const GoalUUID &, std::shared_ptr<const typename ActionT::Goal>)>;
  /// Signature of a callback that accepts or rejects requests to cancel a goal.
  using CancelCallback = std::function<CancelResponse(std::shared_ptr<ServerGoalHandle<ActionT>>)>;
  /// Signature of a callback that is used to notify when the goal has been accepted.
  using AcceptedCallback = std::function<void (std::shared_ptr<ServerGoalHandle<ActionT>>)>;

  using ResponseCallback = std::function<void (std::shared_ptr<void> response)>;

  using GoalRequest = typename ActionT::Impl::SendGoalService::Request;
  using GoalRequestSharedPtr = typename std::shared_ptr<GoalRequest>;
  using GoalRequestDataPair = typename std::pair<uint64_t, GoalRequestSharedPtr>;
  using GoalRequestDataPairSharedPtr = typename std::shared_ptr<GoalRequestDataPair>;

  using ResultRequest = typename ActionT::Impl::GetResultService::Request;
  using ResultRequestSharedPtr = typename std::shared_ptr<ResultRequest>;
  using ResultRequestDataPair = typename std::pair<uint64_t, ResultRequestSharedPtr>;
  using ResultRequestDataPairSharedPtr = typename std::shared_ptr<ResultRequestDataPair>;

  using CancelRequest = typename ActionT::Impl::CancelGoalService::Request;
  using CancelRequestSharedPtr = typename std::shared_ptr<CancelRequest>;
  using CancelRequestDataPair = typename std::pair<uint64_t, CancelRequestSharedPtr>;
  using CancelRequestDataPairSharedPtr = typename std::shared_ptr<CancelRequestDataPair>;

  using ResultResponse = typename ActionT::Impl::GetResultService::Response;
  using ResultResponseSharedPtr = typename std::shared_ptr<ResultResponse>;

  /// Construct an action server.
  /**
   * This constructs an action server, but it will not work until it has been added to a node.
   * Use `rclcpp_action::create_server()` to both construct and add to a node.
   *
   * Three callbacks must be provided:
   *  - one to accept or reject goals sent to the server,
   *  - one to accept or reject requests to cancel a goal,
   *  - one to receive a goal handle after a goal has been accepted.
   * All callbacks must be non-blocking.
   * The result of a goal should be set using methods on `rclcpp_action::ServerGoalHandle`.
   *
   * \param[in] node_base a pointer to the base interface of a node.
   * \param[in] node_clock a pointer to an interface that allows getting a node's clock.
   * \param[in] node_logging a pointer to an interface that allows getting a node's logger.
   * \param[in] name the name of an action.
   *  The same name and type must be used by both the action client and action server to
   *  communicate.
   * \param[in] options Options to pass to the underlying `rcl_action_server_t`.
   * \param[in] handle_goal a callback that decides if a goal should be accepted or rejected.
   * \param[in] handle_cancel a callback that decides if a goal should be attemted to be canceled.
   *  The return from this callback only indicates if the server will try to cancel a goal.
   *  It does not indicate if the goal was actually canceled.
   * \param[in] handle_accepted a callback that is called to give the user a handle to the goal.
   *  execution.
   */
  Server(
    rclcpp::node_interfaces::NodeBaseInterface::SharedPtr node_base,
    rclcpp::node_interfaces::NodeClockInterface::SharedPtr node_clock,
    rclcpp::node_interfaces::NodeLoggingInterface::SharedPtr node_logging,
    const std::string & name,
    const rcl_action_server_options_t & options,
    GoalCallback handle_goal,
    CancelCallback handle_cancel,
    AcceptedCallback handle_accepted,
    rclcpp::IntraProcessSetting ipc_setting = rclcpp::IntraProcessSetting::NodeDefault)
  : ServerBase(
      node_base,
      node_clock,
      node_logging,
      name,
      rosidl_typesupport_cpp::get_action_type_support_handle<ActionT>(),
      options),
    handle_goal_(handle_goal),
    handle_cancel_(handle_cancel),
    handle_accepted_(handle_accepted)
  {
    // Setup intra process if requested.
    if (rclcpp::detail::resolve_use_intra_process(ipc_setting, *node_base)) {
      create_intra_process_action_server(node_base, name, options);
    }
  }

  virtual ~Server()
  {
    if (!use_intra_process_) {
      return;
    }
    auto ipm = lock_intra_process_manager();
    ipm->remove_action_server(ipc_action_server_id_);
  }

protected:
  void
  ipc_execute_goal_request_received(GoalRequestDataPairSharedPtr data)
  {
    uint64_t intra_process_action_client_id = data->first;
    GoalRequestSharedPtr goal_request = data->second;

    rcl_action_goal_info_t goal_info = rcl_action_get_zero_initialized_goal_info();
    GoalUUID uuid = get_goal_id_from_goal_request(goal_request.get());
    convert(uuid, &goal_info);

    // Call user's callback, getting the user's response and a ros message to send back
    auto response_pair = call_handle_goal_callback(uuid, goal_request);

    using Response = typename ActionT::Impl::SendGoalService::Response;
    auto goal_response = std::static_pointer_cast<Response>(response_pair.second);

    auto ipm = lock_intra_process_manager();

    ipm->template intra_process_action_send_goal_response<ActionT>(
      intra_process_action_client_id,
      std::move(goal_response),
      std::hash<GoalUUID>()(uuid));

    const auto user_response = response_pair.first;

    // if goal is accepted, create a goal handle, and store it
    if (GoalResponse::ACCEPT_AND_EXECUTE == user_response ||
      GoalResponse::ACCEPT_AND_DEFER == user_response)
    {
      RCLCPP_DEBUG(
        rclcpp::get_logger("rclcpp_action"), "Accepted goal %s", to_string(uuid).c_str());

      // Hack: Get rcl goal handle for simplicity
      auto handle = this->get_rcl_action_goal_handle(goal_info, uuid);

      if (user_response == GoalResponse::ACCEPT_AND_EXECUTE) {
        // Change status to executing
        rcl_ret_t ret = rcl_action_update_goal_state(handle.get(), GOAL_EVENT_EXECUTE);
        if (RCL_RET_OK != ret) {
          rclcpp::exceptions::throw_from_rcl_error(ret);
        }
      }

      // publish status since a goal's state has changed (was accepted or has begun execution)
      // This part would be the IPC version of publish_status();
      auto status_msg = this->get_status_array();

      ipm->template intra_process_action_publish_status<ActionT>(
        intra_process_action_client_id,
        std::move(status_msg));

      call_goal_accepted_callback(handle, uuid, std::static_pointer_cast<void>(goal_request));
    }
  }

  void
  ipc_execute_cancel_request_received(CancelRequestDataPairSharedPtr data)
  {
    uint64_t intra_process_action_client_id = data->first;
    CancelRequestSharedPtr request = data->second;

    auto ipm = lock_intra_process_manager();

    // Convert c++ message to C message
    rcl_action_cancel_request_t cancel_request = rcl_action_get_zero_initialized_cancel_request();
    convert(request->goal_info.goal_id.uuid, &cancel_request.goal_info);
    cancel_request.goal_info.stamp.sec = request->goal_info.stamp.sec;
    cancel_request.goal_info.stamp.nanosec = request->goal_info.stamp.nanosec;

    auto response = process_cancel_request(cancel_request);

    if (!response->goals_canceling.empty()) {
      // at least one goal state changed, publish a new status message
      auto status_msg = this->get_status_array();

      ipm->template intra_process_action_publish_status<ActionT>(
        intra_process_action_client_id,
        std::move(status_msg));
    }

    GoalUUID uuid = request->goal_info.goal_id.uuid;

    ipm->template intra_process_action_send_cancel_response<ActionT>(
      intra_process_action_client_id,
      std::move(response),
      std::hash<GoalUUID>()(uuid));
  }

  ResultResponseSharedPtr
  get_result_response(size_t goal_id)
  {
    auto it = ipc_goal_results_.find(goal_id);
    if (it != ipc_goal_results_.end()) {
      auto result = it->second;
      ipc_goal_results_.erase(it);
      return result;
    }

    return nullptr;
  }

  void
  ipc_execute_result_request_received(ResultRequestDataPairSharedPtr data)
  {
    uint64_t intra_process_action_client_id = data->first;
    ResultRequestSharedPtr result_request = data->second;

    // check if the goal exists. How?
    GoalUUID uuid = get_goal_id_from_result_request(result_request.get());
    size_t hashed_uuid = std::hash<GoalUUID>()(uuid);

    std::lock_guard<std::recursive_mutex> lock(goal_handles_mutex_);

    ResultResponseSharedPtr result_response = get_result_response(hashed_uuid);

    // Check if a result is already available. If not, it will
    // be sent when ready in the on_terminal_state callback below.
    if (result_response) {
      // Send the result now
      auto ipm = lock_intra_process_manager();

      ipm->template intra_process_action_send_result_response<ActionT>(
        intra_process_action_client_id,
        std::move(result_response),
        hashed_uuid);
    } else {
      goals_result_requested_.push_back(hashed_uuid);
    }
  }

  bool
  client_requested_response(size_t goal_id, ResultResponseSharedPtr typed_result)
  {
    std::lock_guard<std::recursive_mutex> lock(goal_handles_mutex_);

    for (auto it = goals_result_requested_.begin(); it != goals_result_requested_.end(); ++it) {
      if (*it == goal_id) {
        goals_result_requested_.erase(it);
        return true;
      }
    }

    // The cliend didn't ask fo response yet, so store it to send later
    ipc_goal_results_[goal_id] = typed_result;
    return false;
  }

  bool
  ipc_on_terminal_state(const GoalUUID & goal_uuid, std::shared_ptr<void> result_message)
  {
    auto ipm = lock_intra_process_manager();

    size_t hashed_uuid = std::hash<GoalUUID>()(goal_uuid);

    uint64_t ipc_action_client_id = ipm->get_action_client_id_from_goal_uuid(hashed_uuid);

    if (ipc_action_client_id) {
      auto typed_result = std::static_pointer_cast<ResultResponse>(result_message);

      if (client_requested_response(hashed_uuid, typed_result)) {
        ipm->template intra_process_action_send_result_response<ActionT>(
          ipc_action_client_id,
          std::move(typed_result),
          hashed_uuid);

        auto status_msg = this->get_status_array();

        ipm->template intra_process_action_publish_status<ActionT>(
          ipc_action_client_id,
          std::move(status_msg));

        ipm->remove_intra_process_action_client_goal_uuid(hashed_uuid);
      }

      return false;
    }

    RCLCPP_DEBUG(
      rclcpp::get_logger("rclcpp_action"),
      "Action server can't send result response, missing IPC Action client: %s. "
      "Will do inter-process publish",
      this->action_name_.c_str());
    return true;
  }

  bool
  ipc_on_executing(const GoalUUID & goal_uuid)
  {
    auto ipm = lock_intra_process_manager();

    size_t hashed_uuid = std::hash<GoalUUID>()(goal_uuid);

    uint64_t ipc_action_client_id = ipm->get_action_client_id_from_goal_uuid(hashed_uuid);

    if (ipc_action_client_id) {
      // This part would be the IPC version of publish_status();
      auto status_msg = this->get_status_array();

      ipm->template intra_process_action_publish_status<ActionT>(
        ipc_action_client_id,
        std::move(status_msg));
      return false;
    }

    return true;
  }


  bool
  ipc_publish_feedback(typename ActionT::Impl::FeedbackMessage::SharedPtr feedback_msg)
  {
    auto ipm = lock_intra_process_manager();

    size_t hashed_uuid = std::hash<GoalUUID>()(feedback_msg->goal_id.uuid);

    uint64_t ipc_action_client_id = ipm->get_action_client_id_from_goal_uuid(hashed_uuid);

    if (ipc_action_client_id) {
        ipm->template intra_process_action_publish_feedback<ActionT>(
          ipc_action_client_id,
          std::move(feedback_msg));
      return false;
    }

    return true;
  }

  // -----------------------------------------------------
  // API for communication between ServerBase and Server<>

  /// \internal
  void
  call_goal_accepted_callback(
    std::shared_ptr<rcl_action_goal_handle_t> rcl_goal_handle,
    GoalUUID uuid, std::shared_ptr<void> goal_request_message) override
  {
    std::shared_ptr<ServerGoalHandle<ActionT>> goal_handle;
    std::weak_ptr<Server<ActionT>> weak_this = this->shared_from_this();

    // Define callbacks for the ServerGoalHandle, which will be called from the user APP when
    // for example goal_handle->succeed(result) or goal_handle->publish_feedback(feedback);
    auto on_terminal_state =
      [weak_this](const GoalUUID & goal_uuid, std::shared_ptr<void> result_message)
      {
        std::shared_ptr<Server<ActionT>> shared_this = weak_this.lock();
        if (!shared_this) {
          return;
        }

        bool send_inter_process_needed = true;
        if (shared_this->use_intra_process_) {
          send_inter_process_needed = shared_this->ipc_on_terminal_state(goal_uuid, result_message);
        }
        if (send_inter_process_needed) {
          // Send result message to anyone that asked
          shared_this->publish_result(goal_uuid, result_message);
          // Publish a status message any time a goal handle changes state
          shared_this->publish_status();
        }

        // notify base so it can recalculate the expired goal timer
        shared_this->notify_goal_terminal_state();

        // Delete data now (ServerBase and rcl_action_server_t keep data until goal handle expires)
        std::lock_guard<std::recursive_mutex> lock(shared_this->goal_handles_mutex_);
        shared_this->goal_handles_.erase(goal_uuid);
      };

    auto on_executing =
      [weak_this](const GoalUUID & goal_uuid)
      {
        std::shared_ptr<Server<ActionT>> shared_this = weak_this.lock();
        if (!shared_this) {
          return;
        }

        bool send_inter_process_needed = true;
        if (shared_this->use_intra_process_) {
          send_inter_process_needed = shared_this->ipc_on_executing(goal_uuid);
        }
        if (send_inter_process_needed) {
          shared_this->publish_status();
        }
      };

    using FeedbackMsg = typename ActionT::Impl::FeedbackMessage::SharedPtr;

    auto publish_feedback =
      [weak_this](FeedbackMsg feedback_msg)
      {
        std::shared_ptr<Server<ActionT>> shared_this = weak_this.lock();
        if (!shared_this) {
          return;
        }

        bool send_inter_process_needed = true;
        if (shared_this->use_intra_process_) {
          send_inter_process_needed = shared_this->ipc_publish_feedback(feedback_msg);
        }
        if (send_inter_process_needed) {
          shared_this->publish_feedback(std::static_pointer_cast<void>(feedback_msg));
        }
      };

    auto request = std::static_pointer_cast<
      const typename ActionT::Impl::SendGoalService::Request>(goal_request_message);
    auto goal = std::shared_ptr<const typename ActionT::Goal>(request, &request->goal);
    goal_handle.reset(
      new ServerGoalHandle<ActionT>(
        rcl_goal_handle, uuid, goal, on_terminal_state, on_executing, publish_feedback));
    {
      std::lock_guard<std::recursive_mutex> lock(goal_handles_mutex_);
      goal_handles_[uuid] = goal_handle;
    }
    handle_accepted_(goal_handle);
  }

  /// \internal
  std::pair<GoalResponse, std::shared_ptr<void>>
  call_handle_goal_callback(GoalUUID & uuid, std::shared_ptr<void> message) override
  {
    auto request = std::static_pointer_cast<
      typename ActionT::Impl::SendGoalService::Request>(message);
    auto goal = std::shared_ptr<typename ActionT::Goal>(request, &request->goal);
    GoalResponse user_response = handle_goal_(uuid, goal);

    auto ros_response = std::make_shared<typename ActionT::Impl::SendGoalService::Response>();
    ros_response->accepted = GoalResponse::ACCEPT_AND_EXECUTE == user_response ||
      GoalResponse::ACCEPT_AND_DEFER == user_response;
    return std::make_pair(user_response, ros_response);
  }

  /// \internal
  CancelResponse
  call_handle_cancel_callback(const GoalUUID & uuid) override
  {
    std::shared_ptr<ServerGoalHandle<ActionT>> goal_handle;
    {
      std::lock_guard<std::recursive_mutex> lock(goal_handles_mutex_);
      auto element = goal_handles_.find(uuid);
      if (element != goal_handles_.end()) {
        goal_handle = element->second.lock();
      }
    }

    CancelResponse resp = CancelResponse::REJECT;
    if (goal_handle) {
      resp = handle_cancel_(goal_handle);
      if (CancelResponse::ACCEPT == resp) {
        try {
          goal_handle->_cancel_goal();
        } catch (const rclcpp::exceptions::RCLError & ex) {
          RCLCPP_DEBUG(
            rclcpp::get_logger("rclcpp_action"),
            "Failed to cancel goal in call_handle_cancel_callback: %s", ex.what());
          return CancelResponse::REJECT;
        }
      }
    }
    return resp;
  }

  /// \internal
  GoalUUID
  get_goal_id_from_goal_request(void * message) override
  {
    return
      static_cast<typename ActionT::Impl::SendGoalService::Request *>(message)->goal_id.uuid;
  }

  /// \internal
  std::shared_ptr<void>
  create_goal_request() override
  {
    return std::shared_ptr<void>(new typename ActionT::Impl::SendGoalService::Request());
  }

  /// \internal
  GoalUUID
  get_goal_id_from_result_request(void * message) override
  {
    return
      static_cast<typename ActionT::Impl::GetResultService::Request *>(message)->goal_id.uuid;
  }

  /// \internal
  std::shared_ptr<void>
  create_result_request() override
  {
    return std::shared_ptr<void>(new typename ActionT::Impl::GetResultService::Request());
  }

  /// \internal
  std::shared_ptr<void>
  create_result_response(decltype(action_msgs::msg::GoalStatus::status) status) override
  {
    auto result = std::make_shared<typename ActionT::Impl::GetResultService::Response>();
    result->status = status;
    return std::static_pointer_cast<void>(result);
  }

  // End API for communication between ServerBase and Server<>
  // ---------------------------------------------------------

private:
  GoalCallback handle_goal_;
  CancelCallback handle_cancel_;
  AcceptedCallback handle_accepted_;

  using GoalHandleWeakPtr = std::weak_ptr<ServerGoalHandle<ActionT>>;
  /// A map of goal id to goal handle weak pointers.
  /// This is used to provide a goal handle to handle_cancel.
  std::unordered_map<GoalUUID, GoalHandleWeakPtr> goal_handles_;
  std::recursive_mutex goal_handles_mutex_;
  std::string action_name_;

  // Declare the intra-process action server
  using ActionServerIntraProcessT = rclcpp::experimental::ActionServerIntraProcess<ActionT>;
  std::shared_ptr<ActionServerIntraProcessT> ipc_action_server_;
  bool use_intra_process_{false};

  // Map to store results until the client request it
  std::unordered_map<size_t, ResultResponseSharedPtr> ipc_goal_results_;
  // The goals for which the client has requested the result
  std::vector<size_t> goals_result_requested_;

  void
  create_intra_process_action_server(
    rclcpp::node_interfaces::NodeBaseInterface::SharedPtr node_base,
    const std::string & name,
    const rcl_action_server_options_t & options)
  {
    // Setup intra process if requested.
    auto keep_last = RMW_QOS_POLICY_HISTORY_KEEP_LAST;
    if (options.goal_service_qos.history != keep_last ||
      options.result_service_qos.history != keep_last ||
      options.cancel_service_qos.history != keep_last)
    {
      throw std::invalid_argument(
              "intraprocess communication allowed only with keep last history qos policy");
    }

    if (options.goal_service_qos.depth == 0 ||
      options.result_service_qos.depth == 0 ||
      options.cancel_service_qos.depth == 0)
    {
      throw std::invalid_argument(
              "intraprocess communication is not allowed with 0 depth qos policy");
    }

    auto durability_vol = RMW_QOS_POLICY_DURABILITY_VOLATILE;
    if (options.goal_service_qos.durability != durability_vol ||
      options.result_service_qos.durability != durability_vol ||
      options.cancel_service_qos.durability != durability_vol)
    {
      throw std::invalid_argument(
              "intraprocess communication allowed only with volatile durability");
    }

    rcl_action_server_depth_t qos_history;
    qos_history.goal_service_depth = options.goal_service_qos.depth;
    qos_history.result_service_depth = options.result_service_qos.depth;
    qos_history.cancel_service_depth = options.cancel_service_qos.depth;

    std::lock_guard<std::recursive_mutex> lock(goal_handles_mutex_);

    use_intra_process_ = true;

    action_name_ = node_base->resolve_topic_or_service_name(name, true);

    // Create a ActionClientIntraProcess which will be given
    // to the intra-process manager.
    auto context = node_base->get_context();
    ipc_action_server_ = std::make_shared<ActionServerIntraProcessT>(
      context, action_name_, qos_history,
      std::bind(&Server::ipc_execute_goal_request_received, this, std::placeholders::_1),
      std::bind(&Server::ipc_execute_cancel_request_received, this, std::placeholders::_1),
      std::bind(&Server::ipc_execute_result_request_received, this, std::placeholders::_1));

    // Add it to the intra process manager.
    using rclcpp::experimental::IntraProcessManager;
    auto ipm = context->get_sub_context<IntraProcessManager>();
    uint64_t ipc_action_client_id = ipm->add_intra_process_action_server(ipc_action_server_);
    this->setup_intra_process(ipc_action_client_id, ipm);
  }
};
}  // namespace rclcpp_action
#endif  // RCLCPP_ACTION__SERVER_HPP_
