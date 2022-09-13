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

#ifndef RCLCPP__EXPERIMENTAL__ACTION_SERVER_INTRA_PROCESS_HPP_
#define RCLCPP__EXPERIMENTAL__ACTION_SERVER_INTRA_PROCESS_HPP_

#include <functional>
#include <future>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <variant>  // NOLINT, cpplint doesn't think this is a cpp std header

#include "rcutils/logging_macros.h"
#include "rclcpp/experimental/buffers/intra_process_buffer.hpp"
#include "rclcpp/experimental/create_intra_process_buffer.hpp"
#include "rclcpp/experimental/action_server_intra_process_base.hpp"

typedef struct rcl_action_server_depth_s
{
  size_t goal_service_depth;
  size_t cancel_service_depth;
  size_t result_service_depth;
} rcl_action_server_depth_t;

namespace rclcpp
{
namespace experimental
{

template<typename ActionT>
class ActionServerIntraProcess : public ActionServerIntraProcessBase
{
public:
  RCLCPP_SMART_PTR_DEFINITIONS(ActionServerIntraProcess)

  // Useful aliases for the action server data types
  using ResponseCallback = std::function<void (std::shared_ptr<void> response)>;

  using GoalRequest = typename ActionT::Impl::SendGoalService::Request;
  using GoalRequestSharedPtr = typename std::shared_ptr<GoalRequest>;
  using GoalRequestDataPair = typename std::pair<uint64_t, GoalRequestSharedPtr>;
  using GoalRequestDataPairSharedPtr = typename std::shared_ptr<GoalRequestDataPair>;
  using GoalRequestCallback = std::function<void (GoalRequestDataPairSharedPtr)>;

  using ResultRequest = typename ActionT::Impl::GetResultService::Request;
  using ResultRequestSharedPtr = typename std::shared_ptr<ResultRequest>;
  using ResultRequestDataPair = typename std::pair<uint64_t, ResultRequestSharedPtr>;
  using ResultRequestDataPairSharedPtr = typename std::shared_ptr<ResultRequestDataPair>;
  using ResultRequestCallback = std::function<void (ResultRequestDataPairSharedPtr)>;

  using CancelRequest = typename ActionT::Impl::CancelGoalService::Request;
  using CancelRequestSharedPtr = typename std::shared_ptr<CancelRequest>;
  using CancelRequestDataPair = typename std::pair<uint64_t, CancelRequestSharedPtr>;
  using CancelRequestDataPairSharedPtr = typename std::shared_ptr<CancelRequestDataPair>;
  using CancelGoalCallback = std::function<void (CancelRequestDataPairSharedPtr)>;

  ActionServerIntraProcess(
    rclcpp::Context::SharedPtr context,
    const std::string & action_name,
    const rcl_action_server_depth_t & qos_history,
    GoalRequestCallback goal_request_received_callback,
    CancelGoalCallback cancel_request_received_callback,
    ResultRequestCallback result_request_received_callback)
  : execute_goal_request_received_(goal_request_received_callback),
    execute_cancel_request_received_(cancel_request_received_callback),
    execute_result_request_received_(result_request_received_callback),
    ActionServerIntraProcessBase(
      context,
      action_name,
      QoS(qos_history.goal_service_depth))
  {
    // Create the intra-process buffers
    goal_request_buffer_ =
      rclcpp::experimental::create_service_intra_process_buffer<GoalRequestDataPair>(
      QoS(qos_history.goal_service_depth));

    result_request_buffer_ =
      rclcpp::experimental::create_service_intra_process_buffer<ResultRequestDataPair>(
      QoS(qos_history.result_service_depth));

    cancel_request_buffer_ =
      rclcpp::experimental::create_service_intra_process_buffer<CancelRequestDataPair>(
      QoS(qos_history.cancel_service_depth));
  }

  virtual ~ActionServerIntraProcess() = default;

  bool is_ready(rcl_wait_set_t * wait_set)
  {
    (void)wait_set;

    return goal_request_ready_ ||
           cancel_request_ready_ ||
           result_request_ready_ ||
           goal_expired_;
  }

  void store_ipc_action_goal_request(
    uint64_t ipc_action_client_id,
    GoalRequestSharedPtr goal_request)
  {
    goal_request_buffer_->add(
      std::make_pair(ipc_action_client_id, std::move(goal_request)));
    gc_.trigger();
    goal_request_ready_ = true;
    invoke_on_ready_callback(EventType::GoalRequest);
  }

  void store_ipc_action_result_request(
    uint64_t ipc_action_client_id,
    ResultRequestSharedPtr result_request)
  {
    result_request_buffer_->add(
      std::make_pair(ipc_action_client_id, std::move(result_request)));
    gc_.trigger();
    result_request_ready_ = true;
    invoke_on_ready_callback(EventType::ResultRequest);
  }

  void store_ipc_action_cancel_request(
    uint64_t ipc_action_client_id,
    CancelRequestSharedPtr cancel_request)
  {
    cancel_request_buffer_->add(
      std::make_pair(ipc_action_client_id, std::move(cancel_request)));
    gc_.trigger();
    cancel_request_ready_ = true;
    invoke_on_ready_callback(EventType::CancelGoal);
  }

  std::shared_ptr<void>
  take_data() override
  {
    if (goal_request_ready_) {
      auto data = std::make_shared<GoalRequestDataPair>(
        std::move(goal_request_buffer_->consume()));
      return std::static_pointer_cast<void>(data);
    } else if (cancel_request_ready_) {
      auto data = std::make_shared<CancelRequestDataPair>(
        std::move(cancel_request_buffer_->consume()));
      return std::static_pointer_cast<void>(data);
    } else if (result_request_ready_) {
      auto data = std::make_shared<ResultRequestDataPair>(
        std::move(result_request_buffer_->consume()));
      return std::static_pointer_cast<void>(data);
    } else if (goal_expired_) {
      return nullptr;
    } else {
      throw std::runtime_error("Taking data from action server but nothing is ready");
    }
    return nullptr;
  }

  std::shared_ptr<void>
  take_data_by_entity_id(size_t id) override
  {
    // Mark as ready the event type from which we want to take data
    switch (static_cast<EventType>(id)) {
      case EventType::GoalRequest:
        goal_request_ready_ = true;
        break;
      case EventType::CancelGoal:
        cancel_request_ready_ = true;
        break;
      case EventType::ResultRequest:
        result_request_ready_ = true;
        break;
    }

    return take_data();
  }

  void execute(std::shared_ptr<void> & data)
  {
    if (!data && !goal_expired_) {
      throw std::runtime_error("'data' is empty");
    }

    if (goal_request_ready_) {
      goal_request_ready_ = false;
      auto goal_request_data = std::static_pointer_cast<GoalRequestDataPair>(data);
      execute_goal_request_received_(std::move(goal_request_data));
    } else if (cancel_request_ready_) {
      cancel_request_ready_ = false;
      auto cancel_goal_data = std::static_pointer_cast<CancelRequestDataPair>(data);
      execute_cancel_request_received_(std::move(cancel_goal_data));
    } else if (result_request_ready_) {
      result_request_ready_ = false;
      auto result_request_data = std::static_pointer_cast<ResultRequestDataPair>(data);
      execute_result_request_received_(std::move(result_request_data));
    } else if (goal_expired_) {
      // TODO(mauropasse): Handle goal expired case
      // execute_check_expired_goals();
    } else {
      throw std::runtime_error("Executing action server but nothing is ready");
    }
  }

protected:
  // Create one buffer per type of client request
  typename rclcpp::experimental::buffers::ServiceIntraProcessBuffer<
    GoalRequestDataPair>::UniquePtr goal_request_buffer_;

  typename rclcpp::experimental::buffers::ServiceIntraProcessBuffer<
    ResultRequestDataPair>::UniquePtr result_request_buffer_;

  typename rclcpp::experimental::buffers::ServiceIntraProcessBuffer<
    CancelRequestDataPair>::UniquePtr cancel_request_buffer_;

  std::atomic<bool> goal_request_ready_{false};
  std::atomic<bool> cancel_request_ready_{false};
  std::atomic<bool> result_request_ready_{false};
  std::atomic<bool> goal_expired_{false};

  GoalRequestCallback execute_goal_request_received_;
  CancelGoalCallback execute_cancel_request_received_;
  ResultRequestCallback execute_result_request_received_;
};

}  // namespace experimental
}  // namespace rclcpp

#endif  // RCLCPP__EXPERIMENTAL__ACTION_SERVER_INTRA_PROCESS_HPP_
