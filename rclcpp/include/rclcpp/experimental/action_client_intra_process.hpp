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

#ifndef RCLCPP__EXPERIMENTAL__ACTION_CLIENT_INTRA_PROCESS_HPP_
#define RCLCPP__EXPERIMENTAL__ACTION_CLIENT_INTRA_PROCESS_HPP_

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
#include "rclcpp/experimental/action_client_intra_process_base.hpp"

typedef struct rcl_action_client_depth_s
{
  size_t goal_service_depth;
  size_t result_service_depth;
  size_t cancel_service_depth;
  size_t feedback_topic_depth;
  size_t status_topic_depth;
} rcl_action_client_depth_t;

namespace rclcpp
{
namespace experimental
{

template<typename ActionT>
class ActionClientIntraProcess : public ActionClientIntraProcessBase
{
public:
  RCLCPP_SMART_PTR_DEFINITIONS(ActionClientIntraProcess)

  // Useful aliases for the action client data types
  using ResponseCallback = std::function<void (std::shared_ptr<void>)>;

  // Aliases for the GoalResponse ring buffer
  using GoalResponse = typename ActionT::Impl::SendGoalService::Response;
  using GoalResponseSharedPtr = typename std::shared_ptr<GoalResponse>;
  using GoalResponseDataPair = typename std::pair<uint64_t, GoalResponseSharedPtr>;
  using GoalResponseVoidDataPair = typename std::pair<uint64_t, std::shared_ptr<void>>;
  using GoalResponsePairSharedPtr = typename std::shared_ptr<GoalResponseDataPair>;

  // Aliases for the ResultResponse ring buffer
  using ResultResponse = typename ActionT::Impl::GetResultService::Response;
  using ResultResponseSharedPtr = typename std::shared_ptr<ResultResponse>;
  using ResultResponseDataPair = typename std::pair<uint64_t, ResultResponseSharedPtr>;
  using ResultResponseVoidDataPair = typename std::pair<uint64_t, std::shared_ptr<void>>;
  using ResultResponsePairSharedPtr = typename std::shared_ptr<ResultResponseDataPair>;

  // Aliases for the CancelResponse ring buffer
  using CancelResponse = typename ActionT::Impl::CancelGoalService::Response;
  using CancelResponseSharedPtr = typename std::shared_ptr<CancelResponse>;
  using CancelResponseDataPair = typename std::pair<uint64_t, CancelResponseSharedPtr>;
  using CancelResponseVoidDataPair = typename std::pair<uint64_t, std::shared_ptr<void>>;
  using CancelResponsePairSharedPtr = typename std::shared_ptr<CancelResponseDataPair>;

  using FeedbackMessage = typename ActionT::Impl::FeedbackMessage;
  using FeedbackSharedPtr = typename std::shared_ptr<FeedbackMessage>;
  using GoalStatusSharedPtr = typename std::shared_ptr<void>;

  ActionClientIntraProcess(
    rclcpp::Context::SharedPtr context,
    const std::string & action_name,
    const rcl_action_client_depth_t & qos_history,
    ResponseCallback goal_status_callback,
    ResponseCallback feedback_callback,
    std::recursive_mutex & reentrant_mutex)
  : ActionClientIntraProcessBase(
      context,
      action_name,
      QoS(qos_history.goal_service_depth),
      reentrant_mutex)
  {
    // Create the intra-process buffers
    goal_response_buffer_ =
      rclcpp::experimental::create_service_intra_process_buffer<GoalResponsePairSharedPtr>(
      QoS(qos_history.goal_service_depth));

    result_response_buffer_ =
      rclcpp::experimental::create_service_intra_process_buffer<ResultResponsePairSharedPtr>(
      QoS(qos_history.result_service_depth));

    status_buffer_ =
      rclcpp::experimental::create_service_intra_process_buffer<GoalStatusSharedPtr>(
      QoS(qos_history.status_topic_depth));

    feedback_buffer_ =
      rclcpp::experimental::create_service_intra_process_buffer<FeedbackSharedPtr>(
      QoS(qos_history.feedback_topic_depth));

    cancel_response_buffer_ =
      rclcpp::experimental::create_service_intra_process_buffer<CancelResponsePairSharedPtr>(
      QoS(qos_history.cancel_service_depth));

    set_response_callback_to_event_type(EventType::FeedbackReady, feedback_callback);
    set_response_callback_to_event_type(EventType::StatusReady, goal_status_callback);
  }

  virtual ~ActionClientIntraProcess() = default;

  bool is_ready(rcl_wait_set_t * wait_set)
  {
    (void) wait_set;

    is_goal_response_ready_ = goal_response_buffer_->has_data();
    is_result_response_ready_ = result_response_buffer_->has_data();
    is_cancel_response_ready_ = cancel_response_buffer_->has_data();
    is_feedback_ready_ = feedback_buffer_->has_data();
    is_status_ready_ = status_buffer_->has_data();

    return is_feedback_ready_ ||
           is_status_ready_ ||
           is_goal_response_ready_ ||
           is_cancel_response_ready_ ||
           is_result_response_ready_;
  }


  void store_goal_response_callback(size_t goal_id, ResponseCallback response_callback)
  {
    set_response_callback_to_event_type(EventType::GoalResponse, response_callback, goal_id);
  }

  void store_cancel_goal_callback(size_t goal_id, ResponseCallback callback)
  {
    set_response_callback_to_event_type(EventType::CancelResponse, callback, goal_id);
  }

  void store_result_response_callback(size_t goal_id, ResponseCallback callback)
  {
    set_response_callback_to_event_type(EventType::ResultResponse, callback, goal_id);
  }

  // Store responses from server
  void store_ipc_action_goal_response(
    GoalResponseSharedPtr goal_response,
    size_t goal_id)
  {
    goal_response_buffer_->add(
      std::make_shared<GoalResponseDataPair>(
        std::make_pair(goal_id, std::move(goal_response))));

    gc_.trigger();
    invoke_on_ready_callback(EventType::GoalResponse, goal_id);
  }

  void store_ipc_action_result_response(
    ResultResponseSharedPtr result_response,
    size_t goal_id)
  {
    result_response_buffer_->add(
      std::make_shared<ResultResponseDataPair>(
        std::make_pair(goal_id, std::move(result_response))));

    gc_.trigger();
    invoke_on_ready_callback(EventType::ResultResponse, goal_id);
  }

  void store_ipc_action_cancel_response(
    CancelResponseSharedPtr cancel_response,
    size_t goal_id)
  {
    cancel_response_buffer_->add(
      std::make_shared<CancelResponseDataPair>(
        std::make_pair(goal_id, std::move(cancel_response))));

    gc_.trigger();
    invoke_on_ready_callback(EventType::CancelResponse, goal_id);
  }

  void store_ipc_action_feedback(FeedbackSharedPtr feedback)
  {
    feedback_buffer_->add(std::move(feedback));
    gc_.trigger();
    invoke_on_ready_callback(EventType::FeedbackReady);
  }

  void store_ipc_action_goal_status(GoalStatusSharedPtr status)
  {
    status_buffer_->add(std::move(status));
    gc_.trigger();
    invoke_on_ready_callback(EventType::StatusReady);
  }

  std::shared_ptr<void>
  take_data() override
  {
    std::shared_ptr<void> data;

    if (is_goal_response_ready_) {
      data = std::move(goal_response_buffer_->consume());
    }
    else if (is_result_response_ready_) {
      data = std::move(result_response_buffer_->consume());
    }
    else if (is_cancel_response_ready_) {
      data = std::move(cancel_response_buffer_->consume());
    }
    else if (is_feedback_ready_) {
      data = std::move(feedback_buffer_->consume());
    }
    else if (is_status_ready_) {
      data = status_buffer_->consume();
    }

    // Data could be null if there were more events than elements in the buffer
    return data;
  }

  std::shared_ptr<void>
  take_data_by_entity_id(size_t id) override
  {
    // Mark as ready the event type from which we want to take data
    switch (static_cast<EventType>(id)) {
      case EventType::ResultResponse:
        is_result_response_ready_ = result_response_buffer_->has_data();
        break;
      case EventType::CancelResponse:
        is_cancel_response_ready_ = cancel_response_buffer_->has_data();
        break;
      case EventType::GoalResponse:
        is_goal_response_ready_ = goal_response_buffer_->has_data();
        break;
      case EventType::FeedbackReady:
        is_feedback_ready_ = feedback_buffer_->has_data();
        break;
      case EventType::StatusReady:
        is_status_ready_ = status_buffer_->has_data();
        break;
    }

    return take_data();
  }

  void execute(std::shared_ptr<void> & data)
  {
    if (!data) {
      // This can happen when there were more events than elements in the ring buffer
      return;
    }

    if (is_goal_response_ready_.exchange(false)) {
      auto goal_response_pair = std::static_pointer_cast<GoalResponseVoidDataPair>(data);
      auto goal_id = goal_response_pair->first;
      auto & goal_response = goal_response_pair->second;

      call_response_callback_and_erase(
        EventType::GoalResponse,
        goal_response,
        goal_id);
    }
    else if (is_result_response_ready_.exchange(false)) {
      auto result_response_pair = std::static_pointer_cast<ResultResponseVoidDataPair>(data);
      auto goal_id = result_response_pair->first;
      auto & result_response = result_response_pair->second;

      call_response_callback_and_erase(
        EventType::ResultResponse,
        result_response,
        goal_id);
    }
    else if (is_cancel_response_ready_.exchange(false)) {
      auto cancel_response_pair = std::static_pointer_cast<CancelResponseVoidDataPair>(data);
      auto goal_id = cancel_response_pair->first;
      auto & cancel_response = cancel_response_pair->second;

      call_response_callback_and_erase(
        EventType::CancelResponse,
        cancel_response,
        goal_id);
    }
    else if (is_feedback_ready_.exchange(false)) {
      call_response_callback_and_erase(
        EventType::FeedbackReady, data, 0, false);
    }
    else if (is_status_ready_.exchange(false)) {
      call_response_callback_and_erase(
        EventType::StatusReady, data, 0, false);
    }
    else {
      throw std::runtime_error("Executing intra-process action client but nothing is ready");
    }
  }

protected:
  // Declare buffers to store responses coming from action server
  typename rclcpp::experimental::buffers::ServiceIntraProcessBuffer<
    GoalResponsePairSharedPtr>::UniquePtr goal_response_buffer_;

  typename rclcpp::experimental::buffers::ServiceIntraProcessBuffer<
    ResultResponsePairSharedPtr>::UniquePtr result_response_buffer_;

  typename rclcpp::experimental::buffers::ServiceIntraProcessBuffer<
    FeedbackSharedPtr>::UniquePtr feedback_buffer_;

  rclcpp::experimental::buffers::ServiceIntraProcessBuffer<
    GoalStatusSharedPtr>::UniquePtr status_buffer_;

  typename rclcpp::experimental::buffers::ServiceIntraProcessBuffer<
    CancelResponsePairSharedPtr>::UniquePtr cancel_response_buffer_;

  std::atomic<bool> is_feedback_ready_{false};
  std::atomic<bool> is_status_ready_{false};
  std::atomic<bool> is_goal_response_ready_{false};
  std::atomic<bool> is_cancel_response_ready_{false};
  std::atomic<bool> is_result_response_ready_{false};
};

}  // namespace experimental
}  // namespace rclcpp

#endif  // RCLCPP__EXPERIMENTAL__ACTION_CLIENT_INTRA_PROCESS_HPP_
