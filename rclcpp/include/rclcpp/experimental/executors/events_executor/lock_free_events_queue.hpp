// Copyright 2022-2025 iRobot Corporation
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
//    * Redistributions of source code must retain the above copyright
//      notice, this list of conditions and the following disclaimer.
//
//    * Redistributions in binary form must reproduce the above copyright
//      notice, this list of conditions and the following disclaimer in the
//      documentation and/or other materials provided with the distribution.
//
//    * Neither the name of the iRobot Corporation nor the names of its
//      contributors may be used to endorse or promote products derived from
//      this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
// SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
// CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.


#ifndef RCLCPP__EXPERIMENTAL__EXECUTORS__EVENTS_EXECUTOR__LOCK_FREE_EVENTS_QUEUE_HPP_
#define RCLCPP__EXPERIMENTAL__EXECUTORS__EVENTS_EXECUTOR__LOCK_FREE_EVENTS_QUEUE_HPP_

#include "rclcpp/experimental/executors/events_executor/concurrent_queue/blockingconcurrentqueue.h"
#include "rclcpp/experimental/executors/events_executor/events_queue.hpp"

namespace rclcpp
{
namespace experimental
{
namespace executors
{

/**
 * @brief This class implements an EventsQueue as a simple wrapper around
 * the blockingconcurrentqueue.h
 * See https://github.com/cameron314/concurrentqueue
 * It does not perform any checks about the size of queue, which can grow
 * unbounded without being pruned. (there are options about this, read the docs).
 * This implementation is lock free, producers and consumers can use the queue
 * concurrently without the need for synchronization mechanisms. The use of this
 * queue aims to fix the issue of publishers being blocked by the executor extracting
 * events from the queue in a different thread, causing expensive mutex contention.
 */
class LockFreeEventsQueue : public EventsQueue
{
public:
  RCLCPP_PUBLIC
  ~LockFreeEventsQueue() override
  {
    // It's important that all threads have finished using the queue
    // and the memory effects have fully propagated, before it is destructed.
    // Consume all events
    ExecutorEvent event;
    while (event_queue_.try_dequeue(event)) {}
  }

  /**
   * @brief enqueue event into the queue
   * @param event The event to enqueue into the queue
   */
  RCLCPP_PUBLIC
  void
  enqueue(const ExecutorEvent & event) override
  {
    ExecutorEvent single_event = event;
    single_event.num_events = 1;
    for (size_t ev = 0; ev < event.num_events; ev++) {
      event_queue_.enqueue(single_event);
    }
  }

  /**
   * @brief waits for an event until timeout
   * @return true if event, false if timeout
   */
  RCLCPP_PUBLIC
  bool
  dequeue(
    rclcpp::experimental::executors::ExecutorEvent & event,
    std::chrono::nanoseconds timeout = std::chrono::nanoseconds::max()) override
  {
    if (timeout != std::chrono::nanoseconds::max()) {
      return event_queue_.wait_dequeue_timed(event, timeout);
    }

    // If no timeout specified, just wait for an event to arrive
    event_queue_.wait_dequeue(event);
    return true;
  }

  /**
   * @brief Test whether queue is empty
   * @return true if the queue's size is 0, false otherwise.
   */
  RCLCPP_PUBLIC
  bool
  empty() const override
  {
    return event_queue_.size_approx() == 0;
  }

  /**
   * @brief Returns the number of elements in the queue.
   * This estimate is only accurate if the queue has completely
   * stabilized before it is called
   * @return the number of elements in the queue.
   */
  RCLCPP_PUBLIC
  size_t
  size() const override
  {
    return event_queue_.size_approx();
  }

private:
  moodycamel::BlockingConcurrentQueue<rclcpp::experimental::executors::ExecutorEvent> event_queue_;
};

}  // namespace executors
}  // namespace experimental
}  // namespace rclcpp


#endif  // RCLCPP__EXPERIMENTAL__EXECUTORS__EVENTS_EXECUTOR__LOCK_FREE_EVENTS_QUEUE_HPP_
