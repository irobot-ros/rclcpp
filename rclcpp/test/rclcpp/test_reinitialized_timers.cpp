// Copyright 2024 iRobot Corporation. All Rights Reserved.

#include <gtest/gtest.h>

#include <rclcpp/rclcpp.hpp>

#include "rclcpp/experimental/executors/events_executor/events_executor.hpp"
#include "rclcpp/experimental/executors/events_executor/lock_free_events_queue.hpp"

using rclcpp::experimental::executors::EventsExecutor;

class TimersTest
: public testing::Test
{
public:
    void SetUp() override
    {
        rclcpp::init(0, nullptr);
    }

    void TearDown() override
    {
        rclcpp::shutdown();
    }
};

TEST_F(TimersTest, TimersWithSamePeriod)
{
    auto timers_period = std::chrono::milliseconds(50);
    auto node = std::make_shared<rclcpp::Node>("test_node");
    auto events_queue = std::make_unique<rclcpp::experimental::executors::LockFreeEventsQueue>();
    auto executor = std::make_unique<EventsExecutor>(std::move(events_queue), true, rclcpp::ExecutorOptions());

    executor->add_node(node);

    size_t count_1 = 0;
    auto timer_1 = rclcpp::create_timer(
        node,
        node->get_clock(),
        rclcpp::Duration(timers_period),
        [&count_1]() {
            count_1++;
        });

    size_t count_2 = 0;
    auto timer_2 = rclcpp::create_timer(
        node,
        node->get_clock(),
        rclcpp::Duration(timers_period),
        [&count_2]() {
            count_2++;
        });

    {
        std::thread executor_thread([&executor](){
            executor->spin();
        });

        while (count_2 < 10u) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        executor->cancel();
        executor_thread.join();

        EXPECT_GE(count_2, 10u);
        EXPECT_LE(count_2 - count_1, 1u);
    }

    count_1 = 0;
    timer_1 = rclcpp::create_timer(
        node,
        node->get_clock(),
        rclcpp::Duration(timers_period),
        [&count_1]() {
            count_1++;
        });

    count_2 = 0;
    timer_2 = rclcpp::create_timer(
        node,
        node->get_clock(),
        rclcpp::Duration(timers_period),
        [&count_2]() {
            count_2++;
        });

    {
        std::thread executor_thread([&executor](){
            executor->spin();
        });

        while (count_2 < 10u) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        executor->cancel();
        executor_thread.join();

        EXPECT_GE(count_2, 10u);
        EXPECT_LE(count_2 - count_1, 1u);
    }
}

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
