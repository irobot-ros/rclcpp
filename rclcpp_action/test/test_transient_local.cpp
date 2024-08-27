// Copyright 2024 iRobot Corporation. All Rights Reserved.

#include <gtest/gtest.h>

#include <chrono>
#include <thread>

#include <rclcpp/experimental/executors/events_executor/events_executor.hpp>
#include <rclcpp/experimental/executors/events_executor/lock_free_events_queue.hpp>
#include <rclcpp/node.hpp>
#include <rclcpp/publisher.hpp>
#include <rclcpp/subscription.hpp>
#include <std_msgs/msg/empty.hpp>

struct transient_local_test_data_t
{
    // Whether the publisher should use IPC
    bool pub_use_ipc {false};
    // How many early subscribers and whether they should use IPC
    std::vector<bool> early_subs_use_ipc {};
    // How many late subscribers and whether they should use IPC
    std::vector<bool> late_subs_use_ipc {};
};

static std::string to_string(
    const std::vector<bool> & bool_vec,
    const std::string & property_name)
{
    if (bool_vec.empty()) {
        return "NONE;";
    }

    std::string result;
    for (bool b : bool_vec) {
        if (!b) {
            result += "Non-";
        }
        result += property_name + ", ";
    }

    return result;
}

class TransientLocalPermutationTest
: public testing::Test, public testing::WithParamInterface<transient_local_test_data_t>
{
public:
    void SetUp() override
    {
        rclcpp::init(0, nullptr);
        auto p = GetParam();
        std::cout << "Test permutation: { "
                  << (p.pub_use_ipc ? "IPC Publisher, " : "Non-IPC Publisher, ")
                  << "Early: " << to_string(p.early_subs_use_ipc, "IPC Sub")
                  << " Late: " << to_string(p.late_subs_use_ipc, "IPC Sub")
                  << " }"<< std::endl;
    }

    void TearDown() override
    {
        rclcpp::shutdown();
    }
};

INSTANTIATE_TEST_SUITE_P(
    EarlySubsOnlyTest,
    TransientLocalPermutationTest,
    testing::Values(
        ///// tests with only 1 early sub; not very interesting but let's run them
        // non-ipc pub, 1 early non-ipc sub
        transient_local_test_data_t{false, {false}, {}},
        // non-ipc pub, 1 early ipc sub
        transient_local_test_data_t{false, {true}, {}},
        // ipc pub, 1 early non-ipc sub
        transient_local_test_data_t{true, {false}, {}},
        // ipc pub, 1 early ipc sub
        transient_local_test_data_t{true, {true}, {}}
    ));

INSTANTIATE_TEST_SUITE_P(
    LateSubsOnlyTest,
    TransientLocalPermutationTest,
    testing::Values(
        ///// tests with only 1 late sub
        // non-ipc pub, non-ipc late sub
        transient_local_test_data_t{false, {}, {false}},
        // non-ipc pub, ipc late sub
        transient_local_test_data_t{false, {}, {true}},
        // ipc pub, non-ipc late sub
        transient_local_test_data_t{true, {}, {false}},
        // ipc pub, ipc late sub
        transient_local_test_data_t{true, {}, {true}},
        ///// tests with late mixed subscriptions
        // non-ipc pub, mixed late subs
        transient_local_test_data_t{false, {}, {true, false}},
        // ipc pub, mixed late subs
        transient_local_test_data_t{true, {}, {true, false}}
    ));

INSTANTIATE_TEST_SUITE_P(
    EarlyAndLateSubsTest,
    TransientLocalPermutationTest,
    testing::Values(
        ///// tests with early and late subscriptions
        // non-ipc pub, 1 non-ipc early sub, 1 non-ipc late sub
        transient_local_test_data_t{false, {false}, {false}},
        // non-ipc pub, 1 non-ipc early sub, 1 ipc late sub
        transient_local_test_data_t{false, {false}, {true}},
        // non-ipc pub, 1 non-ipc early sub, mixed late sub
        transient_local_test_data_t{false, {false}, {true, false}},
        // non-ipc pub, 1 ipc early sub, 1 non-ipc late sub
        transient_local_test_data_t{false, {true}, {false}},
        // non-ipc pub, 1 ipc early sub, 1 ipc late sub
        transient_local_test_data_t{false, {true}, {true}},
        // non-ipc pub, 1 ipc early sub, mixed late sub
        transient_local_test_data_t{false, {true}, {true, false}},
        // ipc pub, 1 non-ipc early sub, 1 non-ipc late sub
        transient_local_test_data_t{true, {false}, {false}},
        // ipc pub, 1 non-ipc early sub, 1 ipc late sub
        transient_local_test_data_t{true, {false}, {true}},
        // ipc pub, 1 non-ipc early sub, mixed late sub
        transient_local_test_data_t{true, {false}, {true, false}},
        // ipc pub, 1 ipc early sub, 1 non-ipc late sub
        transient_local_test_data_t{true, {true}, {false}},
        // ipc pub, 1 ipc early sub, 1 ipc late sub
        transient_local_test_data_t{true, {true}, {true}},
        // ipc pub, 1 ipc early sub, mixed late sub
        transient_local_test_data_t{true, {true}, {true, false}}
    ));

struct sub_data_t
{
    std::string id {};
    std::atomic<bool> msg_received {false};
};

TEST_P(TransientLocalPermutationTest, PublishWithNoSubs)
{
    auto test_data = GetParam();

    auto pub_options = rclcpp::NodeOptions().use_intra_process_comms(test_data.pub_use_ipc);
    auto pub_node = rclcpp::Node::make_shared("pub_node", pub_options);

    auto publisher = rclcpp::create_publisher<std_msgs::msg::Empty>(
        pub_node,
        "test_topic",
        rclcpp::QoS(10).transient_local());

    auto sub_node = rclcpp::Node::make_shared("sub_node");

    // Add nodes to the executor
    auto events_queue = std::make_unique<rclcpp::experimental::executors::LockFreeEventsQueue>();

    auto executor = std::make_unique<rclcpp::experimental::executors::EventsExecutor>(std::move(events_queue), false, rclcpp::ExecutorOptions());

    executor->add_node(pub_node);
    executor->add_node(sub_node);
    auto executor_thread = std::thread([&](){ executor->spin(); });

    while (!executor->is_spinning()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    std::vector<std::shared_ptr<sub_data_t>> test_subs_data;
    std::vector<std::shared_ptr<rclcpp::SubscriptionBase>> test_subs;

    auto build_subs_from_vec = [&test_subs_data, &test_subs, sub_node=sub_node](
        const std::vector<bool> & subs_use_ipc,
        const std::string & prefix)
    {
        for (size_t i = 0; i < subs_use_ipc.size(); i++) {
            bool use_ipc = subs_use_ipc[i];

            auto sub_data = std::make_shared<sub_data_t>();

            sub_data->id = prefix + "_" + std::to_string(i) +"_with_" + (use_ipc ? "ipc" : "no-ipc");

            auto sub_options = rclcpp::SubscriptionOptions();
            sub_options.use_intra_process_comm = use_ipc ? rclcpp::IntraProcessSetting::Enable : rclcpp::IntraProcessSetting::Disable;
            auto sub = rclcpp::create_subscription<std_msgs::msg::Empty>(
                sub_node,
                "test_topic",
                rclcpp::QoS(10).transient_local(),
                [sub_data=sub_data](std_msgs::msg::Empty::ConstSharedPtr) { sub_data->msg_received = true; },
                sub_options);

            std::cout<<"Created " << sub_data->id << std::endl;
            test_subs_data.push_back(sub_data);
            test_subs.push_back(sub);
        }
    };

    // Create early subscriptions
    build_subs_from_vec(test_data.early_subs_use_ipc, "early");

    // Wait for discovery of early subs (to ensure that they are effectively "early")
    auto start_time = std::chrono::high_resolution_clock::now();
    while (std::chrono::high_resolution_clock::now() - start_time < std::chrono::seconds(10))
    {
        if (publisher->get_subscription_count() == test_subs.size()) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    ASSERT_EQ(publisher->get_subscription_count(), test_subs.size());

    // Publish the message
    auto msg = std::make_unique<std_msgs::msg::Empty>();
    publisher->publish(std::move(msg));

    // Create late subscriptions
    build_subs_from_vec(test_data.late_subs_use_ipc, "late");

    // Run until all subs have received a message or we timeout
    start_time = std::chrono::high_resolution_clock::now();
    while (std::chrono::high_resolution_clock::now() - start_time < std::chrono::seconds(20))
    {
        bool done = true;
        for (const auto & data : test_subs_data) {
            // This sub didn't get a message, so go to next iteration oft
            if (!data->msg_received) {
                done = false;
                break;
            }
        }

        if (done) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }

    executor->cancel();
    executor_thread.join();

    // Verify that all subs received data
    for (const auto & data : test_subs_data) {
        EXPECT_TRUE(data->msg_received) << data->id << " didn't receive a message";
    }
}

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
