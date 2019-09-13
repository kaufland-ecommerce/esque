import pytest

from esque.consumergroup import ConsumerGroupController


@pytest.mark.integration
def test_basics(consumed_topic, consumergroup_controller: ConsumerGroupController):
    consumer_group_id, topic_name, total, consumed = consumed_topic
    group = consumergroup_controller.get_cluster_consumergroup(consumer_group_id)

    assert consumer_group_id in consumergroup_controller.list_consumer_groups(), "Group should be known by broker"
    assert group.total_lag == total - consumed, f"Total lag should be the same as lag for {topic_name} partition 0"
    assert group.topics == [topic_name], f"Should only be subscribed to topic {topic_name}"
    assert group.topic_partition_offset[topic_name][0] == (0, total, consumed, total - consumed), "It's broken, yo!"
    pytest.set_trace()