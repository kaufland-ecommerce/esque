import pytest

from esque.topic import Topic, Watermark
from esque.topic_controller import TopicController


@pytest.mark.integration
def test_offsets(consumed_topic, topic_controller: TopicController):
    consumer_group_id, topic_name, total, consumed = consumed_topic
    filled_topic = topic_controller.get_cluster_topic(topic_name)
    assert filled_topic.offsets == {0: Watermark(12, 0)}


@pytest.mark.integration
def test_partitions(topic_object: Topic):
    assert [p.partition_id for p in topic_object.partitions] == [0]
