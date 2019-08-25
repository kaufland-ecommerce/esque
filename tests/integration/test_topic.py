import pytest

from esque.topic import Topic
from esque.topic_controller import TopicController


@pytest.mark.integration
def test_offsets(filled_topic: Topic, topic_controller: TopicController):
    topic_controller.update_from_cluster(filled_topic)
    assert filled_topic.get_offsets() == {0: (0, 10)}


@pytest.mark.integration
def test_partitions(topic_object: Topic):
    assert [p['partition_id'] for p in topic_object.partitions] == [0]


