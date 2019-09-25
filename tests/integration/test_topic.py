import pytest

from esque.controller.topic_controller import ConfluentTopicController
from esque.resources.topic import Topic, Watermark


@pytest.mark.integration
def test_offsets(filled_topic: Topic, topic_controller: ConfluentTopicController):
    topic_controller._update_from_cluster(filled_topic)
    assert filled_topic.offsets == {0: Watermark(10, 0)}


@pytest.mark.integration
def test_partitions(topic_object: Topic):
    assert [p.partition_id for p in topic_object.partitions] == [0]
