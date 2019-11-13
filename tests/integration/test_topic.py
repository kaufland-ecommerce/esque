import pytest

from esque.controller.topic_controller import TopicController
from esque.resources.topic import Topic, Watermark


@pytest.mark.integration
def test_watermarks(filled_topic: Topic, topic_controller: TopicController):
    topic_controller.update_from_cluster(filled_topic)
    assert filled_topic.watermarks == {0: Watermark(10, 0)}


@pytest.mark.integration
def test_partitions(topic_object: Topic):
    assert [p.partition_id for p in topic_object.partitions] == [0]
