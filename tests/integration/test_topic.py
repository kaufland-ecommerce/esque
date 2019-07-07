import pytest

from esque.topic import Topic
from esque.topic_controller import TopicController


@pytest.mark.integration
def test_offsets(filled_topic: Topic):
    assert filled_topic.get_offsets() == {0: (0, 10)}


@pytest.mark.integration
def test_partitions(topic_object: Topic):
    assert topic_object.partitions == [0]


@pytest.mark.integration
def test_config_diff(changed_topic_object: Topic, topic_controller: TopicController):
    config_diff = topic_controller.diff_with_cluster(changed_topic_object)

    assert config_diff.get("cleanup.policy") == ("delete", "compact")


@pytest.mark.integration
def test_describe(filled_topic: Topic):
    replicas, conf = filled_topic.describe()

    assert isinstance(replicas, list)
    assert list(replicas[0].keys())[0] == "Partition 0"

    assert isinstance(conf, dict)
