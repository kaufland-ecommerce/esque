import pytest

from esque.topic import _Topic


@pytest.mark.integration
def test_offsets(filled_topic: _Topic):
    assert filled_topic.get_offsets() == {0: (0, 10)}


@pytest.mark.integration
def test_partitions(topic_object: _Topic):
    assert topic_object.partitions == [0]


@pytest.mark.integration
def test_describe(filled_topic: _Topic):
    replicas, conf = filled_topic.describe()

    assert isinstance(replicas, list)
    assert list(replicas[0].keys())[0] == "Partition 0"

    assert isinstance(conf, dict)
