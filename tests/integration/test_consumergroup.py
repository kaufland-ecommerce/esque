import pytest

from esque.resources.consumergroup import ConsumerGroup
from esque.resources.topic import Topic


@pytest.mark.integration
def test_describe(consumergroup_instance: ConsumerGroup, topic_object: Topic):
    consumer_offset = consumergroup_instance.describe()

    offset = consumer_offset["offsets"][topic_object.name]

    assert offset["consumer_offset"] == (5, 5)
    assert offset["topic_low_watermark"] == (0, 0)
    assert offset["topic_high_watermark"] == (10, 10)
    assert offset["consumer_lag"] == (5, 5)
