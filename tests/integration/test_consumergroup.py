import pytest
from esque.resources.consumergroup import ConsumerGroup


@pytest.mark.integration
def test_describe(consumergroup_instance: ConsumerGroup):
    consumer_offset = consumergroup_instance.describe()

    offset = consumer_offset["offsets"]

    assert offset["consumer_offset"] == (5, 5)
    assert offset["topic_low_watermark"] == (0, 0)
    assert offset["topic_high_watermark"] == (10, 10)
    assert offset["consumer_lag"] == (5, 5)
