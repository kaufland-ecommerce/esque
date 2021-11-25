import pytest
from confluent_kafka import TopicPartition

from esque.controller.consumergroup_controller import ConsumerGroupController
from esque.resources.consumergroup import ConsumerGroup
from esque.resources.topic import Topic


@pytest.mark.integration
def test_get_consumer_group(partly_read_consumer_group: str, consumergroup_controller: ConsumerGroupController):
    instance = consumergroup_controller.get_consumer_group(partly_read_consumer_group)
    assert isinstance(instance, ConsumerGroup)


@pytest.mark.integration
def test_list_consumer_groups(partly_read_consumer_group: str, consumergroup_controller: ConsumerGroupController):
    groups = consumergroup_controller.list_consumer_groups()
    assert partly_read_consumer_group in groups


def test_delete_consumer_groups(partly_read_consumer_group: str, consumergroup_controller: ConsumerGroupController):
    groups_before_deletion = consumergroup_controller.list_consumer_groups()
    assert partly_read_consumer_group in groups_before_deletion
    consumergroup_controller.delete_consumer_groups(consumer_ids=[partly_read_consumer_group])
    groups_after_deletion = consumergroup_controller.list_consumer_groups()
    assert partly_read_consumer_group not in groups_after_deletion


def test_delete_nonexistent_consumer_groups(
    partly_read_consumer_group: str, consumergroup_controller: ConsumerGroupController
):
    groups_before = consumergroup_controller.list_consumer_groups()
    consumergroup_controller.delete_consumer_groups(consumer_ids=["definitely_nonexistent"])
    groups_after = consumergroup_controller.list_consumer_groups()
    assert groups_before == groups_after


def test_consumer_group_offset_set(consumergroup_controller: ConsumerGroupController, filled_topic: Topic):
    topic = TopicPartition(topic=filled_topic.name, offset=5, partition=0)
    consumer_group_name = "non_existing"
    consumergroup_controller.commit_offsets(consumer_group_name, [topic])
    consumer_group: ConsumerGroup = consumergroup_controller.get_consumer_group(consumer_group_name)
    offsets = consumer_group.get_offsets()
    assert offsets[filled_topic.name][0] == 5
