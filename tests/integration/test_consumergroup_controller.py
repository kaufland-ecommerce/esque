import pytest

from esque.controller.consumergroup_controller import ConsumerGroupController, ConsumerGroupOffsetPlan
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


def test_consumer_groupt_offsets(consumergroup_controller: ConsumerGroupController, filled_topic: Topic):
    custom_plan = ConsumerGroupOffsetPlan(
        topic_name=filled_topic.name,
        current_offset=0,
        proposed_offset=5,
        low_watermark=0,
        high_watermark=5,
        partition_id=0,
    )
    consumer_group = "non_existing"
    consumergroup_controller.edit_consumer_group_offsets(consumer_group, [custom_plan])
    assert consumer_group in consumergroup_controller.list_consumer_groups()
