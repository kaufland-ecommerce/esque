import pytest

from esque.controller.consumergroup_controller import ConsumerGroupController
from esque.resources.consumergroup import ConsumerGroup


@pytest.mark.integration
def test_get_consumer_group(partly_read_consumer_group: str, consumergroup_controller: ConsumerGroupController):
    instance = consumergroup_controller.get_consumergroup(partly_read_consumer_group)
    assert isinstance(instance, ConsumerGroup)


@pytest.mark.integration
def test_list_consumer_groups(partly_read_consumer_group: str, consumergroup_controller: ConsumerGroupController):
    groups = consumergroup_controller.list_consumer_groups()
    assert partly_read_consumer_group in groups


def test_delete_consumer_groups(partly_read_consumer_group: str, consumergroup_controller: ConsumerGroupController):
    groups_before_deletion = consumergroup_controller.list_consumer_groups()
    assert partly_read_consumer_group in groups_before_deletion
    consumergroup_controller.delete_consumergroups(consumer_ids=[partly_read_consumer_group])
    groups_after_deletion = consumergroup_controller.list_consumer_groups()
    assert partly_read_consumer_group not in groups_after_deletion


def test_delete_nonexistent_consumer_groups(
    partly_read_consumer_group: str, consumergroup_controller: ConsumerGroupController
):
    consumergroup_controller.delete_consumergroups(consumer_ids=["definitely_nonexistent"])
    groups = consumergroup_controller.list_consumer_groups()
    assert partly_read_consumer_group in groups
