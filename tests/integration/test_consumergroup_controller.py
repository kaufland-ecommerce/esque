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
