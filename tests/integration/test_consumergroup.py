from typing import Tuple

import pytest
from click.testing import CliRunner

from esque.cli.commands import get_consumergroups, describe_consumergroup
from esque.controller.consumergroup_controller import ConsumerGroupController
from tests.conftest import consumer_with_id


@pytest.mark.integration
def test_new_group(random_id: str, topic_id: str, consumergroup_controller: ConsumerGroupController):
    group_id = f"new_group_{random_id}"
    runner = CliRunner()

    consumer = consumer_with_id(group_id)
    consumer.subscribe([topic_id])
    # need to call `poll` to make sure all assignments etc.
    # are done and the CG is known by the cluster
    consumer.poll(10)
    assert group_id in consumergroup_controller.list_consumer_groups(), "Group should be known by broker"

    result = runner.invoke(get_consumergroups)
    assert result.exit_code == 0 and group_id in result.output, "`get consumergroups` went wrong"

    result = runner.invoke(describe_consumergroup, [group_id])
    assert (
        result.exit_code == 0 and group_id in result.output and "topics []" in result.output
    ), "`describe consumergroups` went wrong"


@pytest.mark.integration
def test_basics(consumed_topic: Tuple[str, str, int, int], consumergroup_controller: ConsumerGroupController):
    consumer_group_id, topic_name, total, consumed = consumed_topic
    group = consumergroup_controller.get_cluster_consumergroup(consumer_group_id)

    assert consumer_group_id in consumergroup_controller.list_consumer_groups(), "Group should be known by broker"
    assert group.total_lag == total - consumed, f"Total lag should be the same as lag for {topic_name} partition 0"
    assert group.topics == [topic_name], f"Should only be subscribed to topic {topic_name}"
    assert group.topic_partition_offset[topic_name][0] == (0, total, consumed, total - consumed), "It's broken, yo!"
