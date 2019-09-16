from typing import List

import pytest

from esque.cli.commands import validate_plan
from esque.errors import ValidationError
from esque.topic import Topic, Partition


def new_partition(partition_id: int, replicas: List[int]):
    return Partition(partition_id, 0, 0, [], replicas[0], replicas)


def dummy_topic():
    topic = Topic("test_topic", 3, 3)

    topic.partition_data = [new_partition(0, [0, 1, 2]), new_partition(1, [1, 2, 0]), new_partition(2, [2, 0, 1])]
    topic.is_only_local = False
    return topic


def topic_controller_mock(mocker):
    topic = dummy_topic()

    topic_controller = mocker.Mock()
    topic_controller.cluster = mocker.Mock()
    topic_controller.cluster.brokers = [{"id": 0}, {"id": 1}, {"id": 2}, {"id": 3}]

    topic_controller.get_cluster_topic.return_value = topic

    return topic_controller


invalid_plans = [
    # Partition does not exist
    ({"partitions": [{"topic": "test_topic", "partition": 100, "replicas": [1, 2, 0]}]}, "E01"),
    # Number of replicas != replication_factor
    ({"partitions": [{"topic": "test_topic", "partition": 1, "replicas": [1, 2, 0, 4]}]}, "E02"),
    ({"partitions": [{"topic": "test_topic", "partition": 1, "replicas": [1, 2]}]}, "E02"),
    # Broker doesn't exist
    ({"partitions": [{"topic": "test_topic", "partition": 0, "replicas": [1, 2, 10]}]}, "E03"),
    # Duplicate replicas
    ({"partitions": [{"topic": "test_topic", "partition": 0, "replicas": [0, 1, 1]}]}, "E04"),
    # No changes in replication
    ({"partitions": [{"topic": "test_topic", "partition": 0, "replicas": [2, 1, 0]}]}, "E05"),
    # Old leader still replica.
    ({"partitions": [{"topic": "test_topic", "partition": 0, "replicas": [3, 1, 0]}]}, "E06"),
]


@pytest.mark.parametrize("plan,code", invalid_plans)
def test_validation(mocker, plan, code):
    topic_controller = topic_controller_mock(mocker)

    with pytest.raises(ValidationError) as err:
        validate_plan(topic_controller, plan)

    assert err.value.code == code, f"Wrong code {err.value.code}, should be {code}"
