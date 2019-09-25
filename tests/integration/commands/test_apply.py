from typing import Any, Dict

import pytest
import yaml
from click.testing import CliRunner
from esque.resources.topic import Topic
from esque.controller.topic_controller import ConfluentTopicController

from esque.cli.commands import apply
from esque.errors import KafkaException


@pytest.mark.integration
def test_apply(cli_runner, topic_controller: ConfluentTopicController, topic_id: str):
    topic_name = f"apply_{topic_id}"
    topic_1 = {
        "name": topic_name + "_1",
        "replication_factor": 1,
        "num_partitions": 50,
        "config": {"cleanup.policy": "compact"},
    }
    topic_2 = {
        "name": topic_name + "_2",
        "replication_factor": 1,
        "num_partitions": 5,
        "config": {"cleanup.policy": "delete", "delete.retention.ms": 50000},
    }
    apply_conf = {"topics": [topic_1]}

    # 1: topic creation
    path = save_yaml(topic_id, apply_conf)
    result = cli_runner.invoke(apply, ["-f", path], input="Y\n")
    assert (
        result.exit_code == 0 and "Successfully applied changes" in result.output
    ), f"Calling apply failed, error: {result.output}"

    # 2: change cleanup policy to delete
    topic_1["config"]["cleanup.policy"] = "delete"
    path = save_yaml(topic_id, apply_conf)
    result = cli_runner.invoke(apply, ["-f", path], input="Y\n")
    assert (
        result.exit_code == 0 and "Successfully applied changes" in result.output
    ), f"Calling apply failed, error: {result.output}"

    # 3: add another topic and change the first one again
    apply_conf["topics"].append(topic_2)
    topic_1["config"]["cleanup.policy"] = "compact"
    path = save_yaml(topic_id, apply_conf)
    result = cli_runner.invoke(apply, ["-f", path], input="Y\n")
    assert (
        result.exit_code == 0 and "Successfully applied changes" in result.output
    ), f"Calling apply failed, error: {result.output}"

    # 4: no changes
    result = cli_runner.invoke(apply, ["-f", path])
    assert (
        result.exit_code == 0 and "No changes detected, aborting" in result.output
    ), f"Calling apply failed, error: {result.output}"

    # 5: change partitions - this attempt should be cancelled
    topic_1["num_partitions"] = 3
    topic_1["config"]["cleanup.policy"] = "delete"
    path = save_yaml(topic_id, apply_conf)
    result = cli_runner.invoke(apply, ["-f", path], input="Y\n")
    assert (
        result.exit_code == 0 and "to `replication_factor` and `num_partitions`" in result.output
    ), f"Calling apply failed, error: {result.output}"
    # reset config to the old settings again
    topic_1["num_partitions"] = 50
    topic_1["config"]["cleanup.policy"] = "compact"

    # final: check results in the cluster to make sure they match
    for topic_conf in apply_conf["topics"]:
        topic_from_conf = Topic.from_dict(topic_conf)
        assert not topic_controller.diff_with_cluster(
            topic_from_conf
        ).has_changes, f"Topic configs don't match, diff is {topic_controller.diff_with_cluster(topic_from_conf)}"


@pytest.mark.integration
def test_apply_duplicate_names(cli_runner: CliRunner, topic_id: str):
    topic_name = f"apply_{topic_id}"
    topic_1 = {
        "name": topic_name,
        "replication_factor": 1,
        "num_partitions": 50,
        "config": {"cleanup.policy": "compact"},
    }
    apply_conf = {"topics": [topic_1, topic_1]}

    # having the same topic name twice in apply should raise an ValueError
    path = save_yaml(topic_id, apply_conf)
    result = cli_runner.invoke(apply, ["-f", path], input="Y\n")
    assert result.exit_code != 0 and isinstance(result.exception, ValueError), f"Calling apply should have failed"


@pytest.mark.integration
def test_apply_invalid_replicas(cli_runner: CliRunner, topic_id: str):
    topic_name = f"apply_{topic_id}"
    topic_1 = {
        "name": topic_name,
        "replication_factor": 100,
        "num_partitions": 50,
        "config": {"cleanup.policy": "compact"},
    }
    apply_conf = {"topics": [topic_1]}

    # having the same topic name twice in apply should raise an ValueError
    path = save_yaml(topic_id, apply_conf)
    result = cli_runner.invoke(apply, ["-f", path], input="Y\n")
    assert result.exit_code != 0 and isinstance(result.exception, KafkaException), f"Calling apply should have failed"


def save_yaml(fname: str, data: Dict[str, Any]) -> str:
    # this path name is in the gitignore so the temp files are not committed
    path = f"tests/test_samples/{fname}_apply.yaml"
    with open(path, "w") as outfile:
        yaml.dump(data, outfile, default_flow_style=False)
    return path
