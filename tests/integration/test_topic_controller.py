from typing import Dict, Any

import confluent_kafka
import pytest
import yaml
from click.testing import CliRunner

from esque.topic import Topic
from esque.topic_controller import TopicController
from esque.errors import KafkaException
from esque.cli.commands import apply


@pytest.fixture()
def topic_controller(cluster):
    yield TopicController(cluster)


@pytest.mark.integration
def test_topic_creation_works(
        topic_controller: TopicController, confluent_admin_client: confluent_kafka.admin.AdminClient, topic_id: str
):
    topics = confluent_admin_client.list_topics(timeout=5).topics.keys()
    assert topic_id not in topics
    topic_controller.create_topics([Topic(topic_id, replication_factor=1)])

    # invalidate cache
    confluent_admin_client.poll(timeout=1)
    topics = confluent_admin_client.list_topics(timeout=5).topics.keys()
    assert topic_id in topics


@pytest.mark.integration
def test_topic_creation_raises_for_wrong_config(
        topic_controller: TopicController,
        confluent_admin_client: confluent_kafka.admin.AdminClient,
        topic_id: str,
):
    topics = confluent_admin_client.list_topics(timeout=5).topics.keys()
    assert topic_id not in topics
    # We only have 1 broker for tests, so a higher replication should fail
    with pytest.raises(KafkaException):
        topic_controller.create_topics([Topic(topic_id, replication_factor=2)])


@pytest.mark.integration
def test_alter_topic_config_works(topic_controller: TopicController, topic_id: str):
    initial_topic = Topic(topic_id, config={"cleanup.policy": "delete"})

    topic_controller.create_topics([initial_topic])
    topic_controller.update_from_cluster(initial_topic)
    replicas, config = initial_topic.describe()
    assert config.get("Config").get("cleanup.policy") == "delete"
    change_topic = Topic(topic_id, config={"cleanup.policy": "compact"})
    topic_controller.alter_configs([change_topic])
    topic_controller.update_from_cluster(change_topic)
    after_changes_applied_topic = topic_controller.get_cluster_topic(topic_id)

    replicas, final_config = after_changes_applied_topic.describe()
    assert final_config.get("Config").get("cleanup.policy") == "compact"


@pytest.mark.integration
def test_topic_deletion_works(
        topic_controller: TopicController, confluent_admin_client: confluent_kafka.admin.AdminClient, topic: str
):
    topics = confluent_admin_client.list_topics(timeout=5).topics.keys()
    assert topic in topics
    topic_controller.delete_topic(topic_controller.get_cluster_topic(topic))
    # Invalidate cache
    confluent_admin_client.poll(timeout=1)
    topics = confluent_admin_client.list_topics(timeout=5).topics.keys()
    assert topic not in topics


@pytest.mark.integration
def test_topic_listing_works(topic_controller: TopicController, topic: str):
    topics = topic_controller.list_topics()
    assert topic in [t.name for t in topics]


@pytest.mark.integration
def test_topic_object_works(topic_controller: TopicController, topic: str):
    topic = topic_controller.get_cluster_topic(topic)
    assert isinstance(topic, Topic)
    assert len(topic.get_offsets()) != 0


@pytest.mark.integration
def test_apply(topic_controller: TopicController, topic_id: str):
    runner = CliRunner()
    topic_name = f"apply_{topic_id}"
    topic_1 = {
        "name": topic_name + "_1",
        "replication_factor": 1,
        "num_partitions": 50,
        "config": {
            "cleanup.policy": "compact"
        }
    }
    topic_2 = {
        "name": topic_name + "_2",
        "replication_factor": 1,
        "num_partitions": 5,
        "config": {
            "cleanup.policy": "delete",
            "delete.retention.ms": 50000
        }
    }
    apply_conf = {
        "topics": [topic_1]
    }

    # 1: topic creation
    path = save_yaml(topic_id, apply_conf)
    result = runner.invoke(apply, ["-f", path], input="Y\n")
    assert result.exit_code == 0 and "Successfully created topics:" in result.output, \
        f"Calling apply failed, error: {result.output}"

    # 2: change cleanup policy to delete
    topic_1["config"]["cleanup.policy"] = "delete"
    path = save_yaml(topic_id, apply_conf)
    result = runner.invoke(apply, ["-f", path], input="Y\n")
    assert result.exit_code == 0 and "Successfully changed topics:" in result.output, \
        f"Calling apply failed, error: {result.output}"

    # 3: add another topic and change the first one again
    apply_conf["topics"].append(topic_2)
    topic_1["config"]["cleanup.policy"] = "compact"
    path = save_yaml(topic_id, apply_conf)
    result = runner.invoke(apply, ["-f", path], input="Y\nY\n")
    assert result.exit_code == 0 and "Successfully changed topics:" in result.output \
           and "Successfully created topics:" in result.output, \
        f"Calling apply failed, error: {result.output}"

    # final: check results in the cluster to make sure they match
    for topic_conf in apply_conf["topics"]:
        topic_from_conf = Topic.from_dict(topic_conf)
        assert topic_controller.diff_with_cluster(topic_from_conf) == {}, \
            f"Topic configs don't match, diff is {topic_controller.diff_with_cluster(topic_from_conf)}"


def save_yaml(fname: str, data: Dict[str, Any]) -> str:
    # this path name is in the gitignore so the temp files are not committed
    path = f"tests/test_samples/{fname}_apply.yaml"
    with open(path, "w") as outfile:
        yaml.dump(data, outfile, default_flow_style=False)
    return path
