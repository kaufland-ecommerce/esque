import json

import confluent_kafka
import pytest

from esque.controller.topic_controller import TopicController
from esque.errors import KafkaException
from esque.resources.topic import Topic, TopicDiff


@pytest.fixture()
def topic_controller(cluster):
    yield cluster.topic_controller


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
    topic_controller: TopicController, confluent_admin_client: confluent_kafka.admin.AdminClient, topic_id: str
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
    config = initial_topic.config
    assert config.get("cleanup.policy") == "delete"
    change_topic = Topic(topic_id, config={"cleanup.policy": "compact"})
    topic_controller.alter_configs([change_topic])
    topic_controller.update_from_cluster(change_topic)
    after_changes_applied_topic = topic_controller.get_cluster_topic(topic_id)

    final_config = after_changes_applied_topic.config
    assert final_config.get("cleanup.policy") == "compact"


@pytest.mark.integration
def test_topic_listing_works(topic_controller: TopicController, topic: str):
    topics = topic_controller.list_topics()
    assert topic in [t.name for t in topics]


@pytest.mark.integration
def test_topic_object_works(topic_controller: TopicController, topic: str):
    topic = topic_controller.get_cluster_topic(topic)
    assert isinstance(topic, Topic)
    assert len(topic.offsets) != 0


@pytest.mark.integration
def test_topic_diff(topic_controller: TopicController, topic_id: str):
    # the value we get from cluster configs is as string
    # testing against this is important to ensure consistency
    default_delete_retention = "86400000"
    topic_conf = {
        "name": topic_id,
        "replication_factor": 1,
        "num_partitions": 50,
        "config": {"cleanup.policy": "compact"},
    }
    get_diff = topic_controller.diff_with_cluster

    conf = json.loads(json.dumps(topic_conf))
    topic = Topic.from_dict(conf)
    topic_controller.create_topics([topic])
    assert not get_diff(topic).has_changes, "Shouldn't have diff on just created topic"

    conf = json.loads(json.dumps(topic_conf))
    conf["config"]["cleanup.policy"] = "delete"
    topic = Topic.from_dict(conf)
    diff = TopicDiff().set_diff("cleanup.policy", "compact", "delete")
    assert get_diff(topic) == diff, "Should have a diff on cleanup.policy"

    conf = json.loads(json.dumps(topic_conf))
    conf["config"]["delete.retention.ms"] = 1500
    topic = Topic.from_dict(conf)
    diff = TopicDiff().set_diff("delete.retention.ms", default_delete_retention, 1500)
    assert get_diff(topic) == diff, "Should have a diff on delete.retention.ms"

    # the same as before, but this time with string values
    conf = json.loads(json.dumps(topic_conf))
    conf["config"]["delete.retention.ms"] = "1500"
    topic = Topic.from_dict(conf)
    diff = TopicDiff().set_diff("delete.retention.ms", default_delete_retention, "1500")
    assert get_diff(topic) == diff, "Should have a diff on delete.retention.ms"

    conf = json.loads(json.dumps(topic_conf))
    conf["num_partitions"] = 3
    topic = Topic.from_dict(conf)
    diff = TopicDiff().set_diff("num_partitions", 50, 3)
    assert get_diff(topic) == diff, "Should have a diff on num_partitions"

    conf = json.loads(json.dumps(topic_conf))
    conf["replication_factor"] = 3
    topic = Topic.from_dict(conf)
    diff = TopicDiff().set_diff("replication_factor", 1, 3)
    assert get_diff(topic) == diff, "Should have a diff on replication_factor"
