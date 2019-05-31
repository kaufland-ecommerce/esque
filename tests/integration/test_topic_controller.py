import confluent_kafka
import pytest

from esque.topic import _Topic, TopicController


@pytest.fixture()
def topic_controller(cluster):
    yield TopicController(cluster)


@pytest.mark.integration
def test_topic_creation_works(
    topic_controller: TopicController,
    confluent_admin_client: confluent_kafka.admin.AdminClient,
    topic_id: str,
):
    topics = confluent_admin_client.list_topics(timeout=5).topics.keys()
    assert topic_id not in topics
    topic_controller.create_topics(
        [topic_controller.get_topic(topic_id, replication_factor=1)]
    )
    # invalidate cache
    confluent_admin_client.poll(timeout=1)
    topics = confluent_admin_client.list_topics(timeout=5).topics.keys()
    assert topic_id in topics


@pytest.mark.integration
def test_alter_topic_config_works(topic_controller: TopicController, topic_id: str):
    initial_topic = topic_controller.get_topic(
        topic_id, config={"cleanup.policy": "delete"}
    )
    topic_controller.create_topics([initial_topic])
    replicas, config = initial_topic.describe()
    assert config.get("Config").get("cleanup.policy") == "delete"
    change_topic = topic_controller.get_topic(
        topic_id, config={"cleanup.policy": "compact"}
    )
    topic_controller.alter_configs([change_topic])
    after_changes_applied_topic = topic_controller.get_topic(topic_id)
    replicas, final_config = after_changes_applied_topic.describe()
    assert final_config.get("Config").get("cleanup.policy") == "compact"


@pytest.mark.integration
def test_topic_deletion_works(
    topic_controller: TopicController,
    confluent_admin_client: confluent_kafka.admin.AdminClient,
    topic: str,
):
    topics = confluent_admin_client.list_topics(timeout=5).topics.keys()
    assert topic in topics
    topic_controller.delete_topic(topic_controller.get_topic(topic))
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
    topic = topic_controller.get_topic(topic)
    assert isinstance(topic, _Topic)
    assert len(topic.get_offsets()) != 0
