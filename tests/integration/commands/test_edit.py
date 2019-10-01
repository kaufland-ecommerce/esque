import click
import confluent_kafka
import pytest
import yaml
from click.testing import CliRunner

from esque.cli.commands import edit_topic
from esque.controller.topic_controller import TopicController
from esque.errors import TopicConfigNotValidException


@pytest.mark.integration
def test_topic_creation_with_template_works(
    monkeypatch,
    topic_controller: TopicController,
    confluent_admin_client: confluent_kafka.admin.AdminClient,
    topic: str,
):

    topics = confluent_admin_client.list_topics(timeout=5).topics.keys()
    assert topic in topics

    config_dict = {
        "config": {
            "cleanup.policy": "delete",
            "compression.type": "producer",
            "delete.retention.ms": "123456789",
            "file.delete.delay.ms": "60000",
            "flush.messages": "123456789",
            "flush.ms": "9223372036854775807",
            "follower.replication.throttled.replicas": "",
            "index.interval.bytes": "4096",
            "leader.replication.throttled.replicas": "",
            "max.message.bytes": "1000012",
            "message.downconversion.enable": "true",
            "message.format.version": "2.2-IV1",
            "message.timestamp.difference.max.ms": "123456789",
            "message.timestamp.type": "CreateTime",
            "min.cleanable.dirty.ratio": "0.5",
            "min.compaction.lag.ms": "0",
            "min.insync.replicas": "1",
            "preallocate": "false",
            "retention.bytes": "-1",
            "retention.ms": "123456789",
            "segment.bytes": "123456789",
            "segment.index.bytes": "123456789",
            "segment.jitter.ms": "0",
            "segment.ms": "123456789",
            "unclean.leader.election.enable": "true",
        }
    }

    def mock_edit_function(text=None, editor=None, env=None, require_save=True, extension=".txt", filename=None):
        return yaml.dump(config_dict, default_flow_style=False)

    monkeypatch.setattr(click, "edit", mock_edit_function)
    result = CliRunner().invoke(edit_topic, topic, input="y\n", catch_exceptions=False)
    assert result.exit_code == 0
    topic_config_dict = topic_controller.get_cluster_topic(topic).as_dict(only_editable=True)
    assert topic_config_dict == config_dict


@pytest.mark.integration
def test_topic_creation_with_unknown_key_fails(
        monkeypatch,
        topic_controller: TopicController,
        confluent_admin_client: confluent_kafka.admin.AdminClient,
        topic: str,
):

    topics = confluent_admin_client.list_topics(timeout=5).topics.keys()
    assert topic in topics

    config_dict = {
        "config": {
            "foo.bar.baz": "true"
        }
    }

    def mock_edit_function(text=None, editor=None, env=None, require_save=True, extension=".txt", filename=None):
        return yaml.dump(config_dict, default_flow_style=False)
    monkeypatch.setattr(click, "edit", mock_edit_function)
    result = CliRunner().invoke(edit_topic, topic, input="y\n", catch_exceptions=True)

    assert result.exception is not None
    assert type(result.exception) is TopicConfigNotValidException


@pytest.mark.integration
def test_topic_creation_with_malformed_key_fails(
        monkeypatch,
        topic_controller: TopicController,
        confluent_admin_client: confluent_kafka.admin.AdminClient,
        topic: str,
):

    topics = confluent_admin_client.list_topics(timeout=5).topics.keys()
    assert topic in topics

    config_dict = {
        "config": {
            "cleanup.policy": "foo_bar_baz"
        }
    }

    def mock_edit_function(text=None, editor=None, env=None, require_save=True, extension=".txt", filename=None):
        return yaml.dump(config_dict, default_flow_style=False)
    monkeypatch.setattr(click, "edit", mock_edit_function)
    result = CliRunner().invoke(edit_topic, topic, input="y\n", catch_exceptions=True)

    assert result.exception is not None
    assert type(result.exception) is TopicConfigNotValidException
