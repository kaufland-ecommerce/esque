from typing import Dict

import pytest
from pytest_cases import fixture

from esque.errors import ValidationException
from esque.validation import validate_editable_topic_config


@fixture
def valid_config() -> Dict:
    return {
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


def test_config_with_valid_keys_works(valid_config):
    validate_editable_topic_config(valid_config)


def test_config_with_unknown_key_fails(valid_config):
    valid_config["config"]["foo.bar.baz"] = "true"
    with pytest.raises(ValidationException):
        validate_editable_topic_config(valid_config)


def test_topic_creation_with_malformed_value_fails(valid_config):
    valid_config["config"]["segment.ms"] = "foo_bar_baz"
    with pytest.raises(ValidationException):
        validate_editable_topic_config(valid_config)
