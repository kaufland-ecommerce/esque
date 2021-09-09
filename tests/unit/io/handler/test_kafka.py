import itertools
from typing import List, Optional, Type
from unittest import mock
from unittest.mock import Mock

import pytest
from confluent_kafka import Consumer, KafkaError, Producer
from confluent_kafka.admin import ClusterMetadata, TopicMetadata

from esque.io.handlers.kafka import KafkaHandler, KafkaHandlerConfig
from esque.io.messages import BinaryMessage
from esque.io.stream_events import TemporaryEndOfPartition


@pytest.fixture(autouse=True)
def consumer_cls_mock(topic_id: str, binary_messages: List[BinaryMessage]):
    with mock.patch("esque.io.handlers.kafka.Consumer", autospec=True) as mocked_cls:
        mocked_instance = mocked_cls({})
        cluster_meta = generate_value_for_list_topics(binary_messages, topic_id)
        mocked_instance.list_topics.return_value = cluster_meta
        yield mocked_cls


def generate_value_for_list_topics(binary_messages: List[BinaryMessage], topic_id: str) -> ClusterMetadata:
    cluster_meta = ClusterMetadata()
    topic_meta = TopicMetadata()
    topic_meta.partitions = {msg.partition: None for msg in binary_messages}
    cluster_meta.topics = {topic_id: topic_meta}
    return cluster_meta


@pytest.fixture(autouse=True)
def producer_cls_mock():
    with mock.patch("esque.io.handlers.kafka.Producer", autospec=True) as mocked_cls:
        yield mocked_cls


@pytest.fixture()
def kafka_handler(unittest_config, topic_id: str):
    return KafkaHandler(
        KafkaHandlerConfig(host="docker", path=topic_id, scheme="kafka", consumer_group_id="test_consumer")
    )


def test_write_single_message(
    producer_cls_mock: Type[Producer], binary_messages: List[BinaryMessage], kafka_handler: KafkaHandler, topic_id: str
):
    message = binary_messages[0]
    kafka_handler.write_message(message)

    producer_mock: Producer = producer_cls_mock(config={})
    producer_mock.produce.assert_called_once_with(
        key=message.key, value=message.value, topic=topic_id, partition=message.partition
    )
    producer_mock.flush.assert_called_once()


def test_write_many_messages(
    producer_cls_mock: Type[Producer], binary_messages: List[BinaryMessage], kafka_handler: KafkaHandler, topic_id: str
):
    kafka_handler.write_many_messages(binary_messages)

    producer_mock: Producer = producer_cls_mock(config={})
    for message in binary_messages:
        producer_mock.produce.assert_any_call(
            key=message.key, value=message.value, topic=topic_id, partition=message.partition
        )
    producer_mock.flush.assert_called_once()


def test_read_message(
    binary_messages: List[BinaryMessage], consumer_cls_mock: Type[Consumer], topic_id: str, kafka_handler: KafkaHandler
):
    message = binary_messages[0]
    confluent_message = binary_message_to_confluent_message(message, topic_id)
    consumer_mock = consumer_cls_mock({})
    consumer_mock.poll.return_value = confluent_message

    assert kafka_handler.read_message() == message


def test_read_many_messages(
    binary_messages: List[BinaryMessage], consumer_cls_mock: Type[Consumer], topic_id: str, kafka_handler: KafkaHandler
):
    confluent_messages = [binary_message_to_confluent_message(message, topic_id) for message in binary_messages]
    consumer_mock = consumer_cls_mock({})
    consumer_mock.poll.side_effect = confluent_messages

    # make sure message_stream doesn't yield less than len(binary_message) items
    message_stream = itertools.chain(kafka_handler.message_stream(), itertools.repeat(None))
    for expected_message, actual_message in zip(binary_messages, message_stream):
        assert expected_message == actual_message


def test_temporary_end_of_stream_events_non_streaming(
    binary_messages: List[BinaryMessage], consumer_cls_mock: Type[Consumer], topic_id: str, kafka_handler: KafkaHandler
):
    partitions = set(msg.partition for msg in binary_messages)
    consumer_mock = consumer_cls_mock({})

    poll_return_values: List[Optional[Mock]] = [
        confluent_eof_message(topic_id, partition, offset=0) for partition in partitions
    ]
    poll_return_values.append(None)
    consumer_mock.poll.side_effect = poll_return_values

    for partition_id in partitions:
        stream_event = kafka_handler.read_message()
        assert isinstance(stream_event, TemporaryEndOfPartition)
        assert stream_event.partition_id == partition_id

    stream_event = kafka_handler.read_message()
    assert isinstance(stream_event, TemporaryEndOfPartition)
    assert stream_event.partition_id == TemporaryEndOfPartition.ALL_PARTITIONS


def test_temporary_end_of_stream_events_streaming(
    binary_messages: List[BinaryMessage], consumer_cls_mock: Type[Consumer], topic_id: str, kafka_handler: KafkaHandler
):
    partitions = set(msg.partition for msg in binary_messages)
    consumer_mock = consumer_cls_mock({})

    poll_return_values: List[Optional[Mock]] = [
        confluent_eof_message(topic_id, partition, offset=0) for partition in partitions
    ]
    poll_return_values.append(None)
    consumer_mock.poll.side_effect = poll_return_values

    stream_iterator = iter(kafka_handler.message_stream())

    for partition_id in partitions:
        stream_event = next(stream_iterator)
        assert isinstance(stream_event, TemporaryEndOfPartition)
        assert stream_event.partition_id == partition_id

    stream_event = next(stream_iterator)
    assert isinstance(stream_event, TemporaryEndOfPartition)
    assert stream_event.partition_id == TemporaryEndOfPartition.ALL_PARTITIONS


def confluent_eof_message(topic_id: str, partition: int, offset: int):
    confluent_message = Mock()
    confluent_message.key.return_value = None
    confluent_message.value.return_value = None
    confluent_message.topic.return_value = topic_id
    confluent_message.partition.return_value = partition
    confluent_message.offset.return_value = offset

    error_mock = Mock()
    error_mock.code.return_value = KafkaError._PARTITION_EOF
    confluent_message.error.return_value = error_mock
    return confluent_message


def binary_message_to_confluent_message(message: BinaryMessage, topic_id: str):
    confluent_message = Mock()
    confluent_message.key.return_value = message.key
    confluent_message.value.return_value = message.value
    confluent_message.topic.return_value = topic_id
    confluent_message.partition.return_value = message.partition
    confluent_message.offset.return_value = message.offset
    confluent_message.error.return_value = None
    return confluent_message
