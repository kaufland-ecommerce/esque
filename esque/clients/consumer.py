import pathlib
from abc import ABC, abstractmethod
from contextlib import ExitStack
from os import path
from typing import Optional, Tuple

import confluent_kafka
import pendulum
from confluent_kafka.cimpl import Message, TopicPartition

from esque.clients.schemaregistry import SchemaRegistryClient
from esque.config import Config
from esque.errors import (
    MessageEmptyException,
    raise_for_kafka_error,
    raise_for_message,
    translate_third_party_exceptions,
)
from esque.messages.avromessage import AvroFileWriter
from esque.messages.message import decode_message
from esque.messages.message import FileWriter, PlainTextFileWriter
from esque.ruleparser.ruleengine import RuleTree


class AbstractConsumer(ABC):
    def __init__(self, group_id: str, topic_name: str, last: bool, match: str = None):
        offset_reset = "earliest"
        if last:
            offset_reset = "latest"
        self._match = match
        if self._match is not None:
            self._rule_tree = RuleTree(match)
        else:
            self._rule_tree = None

        self._config = Config().create_confluent_config()
        self._config.update(
            {
                "group.id": group_id,
                "error_cb": raise_for_kafka_error,
                # We need to commit offsets manually once we"re sure it got saved
                # to the sink
                "enable.auto.commit": True,
                "enable.partition.eof": True,
                # We need this to start at the last committed offset instead of the
                # latest when subscribing for the first time
                "default.topic.config": {"auto.offset.reset": offset_reset},
            }
        )
        self._consumer = confluent_kafka.Consumer(self._config)
        self._topic_name = topic_name

    @translate_third_party_exceptions
    def assign_specific_partitions(self, topic_name: str, partitions: list):
        self._topic_name = topic_name
        if partitions is not None:
            topic_partitions = [TopicPartition(self._topic_name, partition) for partition in partitions]
            self._consumer.assign(topic_partitions)

    @translate_third_party_exceptions
    def _subscribe(self, topic: str) -> None:
        self._consumer.subscribe([topic])

    @abstractmethod
    def consume(self, **kwargs) -> int:
        pass

    @translate_third_party_exceptions
    def consume_single_message(self, timeout=30) -> Message:
        message = self._consumer.poll(timeout=timeout)
        raise_for_message(message)
        return message

    @translate_third_party_exceptions
    def consume_single_acceptable_message(self, timeout=30) -> Message:
        message_acceptable = False
        while not message_acceptable:
            message = self._consumer.poll(timeout=timeout)
            raise_for_message(message)
            decoded_message = decode_message(message)
            message_acceptable = self.consumed_message_matches(decoded_message)
        return message

    @translate_third_party_exceptions
    def consumed_message_matches(self, message: Message):
        if self._rule_tree is not None:
            return self._rule_tree.evaluate(message)
        else:
            return True


class PingConsumer(AbstractConsumer):
    def __init__(self, group_id: str, topic_name: str, last: bool):
        super().__init__(group_id, topic_name, last)
        self._assign_exact_partitions(topic_name)

    @translate_third_party_exceptions
    def consume(self) -> Optional[Tuple[str, int]]:
        message = self.consume_single_message(timeout=1)
        msg_sent_at = pendulum.from_timestamp(float(message.value()))
        delta_sent = pendulum.now() - msg_sent_at
        return message.key(), delta_sent.microseconds / 1000

    @translate_third_party_exceptions
    def _assign_exact_partitions(self, topic: str) -> None:
        self._consumer.assign([TopicPartition(topic=topic, partition=0, offset=0)])


class FileConsumer(AbstractConsumer):
    def __init__(self, group_id: str, topic_name: str, working_dir: pathlib.Path, last: bool, match: str = None):
        super().__init__(group_id, topic_name, last, match)
        self.working_dir = working_dir
        offset_reset = "earliest"
        if last:
            offset_reset = "latest"

        self._config.update({"default.topic.config": {"auto.offset.reset": offset_reset}})
        self._consumer = confluent_kafka.Consumer(self._config)
        if topic_name is not None:
            self._subscribe(topic_name)

    @translate_third_party_exceptions
    def consume(self, amount: int) -> int:
        counter = 0
        file_writers = {}

        with ExitStack() as stack:
            while counter < amount:
                try:
                    message = self.consume_single_message()
                except MessageEmptyException:
                    return counter

                if message.partition() not in file_writers:
                    partition = message.partition()
                    file_writer = self.get_file_writer(partition)
                    stack.enter_context(file_writer)
                    file_writers[partition] = file_writer

                file_writer = file_writers[partition]
                file_writer.write_message_to_file(message)
                counter += 1

        return counter

    def get_file_writer(self, partition: int) -> FileWriter:
        path_suffix = "partition_any" if partition < 0 else f"partition_{partition}"
        return PlainTextFileWriter((self.working_dir / path_suffix))


class AvroFileConsumer(FileConsumer):
    def __init__(self, group_id: str, topic_name: str, working_dir: pathlib.Path, last: bool, match: str = None):
        super().__init__(group_id, topic_name, working_dir, last, match)
        self.schema_registry_client = SchemaRegistryClient(Config().schema_registry)
        if topic_name is not None:
            self._subscribe(topic_name)

    def get_file_writer(self, partition: int) -> FileWriter:
        path_suffix = "partition_any" if partition < 0 else f"partition_{partition}"
        return AvroFileWriter((self.working_dir / path_suffix), self.schema_registry_client)
