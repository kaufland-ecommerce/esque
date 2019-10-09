import pathlib
from abc import ABC, abstractmethod
from contextlib import ExitStack
from typing import Optional, Tuple

import confluent_kafka
import pendulum
from confluent_kafka.cimpl import Message, TopicPartition

from esque.clients.schemaregistry import SchemaRegistryClient
from esque.config import Config
from esque.errors import MessageEmptyException, raise_for_kafka_error, raise_for_message
from esque.messages.avromessage import AvroFileWriter
from esque.messages.message import FileWriter, PlainTextFileWriter
from esque.ruleparser.ruleengine import RuleTree


class AbstractConsumer(ABC):
    def __init__(self, group_id: str, topic_name: str, last: bool, match: str = None, **kwargs):
        offset_reset = "earliest"
        if last:
            offset_reset = "latest"
        if match is not None:
            self.rule_tree = RuleTree(match)

        self._config = Config().create_confluent_config()
        self._config.update(
            {
                "group.id": group_id,
                "error_cb": raise_for_kafka_error,
                # We need to commit offsets manually once we"re sure it got saved
                # to the sink
                "enable.auto.commit": True,
                "enable.partition.eof": False,
                # We need this to start at the last committed offset instead of the
                # latest when subscribing for the first time
                "default.topic.config": {"auto.offset.reset": offset_reset},
            }
        )
        self._consumer = confluent_kafka.Consumer(self._config)
        self._topic_name = topic_name

    def _subscribe(self, topic: str) -> None:
        self._consumer.subscribe([topic])

    @abstractmethod
    def consume(self, **kwargs) -> int:
        pass

    def _consume_single_message(self, timeout=30) -> Message:
        message = self._consumer.poll(timeout=timeout)
        raise_for_message(message)
        return message

    def consumed_message_matches(self, message: Message):
        if self.rule_tree is not None:
            return self.rule_tree.evaluate(message)
        else:
            return True

    def _assign_exact_partitions(self, topic: str, *, offset: int = 0, partition: int = 0) -> None:
        self._consumer.assign([TopicPartition(topic=topic, partition=partition, offset=offset)])


class MessageConsumer(AbstractConsumer):
    def __init__(self, group_id: str, topic_name: str, last: bool, *, starting_offset=0, partition=0):
        super().__init__(group_id, topic_name, last)
        self._assign_exact_partitions(topic_name, offset=starting_offset, partition=partition)

    def consume(self) -> Optional[Tuple[str, int]]:
        message = self._consume_single_message(timeout=1)
        return message


class PingConsumer(AbstractConsumer):
    def __init__(self, group_id: str, topic_name: str, last: bool):
        super().__init__(group_id, topic_name, last)
        self._assign_exact_partitions(topic_name)

    def consume(self) -> Optional[Tuple[str, int]]:
        message = self._consume_single_message(timeout=1)
        msg_sent_at = pendulum.from_timestamp(float(message.value()))
        delta_sent = pendulum.now() - msg_sent_at
        return message.key(), delta_sent.microseconds / 1000


class FileConsumer(AbstractConsumer):
    def __init__(self, group_id: str, topic_name: str, working_dir: pathlib.Path, last: bool):
        super().__init__(group_id, topic_name, last)
        self.working_dir = working_dir
        offset_reset = "earliest"
        if last:
            offset_reset = "latest"

        self._config.update({"default.topic.config": {"auto.offset.reset": offset_reset}})
        self._consumer = confluent_kafka.Consumer(self._config)
        self._subscribe(topic_name)

    def consume(self, amount: int) -> int:
        counter = 0
        file_writers = {}

        with ExitStack() as stack:
            while counter < amount:
                try:
                    message = self._consume_single_message()
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
        return PlainTextFileWriter((self.working_dir / f"partition_{partition}"))


class AvroFileConsumer(FileConsumer):
    def __init__(self, group_id: str, topic_name: str, working_dir: pathlib.Path, last: bool):
        super().__init__(group_id, topic_name, working_dir, last)
        self.schema_registry_client = SchemaRegistryClient(Config().schema_registry)
        self._subscribe(topic_name)

    def get_file_writer(self, partition: int) -> FileWriter:
        return AvroFileWriter((self.working_dir / f"partition_{partition}"), self.schema_registry_client)
