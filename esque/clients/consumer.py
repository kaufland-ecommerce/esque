import pathlib
from abc import ABC
from abc import abstractmethod
from contextlib import ExitStack
from typing import Optional
from typing import Tuple

import click
import confluent_kafka
import pendulum
from confluent_kafka.cimpl import Message
from confluent_kafka.cimpl import TopicPartition

from esque.clients.schemaregistry import SchemaRegistryClient
from esque.config import Config
from esque.errors import EndOfPartitionReachedException
from esque.errors import MessageEmptyException
from esque.errors import raise_for_kafka_error
from esque.errors import raise_for_message
from esque.errors import translate_third_party_exceptions
from esque.messages.avromessage import AvroFileWriter
from esque.messages.message import FileWriter
from esque.messages.message import PlainTextFileWriter
from esque.messages.message import serialize_message
from esque.ruleparser.ruleengine import RuleTree


class AbstractConsumer(ABC):
    def __init__(self, group_id: str, topic_name: str, last: bool, match: str = None):
        self._match = match
        self._last = last
        self._group_id = group_id
        self._consumer = None
        self._config = Config().create_confluent_config()
        if self._match is not None:
            self._rule_tree = RuleTree(match)
        else:
            self._rule_tree = None
        self._topic_name = topic_name

    @translate_third_party_exceptions
    def _setup_config(self):
        offset_reset = "earliest"
        if self._last:
            offset_reset = "latest"
        self._config.update(
            {
                "group.id": self._group_id,
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

    @abstractmethod
    def create_internal_consumer(self):
        pass

    @translate_third_party_exceptions
    def assign_specific_partitions(self, topic_name: str, partitions: list = None, offset: int = 0):
        self._topic_name = topic_name
        if partitions is not None:
            topic_partitions = [
                TopicPartition(self._topic_name, partition=partition, offset=offset) for partition in partitions
            ]
        else:
            topic_partitions = [TopicPartition(self._topic_name, 0, 0)]
        self._consumer.assign(topic_partitions)

    @translate_third_party_exceptions
    def _subscribe(self, topic: str) -> None:
        self._consumer.subscribe([topic])

    @abstractmethod
    def consume(self, **kwargs) -> int:
        pass

    @abstractmethod
    def output_consumed(self, message: Message, file_writer: FileWriter = None):
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
            message_acceptable = self.consumed_message_matches(message)
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
        self._topic_name = topic_name
        self._consumer = None

    @translate_third_party_exceptions
    def consume(self) -> Optional[Tuple[str, int]]:
        message = self.consume_single_message(timeout=10)
        msg_sent_at = pendulum.from_timestamp(float(message.value()))
        delta_sent = pendulum.now() - msg_sent_at
        return message.key(), delta_sent.microseconds / 1000

    def create_internal_consumer(self):
        self._setup_config()
        self._consumer = confluent_kafka.Consumer(self._config)
        self.assign_specific_partitions(self._topic_name, partitions=[0], offset=0)

    def _setup_config(self):
        offset_reset = "earliest"
        if self._last:
            offset_reset = "latest"
        self._config.update(
            {
                "group.id": self._group_id,
                "error_cb": raise_for_kafka_error,
                "enable.auto.commit": True,
                "enable.partition.eof": False,
                "default.topic.config": {"auto.offset.reset": offset_reset},
            }
        )

    def output_consumed(self, message: Message, file_writer: FileWriter = None):
        pass


class StdOutConsumer(AbstractConsumer):
    def __init__(self, group_id: str, topic_name: str, last: bool, match: str = None):
        super().__init__(group_id, topic_name, last, match)
        super()._setup_config()

    def consume(self, amount: int) -> int:
        counter = 0
        while counter < amount:
            try:
                message = self.consume_single_message(10)
            except MessageEmptyException:
                return counter
            except EndOfPartitionReachedException:
                pass
            else:
                counter += 1
                self.output_consumed(message)

        return counter

    def create_internal_consumer(self):
        super()._setup_config()
        self._consumer = confluent_kafka.Consumer(self._config)
        if self._topic_name is not None:
            self._subscribe(self._topic_name)

    def output_consumed(self, message: Message, file_writer: FileWriter = None):
        click.echo(serialize_message(message))


class FileConsumer(AbstractConsumer):
    def __init__(self, group_id: str, topic_name: str, working_dir: pathlib.Path, last: bool, match: str = None):
        super().__init__(group_id, topic_name, last, match)
        self.working_dir = working_dir
        self.file_writers = {}
        super()._setup_config()

    @translate_third_party_exceptions
    def consume(self, amount: int) -> int:
        counter = 0
        file_writers = {}

        with ExitStack() as stack:
            while counter < amount:
                try:
                    message = self.consume_single_message(10)
                except MessageEmptyException:
                    return counter
                except EndOfPartitionReachedException:
                    pass
                else:
                    counter += 1
                    if message.partition() not in file_writers:
                        partition = message.partition()
                        file_writer = self.get_file_writer(partition)
                        stack.enter_context(file_writer)
                        file_writers[partition] = file_writer

                    self.output_consumed(message, file_writers[partition])

        return counter

    def create_internal_consumer(self):
        super()._setup_config()
        self._consumer = confluent_kafka.Consumer(self._config)
        if self._topic_name is not None:
            self._subscribe(self._topic_name)

    def output_consumed(self, message: Message, file_writer: FileWriter = None):
        file_writer.write_message_to_file(message)

    def get_file_writer(self, partition: int) -> FileWriter:
        path_suffix = "partition_any" if partition < 0 else f"partition_{partition}"
        return PlainTextFileWriter((self.working_dir / path_suffix))


class AvroFileConsumer(FileConsumer):
    def __init__(self, group_id: str, topic_name: str, working_dir: pathlib.Path, last: bool, match: str = None):
        super().__init__(group_id, topic_name, working_dir, last, match)
        self.schema_registry_client = SchemaRegistryClient(Config().schema_registry)

    def create_internal_consumer(self):
        super()._setup_config()
        super().create_internal_consumer()

    def get_file_writer(self, partition: int) -> FileWriter:
        path_suffix = "partition_any" if partition < 0 else f"partition_{partition}"
        return AvroFileWriter((self.working_dir / path_suffix), self.schema_registry_client)


class ConsumerFactory:
    def create_consumer(
        self, group_id: str, topic_name: str, working_dir: pathlib.Path, last: bool, avro: bool, match: str = None
    ):
        if working_dir is None:
            consumer = StdOutConsumer(group_id=group_id, topic_name=topic_name, last=last, match=match)
        elif avro:
            consumer = AvroFileConsumer(
                group_id=group_id, topic_name=topic_name, working_dir=working_dir, last=last, match=match
            )
        else:
            consumer = FileConsumer(
                group_id=group_id, topic_name=topic_name, working_dir=working_dir, last=last, match=match
            )
        consumer.create_internal_consumer()
        return consumer

    def create_ping_consumer(self, group_id: str, topic_name: str):
        consumer = PingConsumer(group_id, topic_name, last=False)
        consumer.create_internal_consumer()
        return consumer
