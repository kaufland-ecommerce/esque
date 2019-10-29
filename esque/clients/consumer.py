import pathlib
from abc import ABC, abstractmethod
from typing import Dict, Optional, Tuple

import confluent_kafka
import pendulum
from confluent_kafka.cimpl import Message, TopicPartition

from esque.clients.schemaregistry import SchemaRegistryClient
from esque.config import Config
from esque.errors import EndOfPartitionReachedException, MessageEmptyException, raise_for_kafka_error, raise_for_message, translate_third_party_exceptions
from esque.messages.avromessage import AvroFileWriter, StdOutAvroWriter
from esque.messages.message import FileWriter, GenericWriter, PlainTextFileWriter, StdOutWriter
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
        self._use_single_directory_for_messages = False
        self.writers: Dict[int, GenericWriter] = {}

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
        raise NotImplementedError()

    @translate_third_party_exceptions
    def assign_specific_partitions(self, topic_name: str, partitions: list = None, offset: int = 0):
        self._topic_name = topic_name
        if partitions is not None:
            topic_partitions = [
                TopicPartition(self._topic_name, partition=partition, offset=offset) for partition in partitions
            ]
        else:
            topic_partitions = [TopicPartition(self._topic_name, partition=0, offset=offset)]
        self._consumer.assign(topic_partitions)

    @translate_third_party_exceptions
    def _subscribe(self, topic: str) -> None:
        self._consumer.subscribe([topic])

    @abstractmethod
    def consume(self, **kwargs) -> int:
        pass

    def output_consumed(self, message: Message):
        """
        Outputs the message to a destination determined by the implementation of the inheriting class.
        :param message: Message to output
        :return: This method returns no values
        """
        writer = self.writers.get(message.partition(), self.writers[-1])
        writer.write_message(message)

    def close_all_writers(self):
        for _, w in self.writers.items():
            if isinstance(w, FileWriter) and w.file is not None:
                w.file.close()

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
        return message if message_acceptable else None

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


class PlaintextConsumer(AbstractConsumer):
    def __init__(self, group_id: str, topic_name: str, working_dir: pathlib.Path, last: bool, match: str = None):
        super().__init__(group_id, topic_name, last, match)
        self.working_dir = working_dir
        super()._setup_config()

    @translate_third_party_exceptions
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

        self.close_all_writers()
        return counter

    def create_internal_consumer(self):
        super()._setup_config()
        self._consumer = confluent_kafka.Consumer(self._config)
        if self._topic_name is not None:
            self._subscribe(self._topic_name)

    def output_consumed(self, message: Message):
        if (
            self.working_dir
            and not self._use_single_directory_for_messages
            and message.partition() not in self.writers
        ):
            writer = PlainTextFileWriter(self.working_dir / f"partition_{message.partition()}")
            writer.init_destination_directory()
            self.writers[message.partition()] = writer
        else:
            writer = self.writers.get(message.partition(), self.writers[-1])
        writer.write_message(message)


class AvroFileConsumer(PlaintextConsumer):
    def __init__(self, group_id: str, topic_name: str, working_dir: pathlib.Path, last: bool, match: str = None):
        super().__init__(group_id, topic_name, working_dir, last, match)
        self.schema_registry_client = SchemaRegistryClient(Config().schema_registry)

    def create_internal_consumer(self):
        super()._setup_config()
        super().create_internal_consumer()

    def output_consumed(self, message: Message):
        if (
            self.working_dir
            and not self._use_single_directory_for_messages
            and message.partition() not in self.writers
        ):
            writer = AvroFileWriter(self.working_dir / f"partition_{message.partition()}", self.schema_registry_client)
            writer.init_destination_directory()
            self.writers[message.partition()] = writer
        else:
            writer = self.writers.get(message.partition(), self.writers[-1])

        writer.write_message(message)


class ConsumerFactory:
    def create_consumer(
        self,
        group_id: str,
        topic_name: str,
        working_dir: pathlib.Path,
        last: bool,
        avro: bool,
        merge_partitions_to_single_file: bool = False,
        match: str = None,
    ):
        if avro:
            consumer = AvroFileConsumer(
                group_id=group_id, topic_name=topic_name, working_dir=working_dir, last=last, match=match
            )
            consumer.writers[-1] = (
                StdOutAvroWriter(schema_registry_client=consumer.schema_registry_client)
                if working_dir is None
                else AvroFileWriter(consumer.working_dir / "partition_any", consumer.schema_registry_client)
            )
        else:
            consumer = PlaintextConsumer(
                group_id=group_id, topic_name=topic_name, working_dir=working_dir, last=last, match=match
            )
            consumer.writers[-1] = (
                StdOutWriter() if working_dir is None else PlainTextFileWriter(consumer.working_dir / "partition_any")
            )
        consumer._use_single_directory_for_messages = merge_partitions_to_single_file
        if merge_partitions_to_single_file and consumer.working_dir is not None:
            consumer.writers[-1].init_destination_directory()

        consumer.create_internal_consumer()
        return consumer

    def create_ping_consumer(self, group_id: str, topic_name: str):
        consumer = PingConsumer(group_id, topic_name, last=False)
        consumer._uses_file_output = False
        consumer.writers[0] = StdOutWriter()
        consumer.create_internal_consumer()
        return consumer
