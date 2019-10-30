import pathlib
from abc import ABC, abstractmethod
from typing import Dict, Optional, Tuple

import confluent_kafka
import pendulum
from confluent_kafka.cimpl import Message, TopicPartition

from esque.clients.schemaregistry import SchemaRegistryClient
from esque.config import Config
from esque.errors import (
    EndOfPartitionReachedException,
    MessageEmptyException,
    raise_for_kafka_error,
    raise_for_message,
    translate_third_party_exceptions,
)
from esque.messages.avromessage import AvroFileWriter, StdOutAvroWriter
from esque.messages.message import FileWriter, GenericWriter, PlainTextFileWriter, StdOutWriter, decode_message
from esque.ruleparser.ruleengine import RuleTree


class AbstractConsumer(ABC):
    def __init__(self, group_id: str, topic_name: str, last: bool, match: str = None):
        self._match = match
        self._last = last
        self._group_id = group_id
        self._consumer = None
        self._config = {}
        if self._match is not None:
            self._rule_tree = RuleTree(match)
        else:
            self._rule_tree = None
        self._topic_name = topic_name
        self.writers: Dict[int, GenericWriter] = {}
        self._setup_config()
        self.create_internal_consumer()

    def _setup_config(self):
        offset_reset = "earliest"
        if self._last:
            offset_reset = "latest"
        self._config = Config().create_confluent_config()
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
        raise NotImplementedError()

    def output_consumed(self, message: Message):
        """
        Outputs the message to a destination determined by the implementation of the inheriting class.
        :param message: Message to output
        :return: This method returns no values
        """
        writer = self.writers.get(message.partition(), self.writers[-1])
        writer.write_message(message)

    def close_all_writers(self):
        for w in self.writers.values():
            if isinstance(w, FileWriter) and w.file is not None:
                w.file.close()

    @translate_third_party_exceptions
    def consume_single_message(self, timeout=30) -> Message:
        message = self._consumer.poll(timeout=timeout)
        raise_for_message(message)
        return message

    @translate_third_party_exceptions
    def consume_single_acceptable_message(self, timeout=30) -> Optional[Message]:
        message_acceptable = False
        total_time_remaining = timeout
        while not message_acceptable and total_time_remaining > 0:
            iteration_start = pendulum.now()
            message = self.consume_single_message(timeout=timeout)
            total_time_remaining -= (pendulum.now() - iteration_start).in_seconds()
            message_acceptable = self.consumed_message_matches(message)
        return message if message_acceptable else None

    @translate_third_party_exceptions
    def consumed_message_matches(self, message: Message):
        if self._rule_tree is not None:
            return self._rule_tree.evaluate(message)
        else:
            return True


class PingConsumer(AbstractConsumer):
    @translate_third_party_exceptions
    def consume(self) -> Optional[Tuple[str, int]]:
        message = self.consume_single_message(timeout=10)
        msg_sent_at = pendulum.from_timestamp(float(message.value()))
        delta_sent = pendulum.now() - msg_sent_at
        return message.key(), delta_sent.microseconds / 1000

    def create_internal_consumer(self):
        self._consumer = confluent_kafka.Consumer(self._config)
        self.assign_specific_partitions(self._topic_name, partitions=[0], offset=0)

    def _setup_config(self):
        super()._setup_config()
        self._config["enable.partition.eof"] = False


class PlaintextConsumer(AbstractConsumer):
    def __init__(
        self,
        group_id: str,
        topic_name: str,
        working_dir: pathlib.Path,
        last: bool,
        match: str = None,
        initialize_default_output_directory: bool = False,
    ):
        super().__init__(group_id, topic_name, last, match)
        self.working_dir = working_dir
        self.writers[-1] = (
            StdOutWriter() if working_dir is None else PlainTextFileWriter(self.working_dir / "partition_any")
        )
        self._initialize_default_output_directory = initialize_default_output_directory
        if self._initialize_default_output_directory and self.working_dir is not None:
            self.writers[-1].init_destination_directory()

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
        self._consumer = confluent_kafka.Consumer(self._config)
        if self._topic_name is not None:
            self._subscribe(self._topic_name)

    def output_consumed(self, message: Message):
        if (
            self.working_dir
            and not self._initialize_default_output_directory
            and message.partition() not in self.writers
        ):
            writer = PlainTextFileWriter(self.working_dir / f"partition_{message.partition()}")
            writer.init_destination_directory()
            self.writers[message.partition()] = writer
        else:
            writer = self.writers.get(message.partition(), self.writers[-1])
        writer.write_message(message)


class AvroFileConsumer(PlaintextConsumer):
    def __init__(
        self,
        group_id: str,
        topic_name: str,
        working_dir: pathlib.Path,
        last: bool,
        match: str = None,
        initialize_default_output_directory: bool = False,
    ):
        super().__init__(group_id, topic_name, working_dir, last, match, initialize_default_output_directory)
        self.schema_registry_client = SchemaRegistryClient(Config().schema_registry)
        self.writers[-1] = (
            StdOutAvroWriter(schema_registry_client=self.schema_registry_client)
            if working_dir is None
            else AvroFileWriter(self.working_dir / "partition_any", self.schema_registry_client)
        )
        if self._initialize_default_output_directory and self.working_dir is not None:
            self.writers[-1].init_destination_directory()

    def output_consumed(self, message: Message):
        if (
            self.working_dir
            and not self._initialize_default_output_directory
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
        initialize_default_output_directory: bool = False,
        match: str = None,
    ):
        """
        Creates a Kafka consumer
        :param group_id: ID of the consumer group
        :param topic_name: Topic name for the new consumer
        :param working_dir: The directory to store the consumed messages (if None, than STDOUT is used)
        :param last: Start consuming from the latest committed offset
        :param avro: Are messages in Avro format?
        :param initialize_default_output_directory: If set to true, all messages will be stored in a directory named partition_any, instead having a separate directory for each partition. This argument is only used if working_dir is not None.
        :param match: Match expression for message filtering
        :return: Consumer object
        """
        if avro:
            consumer = AvroFileConsumer(
                group_id=group_id,
                topic_name=topic_name,
                working_dir=working_dir,
                last=last,
                match=match,
                initialize_default_output_directory=initialize_default_output_directory,
            )
        else:
            consumer = PlaintextConsumer(
                group_id=group_id,
                topic_name=topic_name,
                working_dir=working_dir,
                last=last,
                match=match,
                initialize_default_output_directory=initialize_default_output_directory,
            )
        return consumer

    def create_ping_consumer(self, group_id: str, topic_name: str):
        consumer = PingConsumer(group_id, topic_name, last=False)
        return consumer


def consume_to_file_ordered(
    working_dir: pathlib.Path,
    topic: str,
    group_id: str,
    partitions: list,
    numbers: int,
    avro: bool,
    match: str,
    last: bool,
    write_to_stdout: bool = False,
) -> int:
    consumers = []
    factory = ConsumerFactory()
    for partition in partitions:
        consumer = factory.create_consumer(
            group_id=group_id + "_" + str(partition),
            topic_name=None,
            working_dir=None if write_to_stdout else working_dir,
            avro=avro,
            match=match,
            last=last,
            initialize_default_output_directory=True,
        )
        consumer.assign_specific_partitions(topic, [partition])
        consumers.append(consumer)

    messages_by_partition = {}
    partitions_by_timestamp = {}
    total_number_of_messages = 0
    messages_left = True
    # get at least one message from each partition, or exclude those that don't have any messages
    for partition_counter in range(0, len(consumers)):
        max_retry_count = 5
        keep_polling_current_partition = True
        while keep_polling_current_partition:
            try:
                message = consumers[partition_counter].consume_single_acceptable_message(timeout=10)
                decoded_message = decode_message(message)
            except MessageEmptyException:
                # a possible timeout due to a network issue, retry (but not more than max_retry_count attempts)
                max_retry_count -= 1
                if max_retry_count <= 0:
                    partitions.remove(partition_counter)
                    if len(partitions) == 0:
                        messages_left = False
                    keep_polling_current_partition = False
            except EndOfPartitionReachedException:
                keep_polling_current_partition = False
                partitions.remove(partition_counter)
                if len(partitions) == 0:
                    messages_left = False
            else:
                keep_polling_current_partition = False
                partitions_by_timestamp[decoded_message.timestamp] = decoded_message.partition
                if decoded_message.partition not in messages_by_partition:
                    messages_by_partition[decoded_message.partition] = []
                messages_by_partition[decoded_message.partition].append(message)

    # in each iteration, take the earliest message from the map, output it and replace it with a new one (if available)
    # if not, remove the consumer and move to the next one
    while total_number_of_messages < numbers and messages_left:
        if len(partitions_by_timestamp) == 0:
            messages_left = False
        else:
            first_key = sorted(partitions_by_timestamp.keys())[0]
            partition = partitions_by_timestamp[first_key]

            message = messages_by_partition[partition].pop(0)
            consumers[0].output_consumed(message)
            del partitions_by_timestamp[first_key]
            total_number_of_messages += 1

            try:
                message = consumers[partition].consume_single_acceptable_message(timeout=10)
                decoded_message = decode_message(message)
                partitions_by_timestamp[decoded_message.timestamp] = partition
                messages_by_partition[partition].append(message)
            except (MessageEmptyException, EndOfPartitionReachedException):
                partitions.remove(partition)
                messages_by_partition.pop(partition, None)
                if len(partitions) == 0:
                    messages_left = False
    for c in consumers:
        c.close_all_writers()
    return total_number_of_messages


def consume_to_files(
    working_dir: pathlib.Path,
    topic: str,
    group_id: str,
    numbers: int,
    avro: bool,
    match: str,
    last: bool,
    write_to_stdout: bool = False,
) -> int:
    consumer = ConsumerFactory().create_consumer(
        group_id=group_id,
        topic_name=topic,
        working_dir=working_dir if not write_to_stdout else None,
        last=last,
        avro=avro,
        match=match,
        initialize_default_output_directory=False,
    )
    number_consumed_messages = consumer.consume(int(numbers))
    consumer.close_all_writers()
    return number_consumed_messages
