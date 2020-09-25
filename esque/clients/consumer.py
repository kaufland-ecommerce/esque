import logging
import pathlib
from abc import ABC, abstractmethod
from heapq import heappop, heappush
from typing import Dict, List, Optional, Tuple

import confluent_kafka
import pendulum
from confluent_kafka.cimpl import Message, TopicPartition

from esque.clients.schemaregistry import SchemaRegistryClient
from esque.config import Config
from esque.errors import EndOfPartitionReachedException, MessageEmptyException, raise_for_message
from esque.helpers import log_error
from esque.messages.avromessage import AvroFileWriter, StdOutAvroWriter
from esque.messages.message import FileWriter, GenericWriter, PlainTextFileWriter, StdOutWriter, decode_message
from esque.ruleparser.ruleengine import RuleTree

DEFAULT_CONSUME_TIMEOUT = 30  # seconds

MAX_RETRY_COUNT = 5


class AbstractConsumer(ABC):
    def __init__(self, group_id: str, topic_name: str, last: bool, match: str = None, enable_auto_commit: bool = True):
        self._match = match
        self._last = last
        self._group_id = group_id
        self._consumer = None
        self._enable_auto_commit = enable_auto_commit
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
        self._config = Config.get_instance().create_confluent_config()
        self._config.update(
            {
                "group.id": self._group_id,
                "error_cb": log_error,
                # We need to commit offsets manually once we"re sure it got saved
                # to the sink
                "enable.auto.commit": self._enable_auto_commit,
                "enable.partition.eof": True,
                # We need this to start at the last committed offset instead of the
                # latest when subscribing for the first time
                "default.topic.config": {"auto.offset.reset": offset_reset},
            }
        )

    @abstractmethod
    def create_internal_consumer(self):
        raise NotImplementedError()

    def assign_specific_partitions(self, topic_name: str, partitions: list = None, offset: int = 0):
        self._topic_name = topic_name
        if partitions is not None:
            topic_partitions = [
                TopicPartition(self._topic_name, partition=partition, offset=offset) for partition in partitions
            ]
        else:
            topic_partitions = [TopicPartition(self._topic_name, partition=0, offset=offset)]
        self._consumer.assign(topic_partitions)

    def subscribe(self, topics: List[str]) -> None:
        self._consumer.subscribe(topics)

    def close(self) -> None:
        self._consumer.close()

    def commit(self, offsets: List[TopicPartition]):
        self._consumer.commit(offsets=offsets)

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

    def consume_single_message(self, timeout=DEFAULT_CONSUME_TIMEOUT) -> Message:
        message = self._consumer.poll(timeout=timeout)
        raise_for_message(message)
        return message

    def consume_single_acceptable_message(self, timeout=DEFAULT_CONSUME_TIMEOUT) -> Optional[Message]:
        message_acceptable = False
        total_time_remaining = timeout
        while not message_acceptable and total_time_remaining > 0:
            iteration_start = pendulum.now()
            message = self.consume_single_message(timeout=timeout)
            total_time_remaining -= (pendulum.now() - iteration_start).in_seconds()
            message_acceptable = self.consumed_message_matches(message)
        return message if message_acceptable else None

    def consumed_message_matches(self, message: Message):
        if self._rule_tree is not None:
            return self._rule_tree.evaluate(message)
        else:
            return True


class MessageConsumer(AbstractConsumer):
    def create_internal_consumer(self):
        self._consumer = confluent_kafka.Consumer(self._config)

    def consume(self, offset: int = 0, partition: int = 0) -> Message:
        self.assign_specific_partitions(self._topic_name, partitions=[partition], offset=offset)
        return self.consume_single_message()


class PingConsumer(AbstractConsumer):
    def consume(self) -> Optional[Tuple[str, int]]:
        message = self.consume_single_message()
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
        output_directory: pathlib.Path,
        last: bool,
        match: str = None,
        initialize_default_output_directory: bool = False,
        enable_auto_commit: bool = True,
    ):
        super().__init__(group_id, topic_name, last, match, enable_auto_commit)
        self.output_directory = output_directory
        self.writers[-1] = StdOutWriter() if output_directory is None else PlainTextFileWriter(self.output_directory)
        self._initialize_default_output_directory = initialize_default_output_directory
        if self._initialize_default_output_directory and self.output_directory is not None:
            self.writers[-1].init_destination_directory()

    def consume(self, amount: int) -> int:
        counter = 0

        while counter < amount:
            try:
                message = self.consume_single_acceptable_message()
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
            self.subscribe([self._topic_name])

    def output_consumed(self, message: Message):
        if (
            self.output_directory
            and not self._initialize_default_output_directory
            and message.partition() not in self.writers
        ):
            writer = PlainTextFileWriter(self.output_directory, message.partition())
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
        output_directory: pathlib.Path,
        last: bool,
        match: str = None,
        initialize_default_output_directory: bool = False,
        enable_auto_commit: bool = True,
    ):
        super().__init__(
            group_id,
            topic_name,
            output_directory,
            last,
            match,
            initialize_default_output_directory,
            enable_auto_commit,
        )
        self.schema_registry_client = SchemaRegistryClient(Config.get_instance().schema_registry)
        self.writers[-1] = (
            StdOutAvroWriter(schema_registry_client=self.schema_registry_client)
            if output_directory is None
            else AvroFileWriter(self.schema_registry_client, self.output_directory)
        )
        if self._initialize_default_output_directory and self.output_directory is not None:
            self.writers[-1].init_destination_directory()

    def output_consumed(self, message: Message):
        if (
            self.output_directory
            and not self._initialize_default_output_directory
            and message.partition() not in self.writers
        ):
            writer = AvroFileWriter(self.schema_registry_client, self.output_directory, message.partition())
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
        output_directory: pathlib.Path,
        last: bool,
        avro: bool,
        initialize_default_output_directory: bool = False,
        match: str = None,
        enable_auto_commit: bool = True,
    ) -> AbstractConsumer:
        """
        Creates a Kafka consumer
        :param group_id: ID of the consumer group
        :param topic_name: Topic name for the new consumer
        :param output_directory: The directory to store the consumed messages (if None, than STDOUT is used)
        :param last: Start consuming from the latest committed offset
        :param avro: Are messages in Avro format?
        :param initialize_default_output_directory: If set to true, all messages will be stored in a directory named partition_any, instead having a separate directory for each partition. This argument is only used if output_directory is not None.
        :param match: Match expression for message filtering
        :param enable_auto_commit: Allow the consumer to automatically commit offset
        :return: Consumer object
        """
        if avro:
            consumer = AvroFileConsumer(
                group_id=group_id,
                topic_name=topic_name,
                output_directory=output_directory,
                last=last,
                match=match,
                initialize_default_output_directory=initialize_default_output_directory,
                enable_auto_commit=enable_auto_commit,
            )
        else:
            consumer = PlaintextConsumer(
                group_id=group_id,
                topic_name=topic_name,
                output_directory=output_directory,
                last=last,
                match=match,
                initialize_default_output_directory=initialize_default_output_directory,
                enable_auto_commit=enable_auto_commit,
            )
        return consumer

    def create_ping_consumer(self, group_id: str, topic_name: str):
        consumer = PingConsumer(group_id, topic_name, last=False)
        return consumer


def consume_to_file_ordered(
    output_directory: pathlib.Path,
    topic: str,
    group_id: str,
    partitions: List[int],
    desired_message_count: int,
    avro: bool,
    match: str,
    last: bool,
    write_to_stdout: bool = False,
) -> int:

    consumers = _create_consumers(output_directory, topic, group_id, partitions, avro, match, last, write_to_stdout)
    message_heap = _initialize_heap_one_message_per_partition(consumers)
    number_of_messages_returned = _iterate_and_return_messages(message_heap, consumers, desired_message_count)

    for c in consumers:
        c.close_all_writers()

    return number_of_messages_returned


def _create_consumers(
    output_directory: pathlib.Path,
    topic: str,
    group_id: str,
    partitions: List[int],
    avro: bool,
    match: str,
    last: bool,
    write_to_stdout: bool = False,
) -> List[AbstractConsumer]:
    consumers = []
    factory = ConsumerFactory()
    for partition in partitions:
        consumer = factory.create_consumer(
            group_id=group_id,
            topic_name=None,
            output_directory=None if write_to_stdout else output_directory,
            avro=avro,
            match=match,
            last=last,
            initialize_default_output_directory=True,
            enable_auto_commit=False,
        )
        consumer.assign_specific_partitions(topic, [partition])
        consumers.append(consumer)
    return consumers


def _initialize_heap_one_message_per_partition(consumers: List[AbstractConsumer]) -> list:
    message_heap = []
    logger = logging.getLogger(__name__)
    for partition_counter in range(0, len(consumers)):
        retry_count = 0
        keep_polling_current_partition = True
        while keep_polling_current_partition:
            try:
                message_heap = _consume_message_to_heap(consumers[partition_counter], message_heap)
                keep_polling_current_partition = False
            except MessageEmptyException:
                # a possible timeout due to a network issue, retry (but not more than MAX_RETRY_COUNT attempts)
                # retry_count = _handle_empty_message_exception(retry_count)
                retry_count = retry_count + 1
                if retry_count > MAX_RETRY_COUNT:
                    keep_polling_current_partition = False
                    logger.debug(
                        f"Could not read from partition {partition_counter}. "
                        f"Retried {MAX_RETRY_COUNT} times and only got MessageEmptyException."
                    )
            except EndOfPartitionReachedException:
                logger.debug(f"No data in partition {partition_counter}.")
                keep_polling_current_partition = False

    return message_heap


def _iterate_and_return_messages(
    message_heap: list, consumers: List[AbstractConsumer], desired_message_count: int
) -> int:
    # in each iteration, take the earliest message from the map, output it and replace it with a new one (if available)
    # if not, remove the consumer and move to the next one
    count_messages_returned = 0
    logger = logging.getLogger(__name__)
    while count_messages_returned < desired_message_count and message_heap:
        (timestamp, message) = heappop(message_heap)
        consumers[0].output_consumed(message)
        count_messages_returned = count_messages_returned + 1
        partition = message.partition()
        try:
            message_heap = _consume_message_to_heap(consumers[partition], message_heap)
        except (MessageEmptyException, EndOfPartitionReachedException):
            logger.debug(f"Done reading from partition {partition}.")

    return count_messages_returned


def _consume_message_to_heap(consumer: AbstractConsumer, heap: list):
    message = consumer.consume_single_acceptable_message()
    decoded_message = decode_message(message)
    heappush(heap, (decoded_message.timestamp, message))
    return heap


def consume_to_files(
    output_directory: pathlib.Path,
    topic: str,
    group_id: str,
    desired_message_count: int,
    avro: bool,
    match: str,
    last: bool,
    write_to_stdout: bool = False,
) -> int:
    consumer = ConsumerFactory().create_consumer(
        group_id=group_id,
        topic_name=topic,
        output_directory=output_directory if not write_to_stdout else None,
        last=last,
        avro=avro,
        match=match,
        initialize_default_output_directory=False,
        enable_auto_commit=False,
    )
    number_consumed_messages = consumer.consume(int(desired_message_count))
    consumer.close_all_writers()
    return number_consumed_messages
