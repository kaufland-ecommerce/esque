import pathlib
from typing import Optional, Tuple

import click
import confluent_kafka
import pendulum
from confluent_kafka import TopicPartition, Message
from confluent_kafka.avro import AvroProducer

from esque.avromessage import AvroFileReader, AvroFileWriter
from esque.config import Config
from esque.errors import raise_for_kafka_error, raise_for_message, KafkaException
from esque.helpers import delivery_callback, delta_t
from esque.message import (
    KafkaMessage,
    PlainTextFileReader,
    PlainTextFileWriter,
    FileReader,
    FileWriter,
)
from esque.schemaregistry import SchemaRegistryClient

DEFAULT_RETENTION_MS = 7 * 24 * 60 * 60 * 1000


class Consumer:
    def __init__(self, group_id: str, topic_name: str, last: bool):
        offset_reset = "earliest"
        if last:
            offset_reset = "latest"

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
        self._assign_exact_partitions(topic_name)

    def _assign_exact_partitions(self, topic: str) -> None:
        self._consumer.assign([TopicPartition(topic=topic, partition=0, offset=0)])


class PingConsumer(Consumer):
    def consume_ping(self) -> Optional[Tuple[str, int]]:
        msg = self._consumer.consume(timeout=10)[0]

        raise_for_message(msg)

        msg_sent_at = pendulum.from_timestamp(float(msg.value()))
        delta_sent = pendulum.now() - msg_sent_at
        return msg.key(), delta_sent.microseconds / 1000


class FileConsumer(Consumer):
    def __init__(
        self,
        group_id: str,
        topic_name: str,
        working_dir: pathlib.Path,
        file_writer: FileWriter,
        last: bool,
    ):
        super().__init__(group_id, topic_name, last)
        self.working_dir = working_dir
        offset_reset = "earliest"
        if last:
            offset_reset = "latest"

        self._config.update(
            {"default.topic.config": {"auto.offset.reset": offset_reset}}
        )
        self._consumer = confluent_kafka.Consumer(self._config)
        self._assign_exact_partitions(topic_name)
        self.file_writer = file_writer

    @classmethod
    def create(
        cls, group_id: str, topic_name: str, working_dir: pathlib.Path, last: bool
    ):
        return cls(group_id, topic_name, working_dir, PlainTextFileWriter(), last)

    def consume_to_file(self, amount: int) -> int:
        counter = 0
        with (self.working_dir / "data").open("wb") as file:
            while counter < amount:
                try:
                    message = self._consume_single_message()
                except KafkaException as ex:
                    print("An error occurred: " + ex.message)
                    continue

                self.file_writer.write_message_to_file(message, file)
                counter += 1

        return counter

    def _consume_single_message(self) -> Message:
        poll_limit = 10
        counter = 0
        try:
            while counter < poll_limit:
                message = self._consumer.poll(timeout=10)
                if message is None:
                    counter += 1
                    continue
                raise_for_message(message)
                return message
        except KafkaException as ex:
            print("An error occurred: " + ex.message)


class AvroFileConsumer(FileConsumer):
    def __init__(
        self,
        group_id: str,
        topic_name: str,
        working_dir: pathlib.Path,
        file_writer: FileWriter,
        last: bool,
    ):
        super().__init__(group_id, topic_name, working_dir, file_writer, last)
        self.writer = file_writer

    @classmethod
    def create(
        cls, group_id: str, topic_name: str, working_dir: pathlib.Path, last: bool
    ):
        schema_registry_client = SchemaRegistryClient(Config().schema_registry)
        file_writer = AvroFileWriter(working_dir, schema_registry_client)
        return cls(group_id, topic_name, working_dir, file_writer, last)


class PingProducer(object):
    def __init__(self):
        self._config = Config().create_confluent_config()
        self._config.update(
            {"on_delivery": delivery_callback, "error_cb": raise_for_kafka_error}
        )

        self._producer = confluent_kafka.Producer(self._config)

    def produce_ping(self, topic_name: str):
        start = pendulum.now()
        self._producer.produce(
            topic=topic_name, key=str(0), value=str(pendulum.now().timestamp())
        )
        while True:
            left_messages = self._producer.flush(1)
            if left_messages == 0:
                break
            click.echo(
                f"{delta_t(start)} | Still {left_messages} messages left, flushing..."
            )


class FileProducer(object):
    def __init__(self, working_dir: pathlib.Path, file_reader: FileReader):
        self._config = Config().create_confluent_config()
        self._config.update(
            {"on_delivery": delivery_callback, "error_cb": raise_for_kafka_error}
        )

        self._producer = confluent_kafka.Producer(self._config)
        self.working_dir = working_dir
        self.file_reader = file_reader

    @classmethod
    def create(cls, working_dir: pathlib.Path):
        return cls(working_dir, PlainTextFileReader())

    def produce_from_file(self, topic_name: str) -> int:
        with (self.working_dir / "data").open("rb") as file:
            counter = 0
            while True:
                message = self.file_reader.read_from_file(file)
                if message is None:
                    break

                self.produce(topic_name, message)
                counter += 1

            while True:
                left_messages = self._producer.flush(1)
                if left_messages == 0:
                    break

            return counter

    def produce(self, topic_name: str, message: KafkaMessage):
        self._producer.produce(topic=topic_name, key=message.key, value=message.value)


class AvroFileProducer(FileProducer):
    def __init__(self, working_dir: pathlib.Path, file_reader: FileReader):
        super().__init__(working_dir, file_reader)
        self._config.update({"schema.registry.url": Config().schema_registry})
        self._producer = AvroProducer(self._config)
        self.file_reader = file_reader

    @classmethod
    def create(cls, working_dir: pathlib.Path):
        return cls(working_dir, AvroFileReader(working_dir))

    def produce(self, topic_name: str, message: KafkaMessage):
        self._producer.produce(
            topic=topic_name,
            key=message.key,
            value=message.value,
            key_schema=message.key_schema,
            value_schema=message.value_schema,
        )
