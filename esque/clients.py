import json
import pathlib
from contextlib import ExitStack
from glob import glob
from typing import Optional, Tuple

import click
import confluent_kafka
import pendulum
from confluent_kafka import Message
from confluent_kafka.avro import AvroProducer
from confluent_kafka.cimpl import KafkaError

from esque.avromessage import AvroFileReader, AvroFileWriter
from esque.config import Config
from esque.errors import raise_for_kafka_error, raise_for_message
from esque.helpers import delivery_callback, delta_t
from esque.message import KafkaMessage, PlainTextFileReader, PlainTextFileWriter, FileReader, FileWriter
from esque.schemaregistry import SchemaRegistryClient
from abc import ABC, abstractmethod


class AbstractConsumer(ABC):
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
        self._subscribe(topic_name)

    def _subscribe(self, topic: str) -> None:
        self._consumer.subscribe([topic])

    @abstractmethod
    def consume(self, amount: int) -> int:
        pass

    def _consume_single_message(self) -> Optional[Message]:
        poll_limit = 10
        counter = 0
        while counter < poll_limit:
            message = self._consumer.poll(timeout=10)
            if message is None:
                counter += 1
                continue
            if message.error() is not None:
                if message.error().code() == KafkaError._PARTITION_EOF:
                    print("\nEnd of partition reached!".format(**locals()))
                    break
                else:
                    raise RuntimeError(message.error().str())
            raise_for_message(message)
            return message


class PingConsumer(AbstractConsumer):
    def consume(self, amount: int) -> Optional[Tuple[str, int]]:
        msg = self._consumer.consume(timeout=10)[0]

        raise_for_message(msg)

        msg_sent_at = pendulum.from_timestamp(float(msg.value()))
        delta_sent = pendulum.now() - msg_sent_at
        return msg.key(), delta_sent.microseconds / 1000


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
                message = self._consume_single_message()
                if message is None:
                    return counter

                if message.partition() not in file_writers.keys():
                    file_writer = self.get_file_writer((self.working_dir / f"partition_{message.partition()}"))
                    stack.enter_context(file_writer)
                    file_writers[message.partition()] = file_writer

                file_writer = file_writers[message.partition()]
                file_writer.write_message_to_file(message)
                counter += 1

        return counter

    def get_file_writer(self, directory: pathlib.Path) -> FileWriter:
        return PlainTextFileWriter(directory)


class AvroFileConsumer(FileConsumer):
    def __init__(self, group_id: str, topic_name: str, working_dir: pathlib.Path, last: bool):
        super().__init__(group_id, topic_name, working_dir, last)
        self.schema_registry_client = SchemaRegistryClient(Config().schema_registry)

    def get_file_writer(self, directory: pathlib.Path) -> FileWriter:
        return AvroFileWriter(directory, self.schema_registry_client)


class Producer(ABC):
    def __init__(self):
        self._config = Config().create_confluent_config()
        self._config.update({"on_delivery": delivery_callback, "error_cb": raise_for_kafka_error})

    @abstractmethod
    def produce(self, topic_name: str) -> int:
        pass


class PingProducer(Producer):
    def __init__(self):
        super().__init__()
        self._producer = confluent_kafka.Producer(self._config)

    def produce(self, topic_name: str) -> int:
        start = pendulum.now()
        self._producer.produce(topic=topic_name, key=str(0), value=str(pendulum.now().timestamp()))
        while True:
            left_messages = self._producer.flush(1)
            if left_messages == 0:
                break
            click.echo(f"{delta_t(start)} | Still {left_messages} messages left, flushing...")
        return 1


class FileProducer(Producer):
    def __init__(self, working_dir: pathlib.Path):
        super().__init__()
        self._producer = confluent_kafka.Producer(self._config)
        self.working_dir = working_dir

    def produce(self, topic_name: str) -> int:
        path_list = glob(str(self.working_dir / "partition_*"))
        counter = 0
        with ExitStack() as stack:
            for partition_path in path_list:
                file_reader = self.get_file_reader(pathlib.Path(partition_path))
                stack.enter_context(file_reader)
                for message in file_reader.read_from_file():
                    self.produce_message(topic_name, message)
                    counter += 1

            while True:
                left_messages = self._producer.flush(1)
                if left_messages == 0:
                    break
                click.echo(f"Still {left_messages} messages left, flushing...")

            return counter

    def get_file_reader(self, directory: pathlib.Path) -> FileReader:
        return PlainTextFileReader(directory)

    def produce_message(self, topic_name: str, message: KafkaMessage):
        self._producer.produce(topic=topic_name, key=message.key, value=message.value, partition=message.partition)


class AvroFileProducer(FileProducer):
    def __init__(self, working_dir: pathlib.Path):
        super().__init__(working_dir)
        self._config.update({"schema.registry.url": Config().schema_registry})
        self._producer = AvroProducer(self._config)

    def get_file_reader(self, directory: pathlib.Path) -> FileReader:
        return AvroFileReader(directory)

    def produce_message(self, topic_name: str, message: KafkaMessage):
        self._producer.produce(
            topic=topic_name,
            key=json.loads(message.key),
            value=json.loads(message.value),
            key_schema=message.key_schema,
            value_schema=message.value_schema,
            partition=message.partition,
        )
