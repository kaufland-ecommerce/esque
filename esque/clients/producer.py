import json
import pathlib
from abc import ABC, abstractmethod
from glob import glob

import click
import confluent_kafka
import pendulum
from confluent_kafka.avro import AvroProducer

from esque.config import Config
from esque.errors import raise_for_kafka_error
from esque.helpers import delivery_callback, delta_t
from esque.messages.avromessage import AvroFileReader
from esque.messages.message import FileReader, KafkaMessage, PlainTextFileReader


class Producer(ABC):
    def __init__(self):
        self.queue_length = 100000
        self.internal_queue_length_limit = self.queue_length / 0.5
        self._config = Config().create_confluent_config()
        self._config.update(
            {
                "on_delivery": delivery_callback,
                "error_cb": raise_for_kafka_error,
                "queue.buffering.max.messages": self.queue_length,
            }
        )

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
        for partition_path in path_list:
            with self.get_file_reader(pathlib.Path(partition_path)) as file_reader:
                for message in file_reader.read_from_file():
                    self.produce_message(topic_name, message)
                    left_messages = self._producer.flush(0)
                    if left_messages > self.internal_queue_length_limit:
                        self.flush_all()
                    counter += 1
                self.flush_all()

        return counter

    def flush_all(self):
        while True:
            left_messages = self._producer.flush(1)
            if left_messages == 0:
                break
            click.echo(f"Still {left_messages} messages left, flushing...")

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
