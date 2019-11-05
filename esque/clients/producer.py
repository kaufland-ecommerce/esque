import json
import logging
import pathlib
import sys
from abc import ABC, abstractmethod
from glob import glob
from json import JSONDecodeError

import confluent_kafka
import pendulum
from confluent_kafka.avro import AvroProducer

from esque.config import Config
from esque.errors import raise_for_kafka_error, translate_third_party_exceptions
from esque.helpers import delivery_callback, delta_t
from esque.messages.avromessage import AvroFileReader
from esque.messages.message import deserialize_message, FileReader, KafkaMessage, PlainTextFileReader
from esque.ruleparser.ruleengine import RuleTree


class AbstractProducer(ABC):
    def __init__(self, topic_name: str, match: str = None):
        self.queue_length = 100000
        self.internal_queue_length_limit = self.queue_length / 0.5
        self._config = Config().create_confluent_config()
        self._setup_config()
        self.logger = logging.getLogger(__name__)
        self._topic_name = topic_name
        self._match = match
        self._producer = None
        if self._match is not None:
            self._rule_tree = RuleTree(match)
        else:
            self._rule_tree = None
        self.create_internal_producer()

    @translate_third_party_exceptions
    @abstractmethod
    def produce(self) -> int:
        raise NotImplementedError()

    @translate_third_party_exceptions
    def _setup_config(self):
        self._config.update(
            {
                "on_delivery": delivery_callback,
                "error_cb": raise_for_kafka_error,
                "queue.buffering.max.messages": str(self.queue_length),
            }
        )

    @translate_third_party_exceptions
    def flush_all(self, message_prefix: str = None):
        while True:
            left_messages = self._producer.flush(1)
            if left_messages == 0:
                break
            self.logger.info((message_prefix or "") + f"Still {left_messages} messages left, flushing...")

    @translate_third_party_exceptions
    def create_internal_producer(self):
        self._producer = confluent_kafka.Producer(self._config)

    @translate_third_party_exceptions
    def produce_message(self, topic_name: str, message: KafkaMessage):
        if self._rule_tree is None or self._rule_tree.evaluate(message):
            self._producer.produce(
                topic=topic_name,
                key=message.key,
                value=message.value,
                partition=message.partition,
                headers=message.headers,
            )


class PingProducer(AbstractProducer):
    @translate_third_party_exceptions
    def produce(self) -> int:
        start = pendulum.now()
        self.produce_message(
            topic_name=self._topic_name,
            message=KafkaMessage(key=str(0), value=str(pendulum.now().timestamp()), partition=0),
        )
        self.flush_all(message_prefix=f"{delta_t(start)} | ")
        return 1


class StdInProducer(AbstractProducer):
    def __init__(self, topic_name: str, match: str = None, ignore_errors: bool = False):
        super().__init__(topic_name=topic_name, match=match)
        self._ignore_errors = ignore_errors

    @translate_third_party_exceptions
    def produce(self) -> int:
        total_number_of_produced_messages = 0
        # accept lines from stdin until EOF
        for single_message_line in sys.stdin:
            message = None
            try:
                message = deserialize_message(single_message_line.replace("\n", ""))
            except (JSONDecodeError, KeyError):
                if self._ignore_errors:
                    message = KafkaMessage(key=None, value=single_message_line, partition=0)
            if message:
                self.produce_message(topic_name=self._topic_name, message=message)
                total_number_of_produced_messages += 1
        return total_number_of_produced_messages


class FileProducer(AbstractProducer):
    def __init__(self, topic_name: str, working_dir: pathlib.Path, match: str = None):
        super().__init__(topic_name=topic_name, match=match)
        self.working_dir = working_dir

    @translate_third_party_exceptions
    def produce(self) -> int:
        path_list = glob(str(self.working_dir / "partition_*"))
        counter = 0
        for partition_path in path_list:
            with self.get_file_reader(pathlib.Path(partition_path)) as file_reader:
                for message in file_reader.read_message_from_file():
                    self.produce_message(self._topic_name, message)
                    left_messages = self._producer.flush(0)
                    if left_messages > self.internal_queue_length_limit:
                        self.flush_all()
                    counter += 1
                self.flush_all()
        return counter

    def get_file_reader(self, directory: pathlib.Path) -> FileReader:
        return PlainTextFileReader(directory)


class AvroFileProducer(FileProducer):
    @translate_third_party_exceptions
    def create_internal_producer(self):
        self._producer = AvroProducer(self._config)

    def get_file_reader(self, directory: pathlib.Path) -> FileReader:
        return AvroFileReader(directory)

    def _setup_config(self):
        super()._setup_config()
        self._config.update({"schema.registry.url": Config().schema_registry})

    @translate_third_party_exceptions
    def produce_message(self, topic_name: str, message: KafkaMessage):
        if self._rule_tree is None or self._rule_tree.evaluate(message):
            self._producer.produce(
                topic=topic_name,
                key=json.loads(message.key),
                value=json.loads(message.value),
                partition=message.partition,
                key_schema=message.key_schema,
                value_schema=message.value_schema,
            )


class ProducerFactory:
    def create_producer(
        self,
        topic_name: str,
        working_dir: pathlib.Path,
        avro: bool,
        match: str = None,
        ignore_stdin_errors: bool = False,
    ):
        if working_dir is None:
            producer = StdInProducer(topic_name=topic_name, match=match, ignore_errors=ignore_stdin_errors)
        elif avro:
            producer = AvroFileProducer(topic_name=topic_name, working_dir=working_dir, match=match)
        else:
            producer = FileProducer(topic_name=topic_name, working_dir=working_dir, match=match)
        return producer
