import dataclasses
import json
import random
import string
from typing import Any, ClassVar, Dict, List, Optional, Tuple, Type, TypeVar
from unittest import mock

import avro
from click.testing import CliRunner
from confluent_kafka import cimpl
from confluent_kafka.avro import AvroProducer
from confluent_kafka.avro import loads as load_schema
from confluent_kafka.cimpl import Producer as ConfluentProducer
from pytest_cases import fixture

from esque.config import Config


@fixture()
def interactive_cli_runner(mocker: mock, unittest_config: Config) -> CliRunner:
    mocker.patch("esque.cli.helpers._isatty", return_value=True)
    mocker.patch("esque.cli.environment.ESQUE_VERBOSE", new_callable=mock.PropertyMock, return_value="1")
    return CliRunner()


@fixture()
def non_interactive_cli_runner(mocker: mock, unittest_config: Config) -> CliRunner:
    mocker.patch("esque.cli.helpers._isatty", return_value=False)
    mocker.patch("esque.cli.environment.ESQUE_VERBOSE", new_callable=mock.PropertyMock, return_value="1")
    return CliRunner()


T = TypeVar("T")


@dataclasses.dataclass
class KafkaTestMessage:
    key: Any = ""
    value: Any = ""
    binary_key: bytes = b""
    binary_value: bytes = b""
    partition: int = -1
    offset: int = -1
    timestamp: int = 0
    topic: str = ""
    headers: List[Tuple[str, bytes]] = dataclasses.field(default_factory=list)

    def update_callback(self, err: Optional[cimpl.KafkaError], msg: cimpl.Message):
        assert err is None, f"Received KafkaError {err}."
        self.binary_value = msg.value()
        self.binary_key = msg.key()
        self.partition = msg.partition()
        self.offset = msg.offset()
        self.timestamp = msg.timestamp()[1]

    def producer_args(self) -> Dict:
        args = {"key": self.key, "value": self.value, "topic": self.topic, "on_delivery": self.update_callback}
        if self.partition >= 0:
            args["partition"] = self.partition
        if self.timestamp > 0:
            args["timestamp"] = self.timestamp
        if self.headers:
            args["headers"] = self.headers
        return args

    @classmethod
    def random_values(
        cls: Type[T], topic_name: str, num_partitions: int, n: int = 10, generate_headers: bool = False
    ) -> List[T]:
        msgs: List[T] = []
        for _ in range(n):
            key = cls.random_key()
            value = cls.random_value()
            partition = random.randrange(0, num_partitions)
            if generate_headers:
                headers = cls.random_headers()
            else:
                headers = []
            msgs.append(cls(key=key, value=value, topic=topic_name, partition=partition, headers=headers))
        return msgs

    @staticmethod
    def random_key() -> Any:
        return random_str()

    @staticmethod
    def random_value() -> Any:
        return random_str()

    @staticmethod
    def random_headers() -> List[Tuple[str, Optional[bytes]]]:
        header_count = random.randrange(0, 5)
        headers: List[Tuple[str, Optional[bytes]]] = []
        for _ in range(header_count):
            header_key = random_str()
            if random.random() < 0.1:  # 10% chance to be None
                header_value = None
            else:
                header_value = random_str().encode("utf-8")
            headers.append((header_key, header_value))
        return headers


def random_str(length: int = 16) -> str:
    return "".join(random.choices(string.ascii_lowercase, k=length))


def produce_text_test_messages(
    producer: ConfluentProducer, topic: Tuple[str, int], amount: int = 10
) -> List["KafkaTestMessage"]:
    topic_name, num_partitions = topic
    messages = KafkaTestMessage.random_values(
        topic_name=topic_name, num_partitions=num_partitions, n=amount, generate_headers=False
    )
    for msg in messages:
        producer.produce(**msg.producer_args())
    producer.flush()
    return messages


def produce_text_test_messages_with_headers(
    producer: ConfluentProducer, topic: Tuple[str, int], amount: int = 10
) -> List["KafkaTestMessage"]:
    topic_name, num_partitions = topic
    messages = KafkaTestMessage.random_values(
        topic_name=topic_name, num_partitions=num_partitions, n=amount, generate_headers=True
    )
    for msg in messages:
        producer.produce(**msg.producer_args())
    producer.flush()
    return messages


def mk_avro_schema(field_name: str, field_type: str) -> avro.schema.Schema:
    return load_schema(
        json.dumps(
            {
                "type": "record",
                "namespace": "com.example",
                "name": f"MySchema_{field_name}",
                "fields": [{"name": field_name, "type": field_type}],
            }
        )
    )


@dataclasses.dataclass
class AvroKafkaTestMessage(KafkaTestMessage):
    KEY_SCHEMA: ClassVar[avro.schema.Schema] = mk_avro_schema("key", "string")
    VALUE_SCHEMA: ClassVar[avro.schema.Schema] = mk_avro_schema("value", "string")

    def producer_args(self) -> Dict:
        args = super().producer_args()
        args["key_schema"] = self.KEY_SCHEMA
        args["value_schema"] = self.VALUE_SCHEMA
        return args

    @staticmethod
    def random_key() -> Any:
        return {"key": random_str()}

    @staticmethod
    def random_value() -> Any:
        return {"value": random_str()}


def produce_avro_test_messages(
    avro_producer: AvroProducer, topic: Tuple[str, int], amount: int = 10
) -> List[AvroKafkaTestMessage]:

    topic_name, num_partitions = topic
    messages: List[AvroKafkaTestMessage] = AvroKafkaTestMessage.random_values(topic_name, num_partitions, n=amount)

    for msg in messages:
        avro_producer.produce(**msg.producer_args())

    avro_producer.flush()
    return messages


@dataclasses.dataclass
class BinaryKafkaTestMessage(KafkaTestMessage):
    @staticmethod
    def random_key() -> Any:
        return random_bytes()

    @staticmethod
    def random_value() -> Any:
        return random_bytes()


def random_bytes(length: int = 16) -> bytes:
    return random.getrandbits(length * 8).to_bytes(length, "big")


def produce_binary_test_messages(
    producer: ConfluentProducer, topic: Tuple[str, int], amount: int = 10
) -> List[BinaryKafkaTestMessage]:
    topic_name, num_partitions = topic
    messages: List[BinaryKafkaTestMessage] = BinaryKafkaTestMessage.random_values(
        topic_name=topic_name, num_partitions=num_partitions, n=amount, generate_headers=False
    )
    for msg in messages:
        producer.produce(**msg.producer_args())
    producer.flush()
    return messages
