import copy
import dataclasses
import itertools
import json
import random
import string
from typing import Any, ClassVar, Dict, Iterator, List, Optional, Tuple, Type, TypeVar

import avro
from confluent_kafka import Producer as ConfluentProducer
from confluent_kafka import cimpl
from confluent_kafka.avro import AvroProducer
from confluent_kafka.avro import loads as load_schema

from esque.io.messages import Data
from esque.io.serializers import ProtoSerializer

T = TypeVar("T")


@dataclasses.dataclass
class KafkaTestMessage:
    key: Any = ""
    value: Any = ""
    binary_key: bytes = b""
    binary_value: bytes = b""
    partition: int = -1
    offset: int = -1
    timestamp: int = -1
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
        if self.timestamp >= 0:
            args["timestamp"] = self.timestamp
        if self.headers:
            args["headers"] = self.headers
        return args

    @classmethod
    def random_values(cls: Type[T], topic_name: str, n: int = 10, generate_headers: bool = False) -> List[T]:
        msgs: List[T] = []

        timestamp_generator: Iterator[int] = itertools.count(start=10_000, step=10_000)

        for _ in range(n):
            key = cls.random_key()
            value = cls.random_value()
            if generate_headers:
                headers = cls.random_headers()
            else:
                headers = []
            msgs.append(
                cls(key=key, value=value, topic=topic_name, headers=headers, timestamp=next(timestamp_generator))
            )
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


@dataclasses.dataclass
class BinaryKafkaTestMessage(KafkaTestMessage):
    @staticmethod
    def random_key() -> Any:
        return random_bytes()

    @staticmethod
    def random_value() -> Any:
        return random_bytes()


@dataclasses.dataclass
class ProtobufKafkaTestMessage(KafkaTestMessage):

    @staticmethod
    def random_key() -> Any:
        return random_bytes()

    @staticmethod
    def random_value() -> Any:
        return {
            "type_string": random_str(random.randint(1, 10)),
            "optional_string": random.choice([None, random_str(random.randint(1, 10))]),
            "type_int32": random.randint(1, 100),
            "type_int64": random.randint(1, 100),
            "optional_int64": random.choice([None, random.randint(1, 100)]),
            "type_float": round(random.uniform(-5, 5), 2),
        }


def random_bytes(length: int = 16) -> bytes:
    return random.getrandbits(length * 8).to_bytes(length, "big")


def produce_text_test_messages(
    producer: ConfluentProducer, topic_name: str, amount: int = 10
) -> List["KafkaTestMessage"]:
    messages = KafkaTestMessage.random_values(topic_name=topic_name, n=amount, generate_headers=False)
    produce_all(producer, messages)
    return messages


def produce_all(producer: ConfluentProducer, messages: List[KafkaTestMessage]) -> None:
    for msg in messages:
        producer.produce(**msg.producer_args())
    producer.flush()


def produce_text_test_messages_with_headers(
    producer: ConfluentProducer, topic_name: str, amount: int = 10
) -> List["KafkaTestMessage"]:
    messages = KafkaTestMessage.random_values(topic_name=topic_name, n=amount, generate_headers=True)
    produce_all(producer, messages)
    return messages


def produce_avro_test_messages(
    avro_producer: AvroProducer, topic_name: str, amount: int = 10
) -> List[AvroKafkaTestMessage]:
    messages: List[AvroKafkaTestMessage] = AvroKafkaTestMessage.random_values(topic_name, n=amount)
    produce_all(avro_producer, messages)
    return messages


def produce_binary_test_messages(
    producer: ConfluentProducer, topic_name: str, amount: int = 10
) -> List[BinaryKafkaTestMessage]:
    messages: List[BinaryKafkaTestMessage] = BinaryKafkaTestMessage.random_values(
        topic_name=topic_name, n=amount, generate_headers=False
    )
    produce_all(producer, messages)
    return messages


def produce_proto_test_messages(
    proto_serializer: ProtoSerializer, producer: ConfluentProducer, topic_name: str, amount: int = 10
) -> List[ProtobufKafkaTestMessage]:
    messages: List[ProtobufKafkaTestMessage] = ProtobufKafkaTestMessage.random_values(
        topic_name=topic_name, n=amount, generate_headers=False
    )
    copy_of_messages = copy.deepcopy(messages)
    proto_serialised_messages = []
    for msg in messages:
        msg.value = proto_serializer.serialize(Data(msg.value, ProtoSerializer.dict_data_type))
        proto_serialised_messages.append(msg)
    produce_all(producer, proto_serialised_messages)
    return copy_of_messages
