import dataclasses
import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

from confluent_kafka import OFFSET_BEGINNING, OFFSET_END, Consumer, KafkaError, Message, Producer, TopicPartition
from confluent_kafka.admin import TopicMetadata

from esque import config as esque_config
from esque.config import ESQUE_GROUP_ID
from esque.io.exceptions import (
    EsqueIOHandlerReadException,
    EsqueIOHandlerWriteException,
    EsqueIOSerializerConfigNotSupported,
)
from esque.io.handlers import BaseHandler, HandlerConfig
from esque.io.messages import BinaryMessage, MessageHeader
from esque.io.stream_events import EndOfStream, StreamEvent, TemporaryEndOfPartition


@dataclasses.dataclass(frozen=True)
class KafkaHandlerConfig(HandlerConfig):

    consumer_group_id: str = ESQUE_GROUP_ID
    send_timestamp: str = ""

    @property
    def topic_name(self) -> str:
        return self.path

    @property
    def esque_context(self) -> str:
        if not self.host:
            return esque_config.Config.get_instance().current_context
        return self.host


class KafkaHandler(BaseHandler[KafkaHandlerConfig]):
    config_cls = KafkaHandlerConfig
    _eof_reached: Dict[int, bool]
    OFFSET_AT_FIRST_MESSAGE = OFFSET_BEGINNING
    OFFSET_AFTER_LAST_MESSAGE = OFFSET_END

    # hopefully this number won't get assigned any semantics by the Kafka Devs any time soon
    OFFSET_AT_LAST_MESSAGE = -101

    def __init__(self, config: KafkaHandlerConfig):
        super().__init__(config)
        self._assignment_created = False
        self._seek = OFFSET_BEGINNING
        self._high_watermarks: Dict[int, int] = {}
        self._consumer: Optional[Consumer] = None
        self._producer: Optional[Producer] = None
        self._errors: List[KafkaError] = []

    def _get_producer(self) -> Producer:
        if self._producer is not None:
            return self._producer

        config_instance = esque_config.Config()
        with config_instance.temporary_context(self.config.esque_context):
            self._producer = Producer(config_instance.create_confluent_config(include_schema_registry=False))
        return self._producer

    def _get_consumer(self) -> Consumer:
        if self._consumer is not None:
            return self._consumer

        config_instance = esque_config.Config()
        with config_instance.temporary_context(self.config.esque_context):
            group_id = self.config.consumer_group_id
            self._consumer = Consumer(
                {
                    "group.id": group_id,
                    "enable.partition.eof": True,
                    "enable.auto.commit": False,
                    **config_instance.create_confluent_config(include_schema_registry=False),
                }
            )

        topic_metadata: TopicMetadata = self._consumer.list_topics(self.config.topic_name).topics[
            self.config.topic_name
        ]
        if topic_metadata.error is not None:
            raise EsqueIOHandlerReadException(f"Topic {self.config.topic_name!r} not found.")

        self._eof_reached = {partition_id: False for partition_id in topic_metadata.partitions.keys()}
        for partition_id in topic_metadata.partitions.keys():
            self._high_watermarks[partition_id] = self._consumer.get_watermark_offsets(
                TopicPartition(topic=self.config.topic_name, partition=partition_id)
            )[1]

        return self._consumer

    def get_serializer_configs(self) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        raise EsqueIOSerializerConfigNotSupported

    def put_serializer_configs(self, config: Tuple[Dict[str, Any], Dict[str, Any]]) -> None:
        raise EsqueIOSerializerConfigNotSupported

    def write_message(self, binary_message: Union[BinaryMessage, StreamEvent]) -> None:
        self._produce_single_message(binary_message=binary_message)
        self._flush()

    def write_many_messages(self, message_stream: Iterable[Union[BinaryMessage, StreamEvent]]) -> None:
        for binary_message in message_stream:
            self._produce_single_message(binary_message=binary_message)
        self._flush()

    def _produce_single_message(self, binary_message: BinaryMessage) -> None:
        if isinstance(binary_message, StreamEvent):
            return
        partition_arg = {}
        partition = self._io_to_confluent_partition(binary_message.partition)
        if partition is not None:
            partition_arg["partition"] = partition
        self._get_producer().produce(
            topic=self.config.topic_name,
            value=binary_message.value,
            key=binary_message.key,
            headers=self._io_to_confluent_headers(binary_message.headers),
            timestamp=self._io_to_confluent_timestamp(binary_message.timestamp),
            on_delivery=self._delivery_callback,
            **partition_arg,
        )

    def _delivery_callback(self, err: Optional[KafkaError], msg: str):
        if err is None:
            return
        self._errors.append(err)

    def _flush(self):
        self._get_producer().flush()
        if self._errors:
            exception = EsqueIOHandlerWriteException(
                "The following exception(s) occurred while writing to Kafka:\n  " + "\n  ".join(map(str, self._errors))
            )
            self._errors.clear()
            raise exception

    @staticmethod
    def _io_to_confluent_partition(partition: int) -> Optional[int]:
        # TODO: introduce something like the config.send_timestamp flag to make it possible to always return None here.
        #  This would allow for moving messages between topics with different amounts of partitions without making them
        #  unbalanced.
        if partition < 0:
            return None
        return partition

    def _io_to_confluent_timestamp(self, message_ts: datetime.datetime):
        return int(message_ts.timestamp() * 1000) if self.config.send_timestamp else 0

    @staticmethod
    def _io_to_confluent_headers(headers: List[MessageHeader]) -> Optional[List[Tuple[str, Optional[bytes]]]]:
        if not headers:
            return None
        confluent_headers: List[Tuple[str, Optional[bytes]]] = []
        for header in headers:
            key = header.key
            if header.value is not None:
                value = header.value.encode("utf-8")
            else:
                value = None
            confluent_headers.append((key, value))
        return confluent_headers

    def read_message(self) -> Union[BinaryMessage, StreamEvent]:
        if not self._assignment_created:
            self._assign()

        consumed_message: Optional[Message] = None
        while consumed_message is None:
            consumed_message = self._get_consumer().poll(timeout=0.1)
            if consumed_message is None and all(self._eof_reached.values()):
                return TemporaryEndOfPartition("Reached end of all partitions", partition=EndOfStream.ALL_PARTITIONS)
        # TODO: process other error cases (connection issues etc.)
        if consumed_message.error() is not None and consumed_message.error().code() == KafkaError._PARTITION_EOF:
            self._eof_reached[consumed_message.partition()] = True
            return TemporaryEndOfPartition("Reached end of partition", partition=consumed_message.partition())
        else:
            self._eof_reached[consumed_message.partition()] = False

            binary_message = self._confluent_to_binary_message(consumed_message)

            return binary_message

    def _confluent_to_binary_message(self, consumed_message: Message) -> BinaryMessage:
        binary_message = BinaryMessage(
            key=consumed_message.key(),
            value=consumed_message.value(),
            partition=consumed_message.partition(),
            offset=consumed_message.offset(),
            timestamp=self._confluent_to_io_timestamp(consumed_message),
            headers=self._confluent_to_io_headers(consumed_message.headers()),
        )
        return binary_message

    @staticmethod
    def _confluent_to_io_timestamp(consumed_message: Message) -> datetime.datetime:
        return datetime.datetime.fromtimestamp(consumed_message.timestamp()[1] / 1000, tz=datetime.timezone.utc)

    @staticmethod
    def _confluent_to_io_headers(
        confluent_headers: Optional[List[Tuple[str, Optional[bytes]]]]
    ) -> List[MessageHeader]:
        io_headers: List[MessageHeader] = []

        if confluent_headers is None:
            return io_headers

        for confluent_header in confluent_headers:
            key, value = confluent_header
            if value is not None:
                value = value.decode("utf-8")
            io_headers.append(MessageHeader(key, value))

        return io_headers

    def message_stream(self) -> Iterable[Union[BinaryMessage, StreamEvent]]:
        while True:
            yield self.read_message()

    def seek(self, position: int) -> None:
        self._seek = position

    def _assign(self) -> None:
        self._assignment_created = True
        if self._seek == self.OFFSET_AT_LAST_MESSAGE:
            self._get_consumer().assign(
                [
                    TopicPartition(topic=self.config.topic_name, partition=partition_id, offset=high_watermark - 1)
                    for partition_id, high_watermark in self._high_watermarks.items()
                ]
            )
        else:
            self._get_consumer().assign(
                [
                    TopicPartition(topic=self.config.topic_name, partition=partition_id, offset=self._seek)
                    for partition_id in self._eof_reached.keys()
                ]
            )

    def close(self) -> None:
        if self._consumer is not None:
            self._consumer.close()
            self._consumer = None
        if self._producer is not None:
            self._producer.flush()
            self._producer = None
