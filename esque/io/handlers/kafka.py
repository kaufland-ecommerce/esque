import dataclasses
import datetime
import functools
import warnings
from typing import Any, Dict, Iterable, Optional, Tuple, Union

from confluent_kafka import OFFSET_BEGINNING, OFFSET_END, Consumer, KafkaError, Message, Producer, TopicPartition
from confluent_kafka.admin import TopicMetadata

from esque import config as esque_config
from esque.config import ESQUE_GROUP_ID
from esque.io.exceptions import EsqueIOHandlerReadException, EsqueIOSerializerConfigNotSupported
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

    @property
    def partition_count(self) -> int:
        return len(self._eof_reached)

    @functools.cached_property
    def _producer(self) -> Producer:
        config_instance = esque_config.Config()
        with config_instance.temporary_context(self.config.esque_context):
            return Producer(config_instance.create_confluent_config(include_schema_registry=False))

    @functools.cached_property
    def _consumer(self) -> Consumer:
        config_instance = esque_config.Config()
        with config_instance.temporary_context(self.config.esque_context):
            group_id = self.config.consumer_group_id
            consumer = Consumer(
                {
                    "group.id": group_id,
                    "enable.partition.eof": True,
                    "enable.auto.commit": False,
                    **config_instance.create_confluent_config(include_schema_registry=False),
                }
            )

        topic_metadata: TopicMetadata = consumer.list_topics(self.config.topic_name).topics[self.config.topic_name]
        if topic_metadata.error is not None:
            raise EsqueIOHandlerReadException(f"Topic {self.config.topic_name!r} not found.")

        self._eof_reached = {partition_id: False for partition_id in topic_metadata.partitions.keys()}
        for partition_id in topic_metadata.partitions.keys():
            self._high_watermarks[partition_id] = consumer.get_watermark_offsets(
                TopicPartition(topic=self.config.topic_name, partition=partition_id)
            )[1]
        return consumer

    def get_serializer_configs(self) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        raise EsqueIOSerializerConfigNotSupported

    def put_serializer_configs(self, config: Tuple[Dict[str, Any], Dict[str, Any]]) -> None:
        raise EsqueIOSerializerConfigNotSupported

    def write_message(self, binary_message: Union[BinaryMessage, StreamEvent]) -> None:
        if isinstance(binary_message, StreamEvent):
            return
        self._produce_single_message(binary_message=binary_message)
        self._producer.flush()

    def write_many_messages(self, message_stream: Iterable[Union[BinaryMessage, StreamEvent]]) -> None:
        for binary_message in message_stream:
            self._produce_single_message(binary_message=binary_message)
        self._producer.flush()

    def _produce_single_message(self, binary_message: BinaryMessage) -> None:
        self._producer.produce(
            topic=self.config.topic_name,
            value=binary_message.value,
            key=binary_message.key,
            partition=binary_message.partition,
            headers=[(h.key, h.value) for h in binary_message.headers],
            timestamp=int(binary_message.timestamp.timestamp() * 1000) if self.config.send_timestamp else None,
        )

    def read_message(self) -> Union[BinaryMessage, StreamEvent]:
        if not self._assignment_created:
            self.assign()

        consumed_message: Optional[Message] = None
        while consumed_message is None:
            consumed_message = self._consumer.poll(timeout=0.1)
            if consumed_message is None and all(self._eof_reached.values()):
                return TemporaryEndOfPartition(
                    "Reached end of all partitions", partition_id=EndOfStream.ALL_PARTITIONS
                )
        # TODO: process other error cases (connection issues etc.)
        if consumed_message.error() is not None and consumed_message.error().code() == KafkaError._PARTITION_EOF:
            self._eof_reached[consumed_message.partition()] = True
            return TemporaryEndOfPartition("Reached end of partition", partition_id=consumed_message.partition())
        else:
            self._eof_reached[consumed_message.partition()] = False

            if consumed_message.headers() is None:
                headers = []
            else:
                headers = [MessageHeader(k, v) for k, v in consumed_message.headers()]

            return BinaryMessage(
                key=consumed_message.key(),
                value=consumed_message.value(),
                partition=consumed_message.partition(),
                offset=consumed_message.offset(),
                timestamp=datetime.datetime.fromtimestamp(
                    consumed_message.timestamp()[1] / 1000, tz=datetime.timezone.utc
                ),
                headers=headers,
            )

    def message_stream(self) -> Iterable[Union[BinaryMessage, StreamEvent]]:
        while True:
            yield self.read_message()

    def seek(self, position: int) -> None:
        self._seek = position

    def assign(self) -> None:
        if self._assignment_created:
            warnings.warn("Already assigned, some messages from the previous assignment might still be received.")

        self._assignment_created = True
        if self._seek == self.OFFSET_AT_LAST_MESSAGE:
            self._consumer.assign(
                [
                    TopicPartition(topic=self.config.topic_name, partition=partition_id, offset=high_watermark - 1)
                    for partition_id, high_watermark in self._high_watermarks.items()
                ]
            )
        else:
            self._consumer.assign(
                [
                    TopicPartition(topic=self.config.topic_name, partition=partition_id, offset=self._seek)
                    for partition_id in self._eof_reached.keys()
                ]
            )
