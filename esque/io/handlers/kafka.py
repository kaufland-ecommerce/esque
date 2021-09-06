import dataclasses
import functools
from typing import Any, Dict, Iterable, Optional, Tuple, Union

from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaError, Message, Producer, TopicPartition
from confluent_kafka.admin import TopicMetadata

from esque import config as esque_config
from esque.config import ESQUE_GROUP_ID
from esque.io.exceptions import EsqueIOSerializerConfigNotSupported
from esque.io.handlers import BaseHandler, HandlerConfig
from esque.io.messages import BinaryMessage
from esque.io.stream_events import EndOfStream, StreamEvent, TemporaryEndOfPartition


@dataclasses.dataclass(frozen=True)
class KafkaHandlerConfig(HandlerConfig):

    consumer_group_id: Optional[str] = None

    @property
    def topic_name(self) -> str:
        return self.path

    @property
    def esque_context(self) -> str:
        return self.host


class KafkaHandler(BaseHandler[KafkaHandlerConfig]):
    config_cls = KafkaHandlerConfig
    _eof_reached: Dict[int, bool]

    @functools.cached_property
    def _producer(self) -> Producer:
        config_instance = esque_config.Config.get_instance()
        with config_instance.temporary_context(self.config.esque_context):
            return Producer(config_instance.create_confluent_config(include_schema_registry=False))

    @functools.cached_property
    def _consumer(self) -> Consumer:
        config_instance = esque_config.Config.get_instance()
        with config_instance.temporary_context(self.config.esque_context):
            group_id = self.config.consumer_group_id if self.config.consumer_group_id else ESQUE_GROUP_ID
            consumer = Consumer(
                {
                    "group.id": group_id,
                    "enable.partition.eof": True,
                    **config_instance.create_confluent_config(include_schema_registry=False),
                }
            )

        topic_metadata: TopicMetadata = consumer.list_topics(self.config.topic_name).topics[self.config.topic_name]
        self._eof_reached = {partition_id: False for partition_id in topic_metadata.partitions.keys()}
        consumer.assign(
            [
                TopicPartition(topic=self.config.topic_name, partition=partition_id, offset=OFFSET_BEGINNING)
                for partition_id in topic_metadata.partitions.keys()
            ]
        )
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
        )

    def read_message(self) -> Union[BinaryMessage, StreamEvent]:
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
            return BinaryMessage(
                key=consumed_message.key(),
                value=consumed_message.value(),
                partition=consumed_message.partition(),
                offset=consumed_message.offset(),
            )

    def message_stream(self) -> Iterable[Union[BinaryMessage, StreamEvent]]:
        while True:
            yield self.read_message()
