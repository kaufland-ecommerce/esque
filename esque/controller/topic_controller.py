import logging
import re
import time
from contextlib import closing
from itertools import islice
from logging import Logger
from typing import TYPE_CHECKING, Dict, Iterable, List, NamedTuple, Optional, Union, cast

import confluent_kafka
import kafka
import kafka.consumer.fetcher
import pendulum
from confluent_kafka.admin import ConfigResource
from confluent_kafka.cimpl import OFFSET_END, KafkaException, NewTopic, TopicPartition

from esque.config import ESQUE_GROUP_ID, Config
from esque.errors import TopicDeletionException, TopicDoesNotExistException
from esque.helpers import ensure_kafka_future_done
from esque.resources.topic import Partition, Topic, TopicDiff

logger: Logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from esque.cluster import Cluster


class OffsetWithTimestamp(NamedTuple):
    topic: str
    partition: int
    offset: int
    timestamp_ms: Optional[int]


class TopicController:
    def __init__(self, cluster: "Cluster", config: Optional[Config] = None):
        self.cluster: "Cluster" = cluster
        if config is None:
            config = Config.get_instance()
        self.config = config

    def list_topics(
        self,
        *,
        search_string: str = None,
        sort: bool = True,
        hide_internal: bool = False,
        get_topic_objects: bool = True,
        get_partitions: bool = True,
    ) -> List[Topic]:
        topic_results = self.cluster.confluent_client.list_topics().topics.values()
        topic_names = [t.topic for t in topic_results]
        if search_string:
            topic_names = [topic for topic in topic_names if re.match(search_string, topic)]
        if hide_internal:
            topic_names = [topic for topic in topic_names if not topic.startswith("__")]
        if sort:
            topic_names = sorted(topic_names)

        if get_topic_objects:
            topics = [
                self.get_cluster_topic(topic_name, retrieve_partition_watermarks=get_partitions)
                for topic_name in topic_names
            ]
        else:
            topics = list(map(self.get_local_topic, topic_names))
        return topics

    def create_topics(self, topics: List[Topic]):
        for topic in topics:
            partitions = (
                topic.num_partitions if topic.num_partitions is not None else self.config.default_num_partitions
            )
            replicas = (
                topic.replication_factor
                if topic.replication_factor is not None
                else self.config.default_replication_factor
            )
            new_topic = NewTopic(
                topic.name, num_partitions=partitions, replication_factor=replicas, config=topic.config
            )
            future_list = self.cluster.confluent_client.create_topics([new_topic], operation_timeout=60)
            ensure_kafka_future_done(next(islice(future_list.values(), 1)))
            for _ in range(80):
                topic_data = self.cluster.confluent_client.list_topics(topic=topic.name).topics[topic.name]
                if topic_data.error is None:
                    break
                time.sleep(0.125)
            else:
                raise RuntimeError(f"Couldn't create topic {topic}")

    def alter_configs(self, topics: List[Topic]):
        for topic in topics:
            altered_config = self._get_altered_config(topic)
            config_resource = ConfigResource(ConfigResource.Type.TOPIC, topic.name, altered_config)
            future_list = self.cluster.confluent_client.alter_configs([config_resource])
            ensure_kafka_future_done(next(islice(future_list.values(), 1)))

    def _get_altered_config(self, topic: Topic) -> Dict[str, str]:
        cluster_topic = self.get_cluster_topic(topic.name)
        current_config = cluster_topic.config.items()
        altered_config = {}
        for name, value in current_config:
            if name in topic.config:
                altered_config[name] = topic.config[name]
                continue
            altered_config[name] = value
        return altered_config

    def delete_topic(self, topic: Topic) -> bool:
        return self.delete_topics([topic])

    def delete_topics(self, topics: List[Topic]) -> bool:
        futures = self.cluster.confluent_client.delete_topics([topic.name for topic in topics], operation_timeout=60)
        errors: List[str] = []
        for topic_name, future in futures.items():
            try:
                future.result()
            except KafkaException as e:
                errors.append(f"[{topic_name}]: {e.args[0].str()}")
        if errors:
            raise TopicDeletionException("The following exceptions occurred:\n " + "\n ".join(sorted(errors)))
        return True

    def topic_exists(self, topic_name: str) -> bool:
        try:
            self.get_cluster_topic(topic_name, retrieve_last_timestamp=False, retrieve_partition_watermarks=False)
        except TopicDoesNotExistException:
            return False
        return True

    def get_cluster_topic(
        self, topic_name: str, *, retrieve_last_timestamp: bool = False, retrieve_partition_watermarks: bool = True
    ) -> Topic:
        """Convenience function getting an existing topic based on topic_name"""
        return self.update_from_cluster(
            Topic(topic_name),
            retrieve_last_timestamp=retrieve_last_timestamp,
            retrieve_partition_watermarks=retrieve_partition_watermarks,
        )

    def get_local_topic(self, topic_name: str) -> Topic:
        return Topic(topic_name)

    def get_timestamp_of_closest_offset(
        self, topic_name: str, offset: Union[int, str]
    ) -> Dict[int, OffsetWithTimestamp]:
        """
        Gets the timestamp for the message(s) at or right after `offset` in topic `topic_name`.
        The timestamps given in the result are the actual timestamp of the offset that was found.
        If there is no message at or after the given `timestamp`, the resulting offset will be `-1` i.e. end of topic
        partition _not including_ the last message.

        If the topic has multiple partitions, the `offset` wil be used for every partition.
        If there is no message at `offset`, the next available offset will be used.
        If there is no message at all after `offset`, offset will be `-1` and timestamp will be `None` in the output for the
        corresponding partition.
        In the Kafka world, -1 corresponds to the position after the last known message i.e. the end of the topic partition
        _not including_ the last message in the partition.

        :param topic_name: The topic to get the offsets for.
        :param offset: The offset to find timestamps for.
        :return: Dict: partition id -> offset with timestamp.
        """
        messages_received = self._read_one_message_per_partition(topic_name, offset)

        data: Dict[int, OffsetWithTimestamp] = {}
        for partition_id, msg in messages_received.items():
            if msg is None:
                data[partition_id] = OffsetWithTimestamp(
                    topic=topic_name, partition=partition_id, offset=OFFSET_END, timestamp_ms=None
                )
            else:
                data[partition_id] = OffsetWithTimestamp(
                    topic=topic_name, partition=partition_id, offset=msg.offset, timestamp_ms=msg.timestamp
                )
        return data

    def _read_one_message_per_partition(self, topic_name: str, offset: Union[str, int]):
        config = self.config.create_kafka_python_config()
        with closing(kafka.KafkaConsumer(**config)) as consumer:
            topic_partitions = [
                kafka.TopicPartition(topic=topic_name, partition=partition)
                for partition in consumer.partitions_for_topic(topic_name)
            ]
            partition_ends: Dict[kafka.TopicPartition, int] = consumer.end_offsets(topic_partitions)
            partition_starts: Dict[kafka.TopicPartition, int] = consumer.beginning_offsets(topic_partitions)

            if offset == "first":
                partition_offsets = (partition_starts[tp] for tp in topic_partitions)
            elif offset == "last":
                partition_offsets = (partition_ends[tp] - 1 for tp in topic_partitions)
            else:
                partition_offsets = (max(offset, partition_starts[tp]) for tp in topic_partitions)

            assignments = [
                (tp, offset) for tp, offset in zip(topic_partitions, partition_offsets) if partition_ends[tp] > offset
            ]

            consumer.assign([tp for tp, _ in assignments])
            for tp, offset in assignments:
                consumer.seek(tp, offset)

            unassigned_partitions = [tp for tp in topic_partitions if tp not in consumer.assignment()]
            messages_received: Dict[int, Optional[kafka.consumer.fetcher.ConsumerRecord]] = {
                tp.partition: None for tp in unassigned_partitions
            }

            for message in cast(Iterable[kafka.consumer.fetcher.ConsumerRecord], consumer):
                if message.partition not in messages_received:
                    messages_received[message.partition] = message
                    consumer.pause(kafka.TopicPartition(message.topic, message.partition))
                if len(messages_received) == len(topic_partitions):
                    # we have one record for every partition, so we're done.
                    break
        return messages_received

    def get_offsets_closest_to_timestamp(
        self, topic_name: str, timestamp: pendulum.DateTime
    ) -> Dict[int, OffsetWithTimestamp]:
        """
        Gets the offsets of the message(s) in `topic_name` whose timestamps are at, or right after, the given `timestamp`.
        The timestamps given in the result are the actual timestamp of the offset that was found.
        If there is no message at or after the given `timestamp`, the resulting offset will be `-1` i.e. end of topic
        partition _not including_ the last message.

        :param topic_name: The topic to get the offsets for.
        :param timestamp: The timestamp to find offsets for.
        :return: Dict: partition id -> offset with timestamp.
        """

        config = self.config.create_kafka_python_config()
        with closing(kafka.KafkaConsumer(**config)) as consumer:
            topic_partitions = [
                kafka.TopicPartition(topic=topic_name, partition=partition)
                for partition in consumer.partitions_for_topic(topic_name)
            ]
            timestamp_ms = int(timestamp.timestamp() * 1000)
            offsets: Dict[kafka.TopicPartition, kafka.structs.OffsetAndTimestamp] = consumer.offsets_for_times(
                {tp: timestamp_ms for tp in topic_partitions}
            )

        data: Dict[int, OffsetWithTimestamp] = {}
        for tp, offset_data in offsets.items():
            if offset_data is None:
                data[tp.partition] = OffsetWithTimestamp(
                    topic=tp.topic, partition=tp.partition, offset=OFFSET_END, timestamp_ms=None
                )
            else:
                data[tp.partition] = OffsetWithTimestamp(
                    topic=tp.topic,
                    partition=tp.partition,
                    offset=offset_data.offset,
                    timestamp_ms=offset_data.timestamp,
                )
        return data

    def update_from_cluster(
        self, topic: Topic, *, retrieve_last_timestamp: bool = False, retrieve_partition_watermarks: bool = True
    ) -> Topic:
        """Takes a topic and, based on its name, updates all attributes from the cluster"""

        topic.partition_data = self._get_partitions(
            topic, retrieve_last_timestamp, get_partition_watermarks=retrieve_partition_watermarks
        )
        topic.config = self.cluster.retrieve_config(ConfigResource.Type.TOPIC, topic.name)

        topic.is_only_local = False

        return topic

    def _get_partitions(
        self, topic: Topic, retrieve_last_timestamp: bool, get_partition_watermarks: bool = True
    ) -> List[Partition]:
        assert not (
            retrieve_last_timestamp and not get_partition_watermarks
        ), "Can not retrieve timestamp without partition watermarks"

        config = Config.get_instance().create_confluent_config()
        config.update({"group.id": ESQUE_GROUP_ID, "topic.metadata.refresh.interval.ms": "250"})
        with closing(confluent_kafka.Consumer(config)) as consumer:
            confluent_topic = consumer.list_topics(topic=topic.name).topics[topic.name]
            partitions: List[Partition] = []
            if not get_partition_watermarks:
                return [
                    Partition(partition_id, -1, -1, meta.isrs, meta.leader, meta.replicas, None)
                    for partition_id, meta in confluent_topic.partitions.items()
                ]
            for partition_id, meta in confluent_topic.partitions.items():
                try:
                    low, high = consumer.get_watermark_offsets(
                        TopicPartition(topic=topic.name, partition=partition_id)
                    )
                except KafkaException:
                    # retry after metadata should be refreshed (also consider small network delays)
                    # unfortunately we cannot explicitly cause and wait for a metadata refresh
                    time.sleep(1)
                    low, high = consumer.get_watermark_offsets(
                        TopicPartition(topic=topic.name, partition=partition_id)
                    )

                latest_timestamp = None
                if high > low and retrieve_last_timestamp:
                    assignment = [TopicPartition(topic=topic.name, partition=partition_id, offset=high - 1)]
                    consumer.assign(assignment)
                    msg = consumer.poll(timeout=10)
                    if msg is None:
                        logger.warning(
                            f"Due to timeout latest timestamp for topic `{topic.name}` "
                            f"and partition `{partition_id}` is missing."
                        )
                    else:
                        latest_timestamp = float(msg.timestamp()[1]) / 1000
                partition = Partition(partition_id, low, high, meta.isrs, meta.leader, meta.replicas, latest_timestamp)
                partitions.append(partition)
        return partitions

    def diff_with_cluster(self, local_topic: Topic) -> TopicDiff:
        assert local_topic.is_only_local, "Can only diff local topics with remote"

        cluster_topic = self.get_cluster_topic(local_topic.name, retrieve_partition_watermarks=False)
        diffs = TopicDiff()
        diffs.set_diff("num_partitions", cluster_topic.num_partitions, local_topic.num_partitions)
        diffs.set_diff("replication_factor", cluster_topic.replication_factor, local_topic.replication_factor)

        for name, old_value in cluster_topic.config.items():
            diffs.set_diff(name, old_value, local_topic.config.get(name))

        return diffs
