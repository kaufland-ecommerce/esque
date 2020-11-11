import logging
import re
from enum import Enum
from itertools import islice
from logging import Logger
from typing import TYPE_CHECKING, Dict, List, Union

import confluent_kafka
import pendulum
from click import BadParameter
from confluent_kafka.admin import ConfigResource
from confluent_kafka.admin import TopicMetadata as ConfluentTopic
from confluent_kafka.cimpl import NewTopic, TopicPartition
from pykafka.topic import Topic as PyKafkaTopic

from esque.clients.consumer import MessageConsumer
from esque.config import ESQUE_GROUP_ID, Config
from esque.errors import MessageEmptyException, raise_for_kafka_error
from esque.helpers import ensure_kafka_future_done, invalidate_cache_after
from esque.resources.topic import Partition, PartitionInfo, Topic, TopicDiff

logger: Logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from esque.cluster import Cluster


class ClientTypes(Enum):
    Confluent = "Confluent"
    PyKafka = "PyKafka"


ClientType = Union[ConfluentTopic, PyKafkaTopic]


class TopicController:
    def __init__(self, cluster: "Cluster", config: Config):
        self.cluster: "Cluster" = cluster
        self.config = config

    def _get_client_topic(self, topic_name: str, client_type: ClientTypes) -> ClientType:
        confluent_topics = self.cluster.confluent_client.list_topics(topic=topic_name, timeout=10).topics
        # Confluent returns a list of requested topics with an Error as result if the topic doesn't exist
        topic_metadata: ConfluentTopic = confluent_topics[topic_name]
        raise_for_kafka_error(topic_metadata.error)
        if client_type == ClientTypes.Confluent:
            return confluent_topics[topic_name]
        elif client_type == ClientTypes.PyKafka:
            # at least PyKafka does it's own caching, so we don't have to bother
            pykafka_topics = self.cluster.pykafka_client.cluster.topics
            return pykafka_topics[topic_name]
        else:
            raise BadParameter(f"TopicType needs to be part of {ClientTypes}", param=client_type)

    def list_topics(
        self,
        *,
        search_string: str = None,
        sort: bool = True,
        hide_internal: bool = False,
        get_topic_objects: bool = True,
    ) -> List[Topic]:
        self.cluster.confluent_client.poll(timeout=1)
        topic_results = self.cluster.confluent_client.list_topics().topics.values()
        topic_names = [t.topic for t in topic_results]
        if search_string:
            topic_names = [topic for topic in topic_names if re.match(search_string, topic)]
        if hide_internal:
            topic_names = [topic for topic in topic_names if not topic.startswith("__")]
        if sort:
            topic_names = sorted(topic_names)

        if get_topic_objects:
            topics = list(map(self.get_cluster_topic, topic_names))
        else:
            topics = list(map(self.get_local_topic, topic_names))
        return topics

    @invalidate_cache_after
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

    @invalidate_cache_after
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

    @invalidate_cache_after
    def delete_topic(self, topic: Topic):
        future = self.cluster.confluent_client.delete_topics([topic.name])[topic.name]
        ensure_kafka_future_done(future)

    def get_cluster_topic(self, topic_name: str, *, retrieve_last_timestamp: bool = False) -> Topic:
        """Convenience function getting an existing topic based on topic_name"""
        return self.update_from_cluster(Topic(topic_name), retrieve_last_timestamp=retrieve_last_timestamp)

    def get_local_topic(self, topic_name: str) -> Topic:
        return Topic(topic_name)

    def get_offsets_closest_to_timestamp(
        self, group_id: str, topic_name: str, timestamp_limit: pendulum
    ) -> Dict[int, int]:
        topic = self.get_cluster_topic(topic_name=topic_name)
        config = Config.get_instance().create_confluent_config()
        config.update({"group.id": group_id})
        consumer = confluent_kafka.Consumer(config)
        topic_partitions_with_timestamp = [
            TopicPartition(topic.name, partition.partition_id, timestamp_limit.int_timestamp * 1000)
            for partition in topic.partitions
        ]
        topic_partitions_with_new_offsets = consumer.offsets_for_times(topic_partitions_with_timestamp)
        return {
            topic_partition.partition: topic_partition.offset for topic_partition in topic_partitions_with_new_offsets
        }

    def update_from_cluster(self, topic: Topic, *, retrieve_last_timestamp: bool = False) -> Topic:
        """Takes a topic and, based on its name, updates all attributes from the cluster"""

        confluent_topic: ConfluentTopic = self._get_client_topic(topic.name, ClientTypes.Confluent)
        pykafka_topic: PyKafkaTopic = self._get_client_topic(topic.name, ClientTypes.PyKafka)
        low_watermarks = pykafka_topic.earliest_available_offsets()
        high_watermarks = pykafka_topic.latest_available_offsets()

        topic.partition_data = self._get_partition_data(
            confluent_topic, low_watermarks, high_watermarks, topic, retrieve_last_timestamp
        )
        topic.config = self.cluster.retrieve_config(ConfigResource.Type.TOPIC, topic.name)

        topic.is_only_local = False

        return topic

    def _get_partition_data(
        self,
        confluent_topic: ConfluentTopic,
        low_watermarks: PartitionInfo,
        high_watermarks: PartitionInfo,
        topic: Topic,
        retrieve_last_timestamp: bool,
    ) -> List[Partition]:

        consumer = MessageConsumer(ESQUE_GROUP_ID, topic.name, True, enable_auto_commit=False)
        partitions = []

        for partition_id, meta in confluent_topic.partitions.items():
            low = int(low_watermarks[partition_id].offset[0])
            high = int(high_watermarks[partition_id].offset[0])
            latest_timestamp = None
            if high > low and retrieve_last_timestamp:
                try:
                    latest_timestamp = float(consumer.consume(high - 1, partition_id).timestamp()[1]) / 1000
                except MessageEmptyException:
                    logger.warning(
                        f"Due to timeout latest timestamp for topic `{topic.name}` and partition `{partition_id}` is missing."
                    )
            partition = Partition(partition_id, low, high, meta.isrs, meta.leader, meta.replicas, latest_timestamp)
            partitions.append(partition)

        return partitions

    def diff_with_cluster(self, local_topic: Topic) -> TopicDiff:
        assert local_topic.is_only_local, "Can only diff local topics with remote"

        cluster_topic = self.get_cluster_topic(local_topic.name)
        diffs = TopicDiff()
        diffs.set_diff("num_partitions", cluster_topic.num_partitions, local_topic.num_partitions)
        diffs.set_diff("replication_factor", cluster_topic.replication_factor, local_topic.replication_factor)

        for name, old_value in cluster_topic.config.items():
            diffs.set_diff(name, old_value, local_topic.config.get(name))

        return diffs
