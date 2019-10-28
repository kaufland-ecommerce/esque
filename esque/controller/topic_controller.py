import re
from enum import Enum
from itertools import islice
from typing import List, TYPE_CHECKING, Union

from click import BadParameter
from confluent_kafka.admin import ConfigResource, TopicMetadata as ConfluentTopic, TopicMetadata
from confluent_kafka.cimpl import NewTopic
from pykafka.topic import Topic as PyKafkaTopic

from esque.clients.consumer import MessageConsumer
from esque.config import Config, PING_GROUP_ID
from esque.errors import translate_third_party_exceptions, raise_for_kafka_error
from esque.helpers import invalidate_cache_after, ensure_kafka_future_done
from esque.resources.topic import Partition, PartitionInfo, Topic, TopicDiff

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

    @translate_third_party_exceptions
    def _get_client_topic(self, topic_name: str, client_type: ClientTypes) -> ClientType:
        confluent_topics = self.cluster.confluent_client.list_topics(topic=topic_name, timeout=10).topics
        # Confluent returns a list of requested topics with an Error as result if the topic doesn't exist
        topic_metadata: TopicMetadata = confluent_topics[topic_name]
        raise_for_kafka_error(topic_metadata.error)
        if client_type == ClientTypes.Confluent:
            return confluent_topics[topic_name]
        elif client_type == ClientTypes.PyKafka:
            # at least PyKafka does it's own caching, so we don't have to bother
            pykafka_topics = self.cluster.pykafka_client.cluster.topics
            return pykafka_topics[topic_name]
        else:
            raise BadParameter(f"TopicType needs to be part of {ClientTypes}", param=client_type)

    @translate_third_party_exceptions
    def list_topics(
        self,
        *,
        search_string: str = None,
        sort: bool = True,
        hide_internal: bool = True,
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

    @translate_third_party_exceptions
    @invalidate_cache_after
    def create_topics(self, topics: List[Topic]):
        for topic in topics:
            partitions = topic.num_partitions if topic.num_partitions is not None else self.config.default_partitions
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

    @translate_third_party_exceptions
    @invalidate_cache_after
    def alter_configs(self, topics: List[Topic]):
        for topic in topics:
            config_resource = ConfigResource(ConfigResource.Type.TOPIC, topic.name, topic.config)
            future_list = self.cluster.confluent_client.alter_configs([config_resource])
            ensure_kafka_future_done(next(islice(future_list.values(), 1)))

    @translate_third_party_exceptions
    @invalidate_cache_after
    def delete_topic(self, topic: Topic):
        future = self.cluster.confluent_client.delete_topics([topic.name])[topic.name]
        ensure_kafka_future_done(future)

    def get_cluster_topic(self, topic_name: str) -> Topic:
        """Convenience function getting an existing topic based on topic_name"""
        return self.update_from_cluster(Topic(topic_name))

    def get_local_topic(self, topic_name: str) -> Topic:
        return Topic(topic_name)

    @translate_third_party_exceptions
    def update_from_cluster(self, topic: Topic):
        """Takes a topic and, based on its name, updates all attributes from the cluster"""

        confluent_topic: ConfluentTopic = self._get_client_topic(topic.name, ClientTypes.Confluent)
        pykafka_topic: PyKafkaTopic = self._get_client_topic(topic.name, ClientTypes.PyKafka)
        low_watermarks = pykafka_topic.earliest_available_offsets()
        high_watermarks = pykafka_topic.latest_available_offsets()

        topic.partition_data = self._get_partition_data(confluent_topic, low_watermarks, high_watermarks, topic)
        topic.config = self.cluster.retrieve_config(ConfigResource.Type.TOPIC, topic.name)

        topic.is_only_local = False

        return topic

    @translate_third_party_exceptions
    def _get_partition_data(
        self,
        confluent_topic: ConfluentTopic,
        low_watermarks: PartitionInfo,
        high_watermarks: PartitionInfo,
        topic: Topic,
    ) -> List[Partition]:

        consumer = MessageConsumer(PING_GROUP_ID, topic.name, True)
        partitions = []

        for partition_id, meta in confluent_topic.partitions.items():
            low = int(low_watermarks[partition_id].offset[0])
            high = int(high_watermarks[partition_id].offset[0])
            if high > low:
                latest_timestamp = float(consumer.consume(high - 1, partition_id).timestamp()[1]) / 1000
            else:
                latest_timestamp = None
            partition = Partition(partition_id, low, high, meta.isrs, meta.leader, meta.replicas, latest_timestamp)
            partitions.append(partition)

        return partitions

    @translate_third_party_exceptions
    def diff_with_cluster(self, local_topic: Topic) -> TopicDiff:
        assert local_topic.is_only_local, "Can only diff local topics with remote"

        cluster_topic = self.get_cluster_topic(local_topic.name)
        diffs = TopicDiff()
        diffs.set_diff("num_partitions", cluster_topic.num_partitions, local_topic.num_partitions)
        diffs.set_diff("replication_factor", cluster_topic.replication_factor, local_topic.replication_factor)

        for name, old_value in cluster_topic.config.items():
            diffs.set_diff(name, old_value, local_topic.config.get(name))

        return diffs
