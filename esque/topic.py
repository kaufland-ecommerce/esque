import re
from typing import Dict, List, Tuple

import pykafka
from confluent_kafka.admin import ConfigResource
from confluent_kafka.cimpl import NewTopic

from esque.cluster import Cluster
from esque.errors import TopicDoesNotExistException, raise_for_kafka_exception
from esque.helpers import (
    ensure_kafka_futures_done,
    invalidate_cache_after,
    unpack_confluent_config,
)

class TopicConfig:
    def __init__(self, name: str, num_partitions: int, replication_factor: int, config: Dict[str, str] = None):
        self.name = name
        self.num_partitions = num_partitions
        self.replication_factor = replication_factor
        self.config = config

    def get_new_topic(self):
        config = {}
        if len(self.config) >= 1:
            config = self.config

        return NewTopic(
            self.name,
            num_partitions=self.num_partitions,
            replication_factor=self.replication_factor,
            config=config
        )

class Topic:
    def __init__(
            self,
            name: str,
            cluster: Cluster,
            num_partitions: int = None,
            replication_factor: int = None,
            config: Dict[str, str] = None
    ):
        self.name = name
        self.cluster: Cluster = cluster
        self._pykafka_topic_instance = None
        self._confluent_topic_instance = None

    @property
    def _pykafka_topic(self) -> pykafka.Topic:
        if not self._pykafka_topic_instance:
            self._pykafka_topic_instance = self.cluster.pykafka_client.cluster.topics[
                self.name
            ]
        return self._pykafka_topic_instance

    @property
    def _confluent_topic(self):
        if not self._confluent_topic_instance:
            self._confluent_topic_instance = self.cluster.confluent_client.list_topics(
                topic=self.name, timeout=10
            ).topics
        return self._confluent_topic_instance

    @property
    def low_watermark(self):
        return self._pykafka_topic.earliest_available_offsets()

    @property
    def partitions(self) -> List[int]:
        return list(self._pykafka_topic.partitions.keys())

    @property
    def high_watermark(self):
        return self._pykafka_topic.latest_available_offsets()

    def get_offsets(self) -> Dict[int, Tuple[int, int]]:
        """
        Returns the low and high watermark for each partition in a topic
        """
        partitions: List[int] = self.partitions
        low_watermark_offsets = self.low_watermark
        high_watermark_offsets = self.high_watermark

        return {
            partition_id: (
                int(low_watermark_offsets[partition_id][0][0]),
                int(high_watermark_offsets[partition_id][0][0]),
            )
            for partition_id in partitions
        }

    @raise_for_kafka_exception
    def config_diff(self) -> Dict[str, Tuple[str, str]]:
        conf = self.cluster.retrieve_config(ConfigResource.Type.TOPIC, self.name)
        config_list = unpack_confluent_config(conf)
        return {
            name: [str(value), str(self.config.get(name))]
            for name, value in config_list.items()
            if self.config.get(name) and str(self.config.get(name)) != str(value)
        }

    @raise_for_kafka_exception
    def describe(self):
        offsets = self.get_offsets()

        if not self._confluent_topic:
            raise TopicDoesNotExistException()
        replicas = [
            {
                f"Partition {partition}": {
                    "low_watermark": offsets[int(partition)][0],
                    "high_watermark": offsets[int(partition)][1],
                    "partition_isrs": partition_meta.isrs,
                    "partition_leader": partition_meta.leader,
                    "partition_replicas": partition_meta.replicas,
                }
            }
            for t in self._confluent_topic.values()
            for partition, partition_meta in t.partitions.items()
        ]

        conf = self.cluster.retrieve_config(ConfigResource.Type.TOPIC, self.name)
        conf = unpack_confluent_config(conf)

        return replicas, {"Config": conf}

    def __lt__(self, other):
        if self.name < other.name:
            return True
        return False


class TopicController:
    def __init__(self, cluster: Cluster):
        self.cluster: Cluster = cluster

    @raise_for_kafka_exception
    def list_topics(
        self, *, search_string: str = None, sort=True, hide_internal=True
    ) -> List[Topic]:
        self.cluster.confluent_client.poll(timeout=1)
        topics = self.cluster.confluent_client.list_topics().topics
        topics = [self.get_topic(t.topic) for t in topics.values()]
        if search_string:
            topics = [topic for topic in topics if re.match(search_string, topic.name)]
        if hide_internal:
            topics = [topic for topic in topics if not topic.name.startswith("__")]
        if sort:
            topics = sorted(topics)

        return topics

    @raise_for_kafka_exception
    def filter_existing_topics(self, topics: List[Topic]) -> List[Topic]:
        self.cluster.confluent_client.poll(timeout=1)
        confluent_topics = self.cluster.confluent_client.list_topics().topics
        existing_topic_names = [t.topic for t in confluent_topics.values()]
        return [topic for topic in topics if topic.name in existing_topic_names]

    @raise_for_kafka_exception
    @invalidate_cache_after
    def create_topic(
        self,
        topic_name: str,
        *,
        num_partitions: int = 1,
        replication_factor: int = 3,
        topic_config: Dict[str, str] = None,
    ):
        if not topic_config:
            topic_config = {}
        new_topic = NewTopic(
            topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
            config=topic_config,
        )
        return new_topic

    @raise_for_kafka_exception
    @invalidate_cache_after
    def create_topics(self, topics: List[Topic]):
        for topic in topics:
            new_topic = NewTopic(
                topic.name,
                num_partitions=topic.num_partitions,
                replication_factor=topic.replication_factor,
                config=topic.config
            )
            future_list = self.cluster.confluent_client.create_topics([new_topic])
            ensure_kafka_futures_done(list(future_list.values()))

    @raise_for_kafka_exception
    @invalidate_cache_after
    def alter_configs(self, topics: List[Topic]):
        if not topics:
            return
        for topic in topics:
            config_resource = ConfigResource(ConfigResource.Type.TOPIC, topic.name, topic.config)
            future_list = self.cluster.confluent_client.alter_configs([config_resource])
            ensure_kafka_futures_done(list(future_list.values()))

    @raise_for_kafka_exception
    @invalidate_cache_after
    def apply_topic_conf(
            self,
            topics: List[Topic]
    ):
        new_topics = []
        editable_topics = []
        existing_topic_names = [topic.name for topic in self.list_topics()]
        for topic in topics:
            if topic.name in existing_topic_names:
                editable_topics.append(topic)
                continue

            new_topics.append(topic)

        self.alter_configs(editable_topics)
        self.create_topics(new_topics)

        return new_topics

    @raise_for_kafka_exception
    @invalidate_cache_after
    def delete_topic(self, topic: Topic):
        future = self.cluster.confluent_client.delete_topics([topic.name])[topic.name]
        ensure_kafka_futures_done([future])

    def get_topic(self, topic_name: str) -> Topic:
        return Topic(topic_name, self.cluster)
