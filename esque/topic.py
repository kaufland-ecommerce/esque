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
    def __init__(self, name: str, cluster: Cluster):
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
    def create_topics(self, topics: List[NewTopic]):
        if not topics:
            return
        for new_topic in topics:
            future_list = self.cluster.confluent_client.create_topics([new_topic])
            ensure_kafka_futures_done(list(future_list.values()))

    @raise_for_kafka_exception
    @invalidate_cache_after
    def alter_configs(self, configs: List[ConfigResource]):
        if not configs:
            return
        for config_resource in configs:
            future_list = self.cluster.confluent_client.alter_configs([config_resource])
            ensure_kafka_futures_done(list(future_list.values()))

    @raise_for_kafka_exception
    @invalidate_cache_after
    def apply_topic_conf(
            self,
            topics: Dict
    ):
        new_topics = []
        editable_topics = []
        existing_topic_names = [topic.name for topic in self.list_topics()]
        for topic in topics:
            topic_config = TopicConfig(
                topic.get("name"),
                topic.get("num_partitions"),
                topic.get("replication_factor"),
                topic.get("config")
            )
            if topic.get("name") in existing_topic_names:
                editable_topics.append(topic_config)
                continue

            new_topics.append(topic_config.get_new_topic())

        topics_config_diff = self.alter_topic_configs(editable_topics)
        self.create_topics(new_topics)

        return topics_config_diff, new_topics

    @raise_for_kafka_exception
    @invalidate_cache_after
    def delete_topic(self, topic_name):
        future = self.cluster.confluent_client.delete_topics([topic_name])[topic_name]
        ensure_kafka_futures_done([future])

    def get_topic(self, topic_name: str) -> Topic:
        return Topic(topic_name, self.cluster)

    @raise_for_kafka_exception
    @invalidate_cache_after
    def alter_topic_configs(self, editable_topics: List[TopicConfig]) -> Dict[str, Dict[str, List[str]]]:
        config_resources = []
        topics_diff = {}
        for topic_config in editable_topics:
            if not topic_config.config:
                continue

            conf = self.cluster.retrieve_config(ConfigResource.Type.TOPIC, topic_config.name)
            config_list = unpack_confluent_config(conf)
            new_config = topic_config.config
            config_diff = {}
            for name, value in config_list.items():
                if not new_config.get(name):
                    continue
                if str(new_config.get(name)) != str(value):
                    config_diff[name] = [str(value), str(new_config.get(name))]

            config_resources.append(
                ConfigResource(ConfigResource.Type.TOPIC, topic_config.name, topic_config.config)
            )

            if len(config_diff) >= 1:
                topics_diff[topic_config.name] = config_diff

        if len(config_resources) >= 1:
            self.alter_configs(config_resources)

        return topics_diff
