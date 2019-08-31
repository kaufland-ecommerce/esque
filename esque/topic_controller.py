import re
from collections import namedtuple
from typing import List, Dict

from confluent_kafka.admin import ConfigResource
from confluent_kafka.cimpl import NewTopic

from esque.cluster import Cluster
from esque.config import Config
from esque.errors import raise_for_kafka_exception
from esque.helpers import invalidate_cache_after, ensure_kafka_futures_done
from esque.topic import Topic

AttributeDiff = namedtuple("AttributeDiff", ["old", "new"])

class TopicController:
    def __init__(self, cluster: Cluster, config: Config):
        self.cluster: Cluster = cluster
        self.config = config

    @raise_for_kafka_exception
    def list_topics(self, *, search_string: str = None, sort: bool = True, hide_internal: bool = True) -> List[Topic]:
        self.cluster.confluent_client.poll(timeout=1)
        topic_results = self.cluster.confluent_client.list_topics().topics.values()
        topic_names = [t.topic for t in topic_results]
        if search_string:
            topic_names = [topic for topic in topic_names if re.match(search_string, topic)]
        if hide_internal:
            topic_names = [topic for topic in topic_names if not topic.startswith("__")]
        if sort:
            topic_names = sorted(topic_names)

        topics = list(map(self.get_cluster_topic, topic_names))
        return topics

    @raise_for_kafka_exception
    @invalidate_cache_after
    def create_topics(self, topics: List[Topic]):
        for topic in topics:
            partitions = topic.num_partitions if topic.num_partitions is not None else self.config.default_partitions
            replicas = topic.replication_factor if topic.replication_factor is not None else self.config.default_replication_factor
            new_topic = NewTopic(
                topic.name, num_partitions=partitions, replication_factor=replicas, config=topic.config
            )
            future_list = self.cluster.confluent_client.create_topics([new_topic])
            ensure_kafka_futures_done(list(future_list.values()))

    @raise_for_kafka_exception
    @invalidate_cache_after
    def alter_configs(self, topics: List[Topic]):
        for topic in topics:
            config_resource = ConfigResource(ConfigResource.Type.TOPIC, topic.name, topic.config)
            future_list = self.cluster.confluent_client.alter_configs([config_resource])
            ensure_kafka_futures_done(list(future_list.values()))

    @raise_for_kafka_exception
    @invalidate_cache_after
    def delete_topic(self, topic: Topic):
        future = self.cluster.confluent_client.delete_topics([topic.name])[topic.name]
        ensure_kafka_futures_done([future])

    def get_cluster_topic(self, topic_name: str) -> Topic:
        """Convenience function getting an existing topic based on topic_name"""
        return self.update_from_cluster(Topic(topic_name))

    @raise_for_kafka_exception
    def update_from_cluster(self, topic: Topic):
        """Takes a topic and, based on its name, updates all attributes from the cluster"""
        if topic.is_only_local:  # only have to instantiate those once
            topic._pykafka_topic = self.cluster.pykafka_client.cluster.topics[topic.name]
            topic._confluent_topic = self.cluster.confluent_client.list_topics(topic=topic.name, timeout=10).topics

        # TODO put the topic instances into a cache of this class
        low_watermarks = topic._pykafka_topic.earliest_available_offsets()
        high_watermarks = topic._pykafka_topic.latest_available_offsets()
        topic.update_partitions(low_watermarks, high_watermarks)

        topic.config = self.cluster.retrieve_config(ConfigResource.Type.TOPIC, topic.name)
        topic.is_only_local = False
        return topic

    @raise_for_kafka_exception
    def diff_with_cluster(self, topic: Topic) -> Dict[str, AttributeDiff]:
        cluster_state = self.cluster.retrieve_config(ConfigResource.Type.TOPIC, topic.name)
        out = {}
        for name, old_value in cluster_state.items():
            new_val = topic.config.get(name)
            if not new_val or str(new_val) == str(old_value):
                continue
            out[name] = AttributeDiff(str(old_value), str(new_val))

        return out
