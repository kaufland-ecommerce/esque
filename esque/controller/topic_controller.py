import logging
import re
import time
from contextlib import closing
from itertools import islice
from logging import Logger
from typing import TYPE_CHECKING, Dict, List, Optional

import confluent_kafka
import pendulum
from confluent_kafka.admin import ConfigResource
from confluent_kafka.cimpl import KafkaException, NewTopic, TopicPartition

from esque.config import ESQUE_GROUP_ID, Config
from esque.helpers import ensure_kafka_future_done
from esque.resources.topic import Partition, Topic, TopicDiff

logger: Logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from esque.cluster import Cluster


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
            topics = list(map(self.get_cluster_topic, topic_names))
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
        config = Config.get_instance().create_confluent_config()
        config.update({"group.id": group_id})
        with closing(confluent_kafka.Consumer(config)) as consumer:
            topic_data = consumer.list_topics(topic=topic_name).topics[topic_name]
            topic_partitions_with_timestamp = [
                TopicPartition(topic=topic_name, partition=partition_id, offset=timestamp_limit.int_timestamp * 1000)
                for partition_id in topic_data.partitions.keys()
            ]
            topic_partitions_with_new_offsets = consumer.offsets_for_times(topic_partitions_with_timestamp)
        return {
            topic_partition.partition: topic_partition.offset for topic_partition in topic_partitions_with_new_offsets
        }

    def update_from_cluster(self, topic: Topic, *, retrieve_last_timestamp: bool = False) -> Topic:
        """Takes a topic and, based on its name, updates all attributes from the cluster"""

        topic.partition_data = self._get_partition_data(topic, retrieve_last_timestamp)
        topic.config = self.cluster.retrieve_config(ConfigResource.Type.TOPIC, topic.name)

        topic.is_only_local = False

        return topic

    def _get_partition_data(self, topic: Topic, retrieve_last_timestamp: bool) -> List[Partition]:

        config = Config.get_instance().create_confluent_config()
        config.update({"group.id": ESQUE_GROUP_ID, "topic.metadata.refresh.interval.ms": "250"})
        with closing(confluent_kafka.Consumer(config)) as consumer:
            confluent_topic = consumer.list_topics(topic=topic.name).topics[topic.name]
            partitions: List[Partition] = []
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

        cluster_topic = self.get_cluster_topic(local_topic.name)
        diffs = TopicDiff()
        diffs.set_diff("num_partitions", cluster_topic.num_partitions, local_topic.num_partitions)
        diffs.set_diff("replication_factor", cluster_topic.replication_factor, local_topic.replication_factor)

        for name, old_value in cluster_topic.config.items():
            diffs.set_diff(name, old_value, local_topic.config.get(name))

        return diffs
