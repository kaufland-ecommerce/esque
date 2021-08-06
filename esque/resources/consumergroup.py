import logging
from collections import defaultdict
from typing import Dict, Tuple, Union

from esque.cluster import Cluster
from esque.errors import ConsumerGroupDoesNotExistException, ExceptionWithMessage
from esque.resources.topic import Topic

N = Union[int, float]  # N for number
log = logging.getLogger(__name__)


class ConsumerGroup:
    def __init__(self, id: str, cluster: Cluster):
        self.id = id
        self.cluster = cluster
        self.topic_controller = cluster.topic_controller

    @property
    def topics(self):
        return set(self.get_offsets().keys())

    def get_offsets(self) -> Dict[str, Dict[int, int]]:
        consumer_offsets = defaultdict(dict)
        offset_response = self.cluster.kafka_python_client.list_consumer_group_offsets(group_id=self.id)
        for tp, offset in offset_response.items():
            consumer_offsets[tp.topic][tp.partition] = offset.offset
        return consumer_offsets

    def describe(self, *, partitions=False, timestamps=False):
        coordinator_id = self.cluster.kafka_python_client._find_coordinator_ids(group_ids=[self.id])[self.id]
        description = self.cluster.kafka_python_client.describe_consumer_groups(
            group_ids=[self.id], group_coordinator_id=coordinator_id
        )[0]
        if description.state == "Dead":
            raise ConsumerGroupDoesNotExistException(self.id)

        consumer_offsets = self._get_consumer_offsets(partitions=partitions, timestamps=timestamps)
        for broker in self.cluster.brokers:
            if broker["id"] == coordinator_id:
                coordinator_host = broker["host"]
                break
        else:
            raise ExceptionWithMessage(f"Couldn't find broker with id {coordinator_id}")

        return {
            "group_id": self.id,
            "group_coordinator": coordinator_host,
            "offsets": consumer_offsets,
            "meta": {
                "error_code": description.error_code,
                "group": description.group,
                "state": description.state,
                "protocol_type": description.protocol_type,
                "protocol": description.protocol,
                "members": [
                    {
                        "member_id": member.member_id,
                        "client_id": member.client_id,
                        "client_host": member.client_host,
                        "member_metadata": member.member_metadata,
                        "member_assignment": member.member_assignment,
                    }
                    for member in description.members
                ],
                "authorized_operations": description.authorized_operations,
            },
        }

    def _update_minmax(self, minmax: Tuple[N, N], value: N) -> Tuple[N, N]:
        prev_min, prev_max = minmax
        return (min(prev_min, value), max(prev_max, value))

    def _prepare_offsets(self, topic: Topic, partition_data: Dict[int, int], timestamps: bool = False):
        extended_partition_data = topic.partition_data
        topic_watermarks = topic.watermarks
        new_consumer_offsets = {
            "consumer_offset": (float("inf"), float("-inf")),
            "topic_low_watermark": (float("inf"), float("-inf")),
            "topic_high_watermark": (float("inf"), float("-inf")),
            "consumer_lag": (float("inf"), float("-inf")),
        }
        for partition_id, consumer_offset in partition_data.items():
            current_offset = consumer_offset
            new_consumer_offsets["consumer_offset"] = self._update_minmax(
                new_consumer_offsets["consumer_offset"], current_offset
            )
            new_consumer_offsets["topic_low_watermark"] = self._update_minmax(
                new_consumer_offsets["topic_low_watermark"], topic_watermarks[partition_id].low
            )
            new_consumer_offsets["topic_high_watermark"] = self._update_minmax(
                new_consumer_offsets["topic_high_watermark"], topic_watermarks[partition_id].high
            )
            new_consumer_offsets["consumer_lag"] = self._update_minmax(
                new_consumer_offsets["consumer_lag"], topic_watermarks[partition_id].high - current_offset
            )

            if timestamps:
                extended_partition_data = topic.get_partition_data(partition_id)
                if extended_partition_data and extended_partition_data.latest_message_timestamp:
                    if "latest_timestamp" in new_consumer_offsets:
                        new_consumer_offsets["latest_timestamp"] = self._update_minmax(
                            new_consumer_offsets["latest_timestamp"], extended_partition_data.latest_message_timestamp
                        )
                    else:
                        new_consumer_offsets["latest_timestamp"] = (
                            extended_partition_data.latest_message_timestamp,
                            extended_partition_data.latest_message_timestamp,
                        )
        return new_consumer_offsets

    def _prepare_partition_offsets(self, topic: Topic, partition_data: Dict[int, int], timestamps: bool = False):
        topic_watermarks = topic.watermarks
        offsets = {}
        for partition_id, consumer_offset in partition_data.items():
            # TODO somehow include this in the returned dictionary
            if partition_id not in topic_watermarks:
                log.warning(f"Found invalid offset! Partition {partition_id} does not exist for topic {topic.name}")
            offsets[partition_id] = {
                "consumer_offset": consumer_offset,
                "topic_low_watermark": topic_watermarks[partition_id].low,
                "topic_high_watermark": topic_watermarks[partition_id].high,
                "consumer_lag": topic_watermarks[partition_id].high - consumer_offset,
            }
            if timestamps:
                extended_partition_data = topic.get_partition_data(partition_id)
                if extended_partition_data and extended_partition_data.latest_message_timestamp:
                    offsets[partition_id]["latest_timestamp"] = extended_partition_data.latest_message_timestamp
        return offsets

    def _get_consumer_offsets(self, partitions: bool, timestamps: bool):
        consumer_offsets = {}
        handler = self._prepare_offsets
        if partitions:
            handler = self._prepare_partition_offsets

        for topic_id, partition_data in self.get_offsets().items():
            topic = self.topic_controller.get_cluster_topic(topic_id, retrieve_last_timestamp=timestamps)
            consumer_offsets[topic_id] = handler(topic, partition_data, timestamps)
        return consumer_offsets
