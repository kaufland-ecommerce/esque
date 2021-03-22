import logging
from collections import defaultdict
from typing import Any, Dict

from esque.cluster import Cluster
from esque.errors import ConsumerGroupDoesNotExistException, ExceptionWithMessage

# TODO: Refactor this shit hole

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

    def describe(self, *, verbose=False):
        coordinator_id = self.cluster.kafka_python_client._find_coordinator_ids(group_ids=[self.id])[self.id]
        description = self.cluster.kafka_python_client.describe_consumer_groups(
            group_ids=[self.id], group_coordinator_id=coordinator_id
        )[0]
        if description.state == "Dead":
            raise ConsumerGroupDoesNotExistException(self.id)

        consumer_offsets = self._get_consumer_offsets(verbose=verbose)
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

    def _get_consumer_offsets(self, verbose: bool):
        consumer_offsets: Dict[str, Dict[int, Dict[str, Any]]] = {}

        if verbose:
            for topic, partition_data in self.get_offsets().items():
                consumer_offsets[topic] = {}
                for partition_id, consumer_offset in partition_data.items():
                    topic_watermarks = self.topic_controller.get_cluster_topic(topic).watermarks
                    # TODO somehow include this in the returned dictionary
                    if partition_id not in topic_watermarks:
                        log.warning(f"Found invalid offset! Partition {partition_id} does not exist for topic {topic}")
                    consumer_offsets[topic][partition_id] = {
                        "consumer_offset": consumer_offset,
                        "topic_low_watermark": topic_watermarks[partition_id].low,
                        "topic_high_watermark": topic_watermarks[partition_id].high,
                        "consumer_lag": topic_watermarks[partition_id].high - consumer_offset,
                    }
            return consumer_offsets

        for topic, partition_data in self.get_offsets().items():
            consumer_offsets[topic] = {}
            topic_watermarks = self.topic_controller.get_cluster_topic(topic).watermarks
            new_consumer_offsets = {
                "consumer_offset": (float("inf"), float("-inf")),
                "topic_low_watermark": (float("inf"), float("-inf")),
                "topic_high_watermark": (float("inf"), float("-inf")),
                "consumer_lag": (float("inf"), float("-inf")),
            }
            for partition_id, consumer_offset in partition_data.items():
                current_offset = consumer_offset
                old_min, old_max = new_consumer_offsets["consumer_offset"]
                new_consumer_offsets["consumer_offset"] = (min(old_min, current_offset), max(old_max, current_offset))

                old_min, old_max = new_consumer_offsets["topic_low_watermark"]
                new_consumer_offsets["topic_low_watermark"] = (
                    min(old_min, topic_watermarks[partition_id].low),
                    max(old_max, topic_watermarks[partition_id].low),
                )

                old_min, old_max = new_consumer_offsets["topic_high_watermark"]
                new_consumer_offsets["topic_high_watermark"] = (
                    min(old_min, topic_watermarks[partition_id].high),
                    max(old_max, topic_watermarks[partition_id].high),
                )

                old_min, old_max = new_consumer_offsets["consumer_lag"]
                new_consumer_offsets["consumer_lag"] = (
                    min(old_min, topic_watermarks[partition_id].high - current_offset),
                    max(old_max, topic_watermarks[partition_id].high - current_offset),
                )
            consumer_offsets[topic] = new_consumer_offsets
        return consumer_offsets
