import logging
import struct
from typing import Any, Dict, List, Optional, cast

import pykafka
from pykafka.protocol import OffsetFetchResponseV1, PartitionOffsetFetchRequest
from pykafka.protocol.admin import DescribeGroupResponse

from esque.cluster import Cluster
from esque.errors import ConsumerGroupDoesNotExistException

# TODO: Refactor this shit hole

log = logging.getLogger(__name__)


class ConsumerGroup:
    def __init__(self, id: str, cluster: Cluster):
        self.id = id
        self.cluster = cluster
        self._pykafka_group_coordinator_instance: Optional[pykafka.Broker] = None
        self.topic_controller = cluster.topic_controller

    @property
    def _pykafka_group_coordinator(self) -> pykafka.Broker:
        consumer_id = self.id.encode("UTF-8")
        if not self._pykafka_group_coordinator_instance:
            self._pykafka_group_coordinator_instance: pykafka.Broker = cast(
                pykafka.Broker, self.cluster.pykafka_client.cluster.get_group_coordinator(consumer_id)
            )

        return self._pykafka_group_coordinator_instance

    @property
    def topics(self):
        consumer_id = self.id.encode("UTF-8")

        # Get Consumers who already have an offset
        consumer_offsets = self._unpack_offset_response(
            self._pykafka_group_coordinator.fetch_consumer_group_offsets(consumer_id, preqs=[])
        )
        topic_with_offsets = set(topic.decode("UTF-8") for topic in consumer_offsets.keys())

        topics_with_members = set()
        # Get Consumers which have a member
        try:
            resp = self._pykafka_group_coordinator.describe_groups([consumer_id])
            meta = self._unpack_consumer_group_response(resp.groups[consumer_id])
            topics_with_members = set(
                member["member_metadata"]["subscription"][0].decode("UTF-8") for member in meta["members"]
            )
        except struct.error:
            pass

        return list(topic_with_offsets | topics_with_members)

    def describe(self, *, verbose=False):
        consumer_id = self.id.encode("UTF-8")
        if consumer_id not in self._pykafka_group_coordinator.list_groups().groups:
            raise ConsumerGroupDoesNotExistException(self.id)

        resp = self._pykafka_group_coordinator.describe_groups([consumer_id])
        assert len(resp.groups) == 1

        meta = self._unpack_consumer_group_response(resp.groups[consumer_id])
        consumer_offsets = self._get_consumer_offsets(consumer_id, verbose=verbose)

        return {
            "group_id": consumer_id,
            "group_coordinator": self._pykafka_group_coordinator.host,
            "offsets": consumer_offsets,
            "meta": meta,
        }

    def _get_consumer_offsets(self, consumer_id, verbose: bool):
        consumer_offsets = self._unpack_offset_response(
            self._pykafka_group_coordinator.fetch_consumer_group_offsets(consumer_id, preqs=[])
        )

        if verbose:
            for topic in consumer_offsets.keys():
                topic_watermarks = self.topic_controller.get_cluster_topic(topic).watermarks
                for partition_id, consumer_offset in list(consumer_offsets[topic].items()):
                    # TODO somehow include this in the returned dictionary
                    if partition_id not in topic_watermarks:
                        log.warning(
                            f"Found invalid offset! Partition {partition_id} does not exist for topic {topic.decode()}"
                        )
                        del consumer_offsets[topic][partition_id]
                        continue
                    consumer_offsets[topic][partition_id] = {
                        "consumer_offset": consumer_offset,
                        "topic_low_watermark": topic_watermarks[partition_id].low,
                        "topic_high_watermark": topic_watermarks[partition_id].high,
                        "consumer_lag": topic_watermarks[partition_id].high - consumer_offset,
                    }
            return consumer_offsets
        for topic in consumer_offsets.keys():
            topic_watermarks = self.topic_controller.get_cluster_topic(topic).watermarks
            new_consumer_offsets = {
                "consumer_offset": (float("inf"), float("-inf")),
                "topic_low_watermark": (float("inf"), float("-inf")),
                "topic_high_watermark": (float("inf"), float("-inf")),
                "consumer_lag": (float("inf"), float("-inf")),
            }
            for partition_id, consumer_offset in consumer_offsets[topic].items():
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

            return new_consumer_offsets

    def _get_member_assignment(self, member_assignment: Dict[str, Any]) -> List[PartitionOffsetFetchRequest]:
        """
        Creates a list of style [PartitionOffsetFetchRequest('topic', partition_id)]
        """
        return [
            PartitionOffsetFetchRequest(topic, partition)
            for member in member_assignment
            for topic, partitions in member["member_assignment"]["partition"].items()
            for partition in partitions
        ]

    def _unpack_offset_response(self, resp: OffsetFetchResponseV1) -> Dict[str, Any]:

        return {
            topic_name: {
                partition_id: partition_data._asdict()["offset"] for partition_id, partition_data in partitions.items()
            }
            for topic_name, partitions in resp.topics.items()
        }

    def _unpack_consumer_group_response(self, resp: DescribeGroupResponse) -> Dict[str, Any]:
        return {
            "group_id": resp.group_id,
            "protocol": resp.protocol,
            "state": resp.state,
            "error_code": resp.error_code,
            "protocol_type": resp.protocol_type,
            "members": [
                {
                    "member_id": member.member_id,
                    "client_id": member.client_id,
                    "client_host": member.client_host,
                    "member_metadata": {
                        # "version": member.member_metadata.version,
                        "subscription": [topic for topic in member.member_metadata.topic_names],
                        # "client": member.member_metadata.user_data,
                    },
                    "member_assignment": {
                        # "version": member.member_assignment.version,
                        "partition": {
                            assignment[0]: assignment[1]
                            for assignment in member.member_assignment.partition_assignment
                        }
                    },
                }
                for _, member in resp.members.items()
            ],
        }
