from typing import Any, Dict, List, Optional, cast

import pykafka
from pykafka.protocol import OffsetFetchResponseV1, PartitionOffsetFetchRequest
from pykafka.protocol.admin import DescribeGroupResponse

from esque.cluster import Cluster
from esque.errors import ConsumerGroupDoesNotExistException
from esque.topic import TopicController


# TODO: Refactor this shit hole


class ConsumerGroup:
    def __init__(self, id: str, cluster: Cluster):
        self.id = id
        self.cluster = cluster
        self._pykafka_group_coordinator_instance: Optional[pykafka.Broker] = None
        self.topic_controller = TopicController(self.cluster)

    @property
    def _pykafka_group_coordinator(self) -> pykafka.Broker:
        consumer_id = self.id.encode("UTF-8")
        if not self._pykafka_group_coordinator_instance:
            self._pykafka_group_coordinator_instance: pykafka.Broker = cast(
                pykafka.Broker,
                self.cluster.pykafka_client.cluster.get_group_coordinator(consumer_id),
            )

        return self._pykafka_group_coordinator_instance

    def describe(self, *, verbose=False):
        consumer_id = self.id.encode("UTF-8")
        if consumer_id in self._pykafka_group_coordinator.list_groups().groups:
            resp = self._pykafka_group_coordinator.describe_groups([consumer_id])
            assert len(resp.groups) == 1

            meta = self._unpack_consumer_group_response(resp.groups[consumer_id])
            topic_assignment = self._get_member_assignment(meta["members"])

            consumer_offsets = self.get_consumer_offsets(
                self._pykafka_group_coordinator,
                consumer_id,
                topic_assignment,
                verbose=verbose,
            )

            return {
                "group_id": consumer_id,
                "group_coordinator": self._pykafka_group_coordinator.host,
                "offsets": consumer_offsets,
                "meta": meta,
            }
        raise ConsumerGroupDoesNotExistException()

    def get_consumer_offsets(
        self, group_coordinator, consumer_id, topic_assignment, verbose
    ):
        consumer_offsets = self._unpack_offset_response(
            group_coordinator.fetch_consumer_group_offsets(consumer_id, preqs=[])
        )

        if verbose:
            for topic in consumer_offsets.keys():
                topic_offsets = self.topic_controller.get_topic(topic).get_offsets()
                for partition_id, consumer_offset in consumer_offsets[topic].items():
                    consumer_offsets[topic][partition_id] = {
                        "consumer_offset": consumer_offset,
                        "topic_low_watermark": topic_offsets[partition_id][0],
                        "topic_high_watermark": topic_offsets[partition_id][1],
                        "consumer_lag": topic_offsets[partition_id][1]
                        - consumer_offset,
                    }
            return consumer_offsets
        for topic in consumer_offsets.keys():
            topic_offsets = self.topic_controller.get_topic(topic).get_offsets()
            new_consumer_offsets = {
                "consumer_offset": (float("inf"), float("-inf")),
                "topic_low_watermark": (float("inf"), float("-inf")),
                "topic_high_watermark": (float("inf"), float("-inf")),
                "consumer_lag": (float("inf"), float("-inf")),
            }
            for partition_id, consumer_offset in consumer_offsets[topic].items():
                current_offset = consumer_offset
                old_min, old_max = new_consumer_offsets["consumer_offset"]
                new_consumer_offsets["consumer_offset"] = (
                    min(old_min, current_offset),
                    max(old_max, current_offset),
                )

                old_min, old_max = new_consumer_offsets["topic_low_watermark"]
                new_consumer_offsets["topic_low_watermark"] = (
                    min(old_min, topic_offsets[partition_id][0]),
                    max(old_max, topic_offsets[partition_id][0]),
                )

                old_min, old_max = new_consumer_offsets["topic_high_watermark"]
                new_consumer_offsets["topic_high_watermark"] = (
                    min(old_min, topic_offsets[partition_id][1]),
                    max(old_max, topic_offsets[partition_id][1]),
                )

                old_min, old_max = new_consumer_offsets["consumer_lag"]
                new_consumer_offsets["consumer_lag"] = (
                    min(old_min, topic_offsets[partition_id][1] - current_offset),
                    max(old_max, topic_offsets[partition_id][1] - current_offset),
                )

            return new_consumer_offsets

    def _get_member_assignment(
        self, member_assignment: Dict[str, Any]
    ) -> List[PartitionOffsetFetchRequest]:
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
                partition_id: partition_data._asdict()["offset"]
                for partition_id, partition_data in partitions.items()
            }
            for topic_name, partitions in resp.topics.items()
        }

    def _unpack_consumer_group_response(
        self, resp: DescribeGroupResponse
    ) -> Dict[str, Any]:
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
                        "subscription": [
                            topic for topic in member.member_metadata.topic_names
                        ],
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


class ConsumerGroupController:
    def __init__(self, cluster: Cluster):
        self.cluster = cluster

    def get_consumergroup(self, consumer_id) -> ConsumerGroup:
        return ConsumerGroup(consumer_id, self.cluster)

    def list_consumer_groups(self) -> List[str]:
        brokers: Dict[
            int, pykafka.broker.Broker
        ] = self.cluster.pykafka_client.cluster.brokers
        return list(
            set(
                group.decode("UTF-8")
                for _, broker in brokers.items()
                for group in broker.list_groups().groups
            )
        )
