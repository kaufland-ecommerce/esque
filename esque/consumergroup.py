from collections import namedtuple
from functools import reduce
from operator import add
from typing import Dict, List, Optional, TYPE_CHECKING, Tuple

import pykafka

from esque.errors import ConsumerGroupDoesNotExistException

if TYPE_CHECKING:
    from esque.cluster import Cluster

OffsetInfo = namedtuple("OffsetInfo", ["low_watermark", "high_watermark", "current", "lag"])
MemberInfo = namedtuple("MemberInfo", ["id", "client", "host", "subscriptions", "assignments"])

TopicPartitionOffset = Dict[str, Dict[int, OffsetInfo]]


class ConsumerGroup:
    def __init__(self, group_id: str):
        self.group_id = group_id
        self.state: Optional[str] = None
        self.members: Optional[List[MemberInfo]] = None
        self.topic_partition_offset: Optional[TopicPartitionOffset] = None


    @property
    def id_bytes(self) -> bytes:
        return self.group_id.encode("UTF-8")

    @property
    def topics(self) -> List[str]:
        return list(self.topic_partition_offset.keys())

    @property
    def member_names(self) -> List[str]:
        return [f"{m.id} / {m.client} @ {m.host}" for m in self.members]

    @property
    def partition_amount(self) -> int:
        return sum([len(po.values()) for t, po in self.topic_partition_offset.items()])


    @property
    def offset_overview(self) -> Tuple[int, int, int]:
        offsets = [o.current for t, po in self.topic_partition_offset.items() for p, o in po.items()]
        return min(offsets), sum(offsets) // len(offsets), max(offsets)

    @property
    def total_lag(self) -> int:
        lags = [offsets.lag for _, po in self.topic_partition_offset.items() for p, offsets in po.items()]
        return sum(lags)


class ConsumerGroupController:
    def __init__(self, cluster: "Cluster"):
        self.cluster = cluster

    def get_cluster_consumergroup(self, group_id: str) -> ConsumerGroup:
        if group_id not in self.list_consumer_groups():
            raise ConsumerGroupDoesNotExistException(f"No consumer group with id {group_id} found on the cluster")

        group = ConsumerGroup(group_id)
        self.update_from_cluster(group)
        return group

    def update_from_cluster(self, group: ConsumerGroup):
        coordinator = self.cluster.pykafka_client.cluster.get_group_coordinator(group.id_bytes)
        self._update_members(coordinator, group)
        self._update_offsets(coordinator, group)

    def _update_offsets(self, coordinator, group: ConsumerGroup):

        offsets = coordinator.fetch_consumer_group_offsets(group.id_bytes, preqs=[])
        tpo = {}
        for topic, partition_offsets in offsets.topics.items():
            topic_name = topic.decode("utf-8")
            topic_offsets = self.cluster.topic_controller.get_cluster_topic(topic).offsets
            part_offs = {}

            for partition, offset_data in partition_offsets.items():
                current = offset_data.offset
                low = topic_offsets[partition].low
                high = topic_offsets[partition].high
                part_offs[partition] = OffsetInfo(low, high, current, high - current)
            tpo[topic_name] = part_offs

        group.topic_partition_offset = tpo

    def _update_members(self, coordinator, group: ConsumerGroup):
        describer = coordinator.describe_groups([group.id_bytes])
        desc = describer.groups[group.id_bytes]

        group.state = desc.state

        members = []
        for _, m in desc.members.items():
            subs = [topic for topic in m.member_metadata.topic_names]
            assignments = {assign[0]: assign[1] for assign in m.member_assignment.partition_assignment}
            m_info = MemberInfo(m.member_id, m.client_id, m.client_host, subs, assignments)
            members.append(m_info)

        group.members = members

    def list_consumer_groups(self) -> List[str]:
        brokers: Dict[int, pykafka.broker.Broker] = self.cluster.pykafka_client.cluster.brokers
        return list(
            set(group.decode("UTF-8") for _, broker in brokers.items() for group in broker.list_groups().groups)
        )
