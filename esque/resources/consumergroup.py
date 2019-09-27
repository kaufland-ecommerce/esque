from collections import namedtuple
from typing import Dict, List, Optional, Tuple


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
    def offsets(self) -> List[int]:
        offsets = [o.current for t, po in self.topic_partition_offset.items() for p, o in po.items()]
        offsets = [0] if len(offsets) == 0 else offsets
        return offsets

    @property
    def member_names(self) -> List[str]:
        return [f"{m.id} : {m.client} @ {m.host} -> {', '.join(m.subscriptions)}" for m in self.members]

    @property
    def partition_amount(self) -> int:
        return sum([len(po.values()) for t, po in self.topic_partition_offset.items()])

    @property
    def offset_overview(self) -> Tuple[int, int, int]:
        """returns offset as min, avg, max over all partitions"""
        offsets = self.offsets
        return min(offsets), round(sum(offsets) / len(offsets), 1), max(offsets)

    @property
    def total_lag(self) -> int:
        lags = [offsets.lag for _, po in self.topic_partition_offset.items() for p, offsets in po.items()]
        return sum(lags)

    @property
    def relative_lag(self) -> float:
        offsets = self.offsets
        low_watermark = min(
            [offsets.low_watermark for _, po in self.topic_partition_offset.items() for p, offsets in po.items()]
        )
        high_watermark = max(
            [offsets.high_watermark for _, po in self.topic_partition_offset.items() for p, offsets in po.items()]
        )
        return ((high_watermark - max(offsets)) / (high_watermark - low_watermark)) * 100
