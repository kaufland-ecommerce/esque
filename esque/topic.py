from collections import namedtuple
from functools import total_ordering
from typing import Dict, List, Union, Optional

import yaml
from pykafka.protocol.offset import OffsetPartitionResponse

from esque.errors import raise_for_kafka_exception
from esque.resource import KafkaResource

TopicDict = Dict[str, Union[int, str, Dict[str, str]]]
PartitionInfo = Dict[int, OffsetPartitionResponse]

Watermark = namedtuple("Watermark", ["high", "low"])


class Partition(KafkaResource):
    def __init__(
        self,
        partition_id: int,
        low_watermark: int,
        high_watermark: int,
        partition_isrs,
        partition_leader,
        partition_replicas,
    ):
        self.partition_id = partition_id
        self.watermark = Watermark(high_watermark, low_watermark)
        self.partition_isrs = partition_isrs
        self.partition_leader = partition_leader
        self.partition_replicas = partition_replicas

    def as_dict(self):
        return {
            "partition_id": self.partition_id,
            "low_watermark": self.watermark.low,
            "high_watermark": self.watermark.high,
            "partition_isrs": self.partition_isrs,
            "partition_leader": self.partition_leader,
            "partition_replicas": self.partition_replicas,
        }


@total_ordering
class Topic(KafkaResource):
    def __init__(
        self,
        name: Union[str, bytes],
        num_partitions: int = None,
        replication_factor: int = None,
        config: Dict[str, str] = None,
    ):
        # Should we warn in those cases to force clients to migrate to string-only?
        if isinstance(name, bytes):
            name = name.decode("ascii")
        self.name = name

        # TODO remove those two, replace with the properties below
        self.num_partitions = num_partitions
        self.replication_factor = replication_factor
        self.config = config if config is not None else {}

        self._partitions: Optional[List[Partition]] = None
        self._pykafka_topic = None
        self._confluent_topic = None

        self.is_only_local = True

    # properties
    @property
    def partitions(self) -> List[Partition]:
        assert not self.is_only_local, "Need to update topic before updating partitions"
        return self._partitions

    @property
    def partition_amount(self) -> int:
        return len(self.partitions)

    @property
    def replication(self) -> int:
        reps = set(p.partition_replicas for p in self.partitions)
        if len(reps) != 1:
            raise ValueError(f"Topic partitions have different replication factors! {reps}")
        return reps.pop()

    @property
    def offsets(self) -> Dict[int, Watermark]:
        """
        Returns the low and high watermark for each partition in a topic
        """
        return {partition.partition_id: partition.watermark for partition in self.partitions}

    # conversions and factories
    @classmethod
    def from_dict(cls, dict_object: TopicDict) -> "Topic":
        return cls(
            dict_object.get("name"),
            dict_object.get("num_partitions"),
            dict_object.get("replication_factor"),
            dict_object.get("config"),
        )

    def as_dict(self, only_editable=False) -> TopicDict:
        if only_editable:
            return {"config": self.config}
        return {
            "num_partitions": self.num_partitions,
            "replication_factor": self.replication_factor,
            "config": self.config,
        }

    def to_yaml(self, only_editable=False) -> str:
        return yaml.dump(self.as_dict(only_editable=only_editable))

    def from_yaml(self, data) -> None:
        new_values = yaml.safe_load(data)
        for attr, value in new_values.items():
            setattr(self, attr, value)

    # update hook (TODO move to topic controller/factory?)
    @raise_for_kafka_exception
    def update_partitions(self, low_watermarks: PartitionInfo, high_watermarks: PartitionInfo):

        partitions = []
        for t in self._confluent_topic.values():
            for partition_id, partition_meta in t.partitions.items():
                partition = Partition(
                    partition_id,
                    int(low_watermarks[partition_id].offset[0]),
                    int(high_watermarks[partition_id].offset[0]),
                    partition_meta.isrs,
                    partition_meta.leader,
                    partition_meta.replicas,
                )
                partitions.append(partition)

        self._partitions = partitions

    # object behaviour
    def __lt__(self, other: "Topic"):
        return self.name < other.name

    def __eq__(self, other: "Topic"):
        return self.name == other.name

    def __hash__(self):
        return hash(self.name)
