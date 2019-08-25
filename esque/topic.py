from typing import Dict, List, Tuple, Union, Any, Optional

import yaml
from pykafka.protocol.offset import OffsetPartitionResponse

from esque.errors import raise_for_kafka_exception
from esque.resource import KafkaResource

TopicDict = Dict[str, Union[int, str, Dict[str, str]]]
PartitionInfo = Dict[int, OffsetPartitionResponse]


class Partition(KafkaResource):
    def as_dict(self):
        return {
            "partition_id": self.partition_id,
            "low_watermark": self.low_watermark,
            "high_watermark": self.high_watermark,
            "partition_isrs": self.partition_isrs,
            "partition_leader": self.partition_leader,
            "partition_replicas": self.partition_replicas

        }

    def __init__(
            self,
            partition_id: int,
            low_watermark: int,
            high_watermark: int,
            partition_isrs,
            partition_leader,
            partition_replicas
    ):
        self.partition_id = partition_id
        self.low_watermark = low_watermark
        self.high_watermark = high_watermark
        self.partition_isrs = partition_isrs
        self.partition_leader = partition_leader
        self.partition_replicas = partition_replicas


class Topic(KafkaResource):
    def __init__(
            self,
            name: Union[str,
                        bytes],
            num_partitions: int = None,
            replication_factor: int = None,
            config: Dict[str, str] = None,
    ):
        # Should we warn in those cases to force clients to migrate to string-only?
        if isinstance(name, bytes):
            name = name.decode("ascii")
        self.name = name

        self.num_partitions = num_partitions
        self.replication_factor = replication_factor
        self.config = config if config is not None else {}

        self.__partitions: Optional[List[Dict[str, Any]]] = None
        self._pykafka_topic = None
        self._confluent_topic = None

        self.is_only_local = True

    @property
    def partitions(self):
        assert not self.is_only_local, "Need to update topic before updating partitions"
        return self.__partitions

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

    def get_offsets(self) -> Dict[int, Tuple[int, int]]:
        """
        Returns the low and high watermark for each partition in a topic
        """

        assert not self.is_only_local, "Need to update topic before describing offsets"

        return {
            partition["partition_id"]: (
                partition["low_watermark"],
                partition["high_watermark"]
            )
            for partition in self.partitions
        }

    @raise_for_kafka_exception
    def update_partitions(self, low_watermark: PartitionInfo, high_watermark: PartitionInfo):

        partitions = []
        for t in self._confluent_topic.values():
            for partition_id, partition_meta in t.partitions.items():
                partition = {
                    "partition_id": partition_id,
                    "low_watermark": int(low_watermark[partition_id].offset[0]),
                    "high_watermark": int(high_watermark[partition_id].offset[0]),
                    "partition_isrs": partition_meta.isrs,
                    "partition_leader": partition_meta.leader,
                    "partition_replicas": partition_meta.replicas,

                }
                partitions.append(partition)

        self.__partitions = partitions

    def __lt__(self, other):
        if self.name < other.name:
            return True
        return False
