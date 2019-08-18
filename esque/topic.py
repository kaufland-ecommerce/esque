from typing import Dict, List, Tuple, Union

import yaml

from esque.errors import raise_for_kafka_exception
from esque.resource import KafkaResource


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

        self.num_partitions = num_partitions if num_partitions is not None else 1
        self.replication_factor = replication_factor if replication_factor is not None else 1
        self.config = config if config is not None else {}

        self.low_watermark = None
        self.high_watermark = None
        self.partitions = None
        self._pykafka_topic = None
        self._confluent_topic = None

        self.is_only_local = True

    def as_dict(self) -> Dict[str, Union[int, Dict[str, str]]]:
        return {
            "num_partitions": self.num_partitions,
            "replication_factor": self.replication_factor,
            "config": self.config,
        }

    def to_yaml(self) -> str:
        return yaml.dump(self.as_dict())

    def from_yaml(self, data) -> None:
        new_values = yaml.safe_load(data)
        for attr, value in new_values.items():
            setattr(self, attr, value)


    def get_offsets(self) -> Dict[int, Tuple[int, int]]:
        """
        Returns the low and high watermark for each partition in a topic
        """

        assert not self.is_only_local, "Need to update topic before describing offsets"

        partitions: List[int] = self.partitions
        low_watermark_offsets = self.low_watermark
        high_watermark_offsets = self.high_watermark

        return {
            partition_id: (
                int(low_watermark_offsets[partition_id][0][0]),
                int(high_watermark_offsets[partition_id][0][0]),
            )
            for partition_id in partitions
        }

    @raise_for_kafka_exception
    def describe(self):
        assert not self.is_only_local, "Need to update topic before describing"

        offsets = self.get_offsets()
        replicas = [
            {
                f"Partition {partition}": {
                    "low_watermark": offsets[int(partition)][0],
                    "high_watermark": offsets[int(partition)][1],
                    "partition_isrs": partition_meta.isrs,
                    "partition_leader": partition_meta.leader,
                    "partition_replicas": partition_meta.replicas,
                }
            }
            for t in self._confluent_topic.values()
            for partition, partition_meta in t.partitions.items()
        ]

        return replicas, {"Config": self.config}

    def __lt__(self, other):
        if self.name < other.name:
            return True
        return False
