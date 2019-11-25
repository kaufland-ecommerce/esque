from collections import namedtuple
from functools import total_ordering
from typing import Any, Dict, Generator, List, Optional, Tuple, Union

import pendulum
import yaml
from pykafka.protocol.offset import OffsetPartitionResponse

from esque.resources.resource import KafkaResource

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
        partition_replicas: List[int],  # list of brokers holding a replica
        latest_message_timestamp: float = None,
    ):
        self.partition_id = partition_id
        self.watermark = Watermark(high_watermark, low_watermark)
        self.partition_isrs = partition_isrs
        self.partition_leader = partition_leader
        self.partition_replicas = partition_replicas
        self.latest_message_timestamp = None
        if latest_message_timestamp is not None:
            self.latest_message_timestamp = pendulum.from_timestamp(latest_message_timestamp).to_datetime_string()

    def as_dict(self):
        return {
            "partition_id": self.partition_id,
            "low_watermark": self.watermark.low,
            "high_watermark": self.watermark.high,
            "partition_isrs": self.partition_isrs,
            "partition_leader": self.partition_leader,
            "partition_replicas": self.partition_replicas,
            "latest_message_timestamp": self.latest_message_timestamp,
        }


class AttributeDiff:
    def __init__(self, remote, local):
        self.remote = remote
        self.local = local
        assert type(remote) == type(
            local
        ), f"Attributes should be given as the same type, not {type(remote)} and {type(local)}"

    @property
    def old(self):
        return self.remote

    @property
    def new(self):
        return self.local

    def __eq__(self, other: "AttributeDiff"):
        return self.remote == other.remote and self.local == other.local

    def __hash__(self):
        return hash(hash(self.remote) + hash(self.local))

    def __repr__(self):
        return f"<AttributeDiff[remote:{self.remote}, local:{self.local}]>"


class TopicDiff:
    INVALID_CHANGES = ["num_partitions", "replication_factor"]

    def __init__(self):
        self._diffs: Dict[str, AttributeDiff] = {}

    def set_diff(self, name: str, remote, local) -> "TopicDiff":

        # config values of local topics can be set as int or string
        # since all "config" variables we get from cluster topics are sent to us as strings,
        # we need to convert the local ones to string to match them correctly (exclude None's)
        local = str(local) if isinstance(remote, str) and local is not None else local

        if remote == local:
            return self

        # TODO: this should be handled correctly by checking the cluster defaults, like
        # if remote == cluster_default(name) and local is None: return
        # currently, if an attribute that was set get's un-set, it's ignored
        if local is None:
            return self

        assert type(remote) == type(
            local
        ), f"Attributes for {name} should be given as the same type, not {type(remote)} and {type(local)}"
        self._diffs[name] = AttributeDiff(remote, local)

        # allow chaining of set-calls
        return self

    @property
    def is_valid(self) -> bool:
        return set(self._diffs.get(a, None) for a in self.INVALID_CHANGES) == {None}

    @classmethod
    def from_dict(cls, diff_dict: Dict[str, AttributeDiff]) -> "TopicDiff":
        td = TopicDiff()
        td._diffs = diff_dict
        return td

    @property
    def has_changes(self) -> bool:
        return len(self._diffs.keys()) > 0

    def changes(self) -> Generator[Tuple[str, Any, Any], None, None]:
        for key, val in self._diffs.items():
            yield key, val.remote, val.local

    def __eq__(self, other: "TopicDiff") -> bool:
        return self._diffs == other._diffs

    def __repr__(self):
        return f"<TopicDiff[{str(self._diffs)}>"


def copy_to_local(topic: "Topic") -> "Topic":
    return Topic(topic.name, topic.num_partitions, topic.replication_factor, topic.config)


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

        # those settings are only used until the topic is updated from cluster
        self.__num_partitions = num_partitions
        self.__replication_factor = replication_factor

        self.config = config if config is not None else {}

        self.partition_data: Optional[List[Partition]] = None
        self.is_only_local = True

    # properties
    @property
    def partitions(self) -> List[Partition]:
        assert not self.is_only_local, "Need to update topic before updating partitions"
        return self.partition_data

    @property
    def watermarks(self) -> Dict[int, Watermark]:
        """
        Returns the low and high watermark for each partition in a topic
        """
        return {partition.partition_id: partition.watermark for partition in self.partitions}

    @property
    def num_partitions(self) -> int:
        if self.is_only_local:
            return self.__num_partitions
        return len(self.partitions)

    @property
    def replication_factor(self) -> int:
        if self.is_only_local:
            return self.__replication_factor
        partition_replication_factors = set(len(p.partition_replicas) for p in self.partitions)
        assert len(partition_replication_factors) == 1, "Different replication factors for partitions!"
        return partition_replication_factors.pop()

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
        return yaml.dump(self.as_dict(only_editable=only_editable), default_flow_style=False)

    def update_from_dict(self, new_values: TopicDict) -> None:
        for attr, value in new_values.items():
            setattr(self, attr, value)

    # object behaviour
    def __lt__(self, other: "Topic"):
        return self.name < other.name

    def __eq__(self, other: "Topic"):
        return self.name == other.name

    def __hash__(self):
        return hash(self.name)
