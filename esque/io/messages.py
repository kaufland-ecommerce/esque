import dataclasses
import datetime
from typing import Any, List, NamedTuple, Optional

from esque.io.data_types import DataType, NoData


class MessageHeader(NamedTuple):
    key: str
    value: Optional[str]


class Data(NamedTuple):
    payload: Any
    data_type: DataType


def now_utc() -> datetime.datetime:
    return datetime.datetime.now(tz=datetime.timezone.utc)


@dataclasses.dataclass
class Message:
    key: Data = Data(None, data_type=NoData())
    value: Data = Data(None, data_type=NoData())
    partition: int = -1
    offset: int = -1
    timestamp: datetime.datetime = dataclasses.field(default_factory=now_utc)
    headers: List[MessageHeader] = dataclasses.field(default_factory=list)


@dataclasses.dataclass
class BinaryMessage:
    key: Optional[bytes] = None
    value: Optional[bytes] = None
    partition: int = -1
    offset: int = -1
    timestamp: datetime.datetime = dataclasses.field(default_factory=now_utc)
    headers: List[MessageHeader] = dataclasses.field(default_factory=list)
