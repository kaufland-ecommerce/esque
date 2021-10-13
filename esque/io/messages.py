import dataclasses
import datetime
from typing import Any, ClassVar, List, NamedTuple, Optional

from esque.io.data_types import DataType, NoData


class MessageHeader(NamedTuple):
    key: str
    value: Optional[str]


@dataclasses.dataclass
class Data:
    payload: Any
    data_type: DataType

    NO_DATA: ClassVar["Data"]


Data.NO_DATA = Data(payload=None, data_type=NoData())


def now_utc() -> datetime.datetime:
    return datetime.datetime.now(tz=datetime.timezone.utc)


@dataclasses.dataclass
class Message:
    key: Data = Data.NO_DATA
    value: Data = Data.NO_DATA
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
