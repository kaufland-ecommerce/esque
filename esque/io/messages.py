import dataclasses
import datetime
from typing import Any, List, NamedTuple, Optional

from esque.io.data_types import DataType


class MessageHeader(NamedTuple):
    key: str
    value: Optional[str]


class Data(NamedTuple):
    payload: Any
    data_type: DataType


@dataclasses.dataclass
class Message:
    key: Data
    value: Data
    partition: int
    offset: int
    timestamp: datetime.datetime
    headers: List[MessageHeader]


@dataclasses.dataclass
class BinaryMessage:
    key: Optional[bytes]
    value: Optional[bytes]
    partition: int
    offset: int
    timestamp: datetime.datetime
    headers: List[MessageHeader]
