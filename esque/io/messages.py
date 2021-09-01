import dataclasses
from typing import Any, Optional

from esque.io.data_types import DataType


@dataclasses.dataclass
class Data:
    payload: Any
    data_type: DataType


# TODO introduce headers
@dataclasses.dataclass
class Message:
    key: Data
    value: Data
    partition: int
    offset: int


# TODO introduce headers
@dataclasses.dataclass
class BinaryMessage:
    key: Optional[bytes]
    value: Optional[bytes]
    partition: int
    offset: int
