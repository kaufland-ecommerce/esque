import dataclasses
from typing import Any

from esque.io.data_types import DataType


@dataclasses.dataclass
class Data:
    payload: Any
    data_type: DataType


@dataclasses.dataclass
class Message:
    key: Data
    value: Data
    partition: int
    offset: int


@dataclasses.dataclass
class BinaryMessage:
    key: bytes
    value: bytes
    partition: int
    offset: int
