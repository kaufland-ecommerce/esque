import dataclasses
from typing import Any


@dataclasses.dataclass
class Message:
    key: Any
    value: Any
    partition: int
    offset: int


@dataclasses.dataclass
class BinaryMessage:
    key: bytes
    value: bytes
    partition: int
    offset: int
