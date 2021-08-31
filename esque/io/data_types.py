import dataclasses
from typing import List, Type, TypeVar


class DataType:
    pass


class EsqueDataType(DataType):
    pass


CDT = TypeVar("CDT", bound="CustomDataType")


class CustomDataType(DataType):
    @classmethod
    def from_esque_data_type(cls: Type[CDT], data_type: EsqueDataType) -> CDT:
        raise NotImplementedError

    def to_esque_data_type(self) -> EsqueDataType:
        raise NotImplementedError


@dataclasses.dataclass
class Field:
    name: str
    data_type: EsqueDataType


@dataclasses.dataclass
class Struct(EsqueDataType):
    schema: List[Field]


@dataclasses.dataclass
class Array(EsqueDataType):
    element_type: "EsqueDataType"


class PrimitiveField(EsqueDataType):
    pass


class Integer(PrimitiveField):
    pass


class Float(PrimitiveField):
    pass


class String(PrimitiveField):
    pass


class Bytes(PrimitiveField):
    pass


class Date(PrimitiveField):
    pass


class Time(PrimitiveField):
    pass


class DateTime(PrimitiveField):
    pass


class Timestamp(PrimitiveField):
    pass
