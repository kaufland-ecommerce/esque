import dataclasses
import warnings
from typing import List, Type, TypeVar


class DataType:
    pass


class EsqueDataType(DataType):
    pass


class UnknownDataType(DataType):
    """
    Used when the data type is unknown or irrelevant
    """

    def __eq__(self, other):
        if not isinstance(other, DataType):
            return NotImplemented
        warnings.warn(
            f"Comparing UnknownDatatype to {type(other).__name__}. "
            "Unknown types don't have a proper value and therefore can never be considered equal to anything."
        )
        return False


CDT = TypeVar("CDT", bound="CustomDataType")

# TODO: create a method that looks up a data type in the internal cache based on some internal representation
#  (hash of the normalized string that represents the type or something similar) and returns a cached instance (if available)
#  or caches the one that was instantiated by the serializer (if it is not available in the cache).
#  Consider two objects with the same fields declared in a different order to be of DIFFERENT types.
#  Examples:
#  type=String()
#  cached_type=type.internalize()
#  assert cached_type is type
#  second_type=String()
#  second_cached_type=second_type.internalize()
#  assert second_cached_type is not second_type and second_cached_type == second_type
#  assert second_cached_type is cached_type


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
class NoData(EsqueDataType):
    pass


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
