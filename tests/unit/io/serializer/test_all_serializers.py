from typing import List

from pytest_cases import parametrize_with_cases

from esque.io.messages import Data
from esque.io.serializers import DataSerializer


@parametrize_with_cases("serializer")
def test_serialize_many_no_data(serializer: DataSerializer, no_data: Data):
    actual_serialized_data: List = list(serializer.serialize_many([no_data]))
    assert actual_serialized_data == [None]

    actual_deserialized_data: List[Data] = list(serializer.deserialize_many([None]))
    assert actual_deserialized_data == [no_data]


@parametrize_with_cases("serializer")
def test_serialize_no_data(serializer: DataSerializer, no_data: Data):
    actual_serialized_data: None = serializer.serialize(no_data)
    assert actual_serialized_data is None

    actual_deserialized_data: Data = serializer.deserialize(None)
    assert actual_deserialized_data == no_data
