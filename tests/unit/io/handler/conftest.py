from typing import List

import pytest

from esque.io.messages import BinaryMessage


@pytest.fixture()
def binary_messages() -> List[BinaryMessage]:
    return [
        BinaryMessage(key=b"foo1", value=b"bar1", partition=0, offset=0),
        BinaryMessage(key=b"foo2", value=b"bar2", partition=0, offset=1),
        BinaryMessage(key=b"foo3", value=b"bar3", partition=1, offset=2),
        BinaryMessage(key=b"foo4", value=b"bar4", partition=1, offset=3),
    ]
