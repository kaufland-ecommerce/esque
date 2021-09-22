import datetime
from typing import Iterable, Tuple

from esque.cli.helpers import attrgetter
from esque.io.messages import BinaryMessage
from esque.io.stream_events import TemporaryEndOfPartition

# partition, input, expected_output
SortCase = Tuple[int, Iterable[BinaryMessage], Iterable[BinaryMessage]]


def mk_binary_message(partition: int, offset: int, ts: int) -> BinaryMessage:
    return BinaryMessage(
        key=f"k_p{partition}_o{offset}".encode("utf-8"),
        value=f"v_p{partition}_o{offset}".encode("utf-8"),
        partition=partition,
        offset=offset,
        timestamp=datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc),
        headers=[],
    )


def case_simple_sort() -> SortCase:
    input = [
        mk_binary_message(partition=0, offset=0, ts=2),
        mk_binary_message(partition=1, offset=0, ts=1),
        mk_binary_message(partition=2, offset=0, ts=4),
        mk_binary_message(partition=1, offset=1, ts=3),
        mk_binary_message(partition=1, offset=2, ts=5),
        mk_binary_message(partition=2, offset=1, ts=7),
        mk_binary_message(partition=0, offset=1, ts=6),
    ]
    expected_output = sorted(input, key=attrgetter("timestamp"))
    return 3, input, expected_output


def case_sort_with_events() -> SortCase:
    input = [
        mk_binary_message(partition=0, offset=0, ts=2),
        mk_binary_message(partition=1, offset=0, ts=1),
        mk_binary_message(partition=2, offset=0, ts=4),
        mk_binary_message(partition=1, offset=1, ts=3),
        mk_binary_message(partition=1, offset=2, ts=5),
        TemporaryEndOfPartition(partition=1, msg=""),
        mk_binary_message(partition=2, offset=1, ts=7),
        TemporaryEndOfPartition(partition=2, msg=""),
        mk_binary_message(partition=0, offset=1, ts=6),
        TemporaryEndOfPartition(partition=0, msg=""),
        TemporaryEndOfPartition(partition=TemporaryEndOfPartition.ALL_PARTITIONS, msg=""),
    ]
    expected_output = [
        mk_binary_message(partition=1, offset=0, ts=1),
        mk_binary_message(partition=0, offset=0, ts=2),
        mk_binary_message(partition=1, offset=1, ts=3),
        mk_binary_message(partition=2, offset=0, ts=4),
        mk_binary_message(partition=1, offset=2, ts=5),
        TemporaryEndOfPartition(partition=1, msg=""),
        mk_binary_message(partition=0, offset=1, ts=6),
        TemporaryEndOfPartition(partition=0, msg=""),
        mk_binary_message(partition=2, offset=1, ts=7),
        TemporaryEndOfPartition(partition=2, msg=""),
        TemporaryEndOfPartition(partition=TemporaryEndOfPartition.ALL_PARTITIONS, msg=""),
    ]
    return 3, input, expected_output
