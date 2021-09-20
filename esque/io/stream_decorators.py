from typing import Callable, Iterable, TypeVar, Union

from esque.io.messages import BinaryMessage, Message
from esque.io.stream_events import EndOfStream, NthMessageRead, StreamEvent

M = TypeVar("M", bound=Union[Message, BinaryMessage])
MessageStream = Iterable[Union[M, StreamEvent]]


def skip_stream_events(iterable: MessageStream) -> Iterable[M]:
    for elem in iterable:
        if isinstance(elem, StreamEvent):
            continue
        yield elem


def stop_at_temporary_end_of_stream(iterable: MessageStream) -> MessageStream:
    """
    Enables an iterator to be consumed until an end of stream is reached. Meant to be used with :meth:`BaseHandler.message_stream()`.
    Check the docstring for :meth:`BaseHandler.message_stream()` for a more thorough definition of a temporary end of stream.
    Since the temporary end cannot follow a permanent end of stream , this function also stops at the permanent end of stream.
    The :class:`EndOfStream` object will still be yielded as final element of the iterable.

    :param iterable: The iterable to be decorated
    :return: The iterable that stops _after_ the underlying Iterator yielded :class:`EndOfStream`
    """
    for elem in iterable:
        yield elem
        if isinstance(elem, EndOfStream):
            break


def stop_at_temporary_end_of_all_stream_partitions(iterable: MessageStream) -> MessageStream:
    """
    Enables an iterator to be consumed until the end of all the stream's partitions is reached.
    Meant to be used with :meth:`BaseHandler.message_stream()`.
    Check the docstring for :meth:`BaseHandler.message_stream()` for a more thorough definition of a temporary end of stream.
    Since the temporary end cannot follow a permanent end of stream , this function also stops at the permanent end of stream.
    The :class:`EndOfStream` object will still be yielded as final element of the iterable.

    :param iterable: The iterable to be decorated
    :return: The iterable that stops _after_ the underlying Iterator yielded :class:`EndOfStream`
    """
    for elem in iterable:
        yield elem
        if isinstance(elem, EndOfStream) and elem.partition_id == EndOfStream.ALL_PARTITIONS:
            break


def stop_after_nth_message(n: int) -> Callable[[MessageStream], MessageStream]:
    """
    Creates a decorator that enables an iterator to be consumed until n messages have been read.
    Meant to be used with :meth:`BaseHandler.message_stream()`.
    The decorator ignores any :class:StreamEvent objects that it encounters and only counts proper messages.
    :param n: The number of messages to consume before stopping
    :return: The iterable decorator which stops after the nth consumed message
    """

    def _stop_after_nth_message(iterable: MessageStream):
        i = 0
        for elem in iterable:
            yield elem
            if not isinstance(elem, StreamEvent):
                i += 1
            if i == n:
                yield NthMessageRead(f"{n} messages have been read.")
                break

    return _stop_after_nth_message


def skip_messages_with_offset_below(lbound: int) -> Callable[[MessageStream], MessageStream]:
    """
    Creates a decorator that enables an iterator to jump over messages until their offset is greater or equal to
    `lbound`.
    Meant to be used with :meth:`BaseHandler.message_stream()`.
    The decorator won't skip any :class:StreamEvent objects that it encounters.
    :param lbound: The offset boundary below which messages should be skipped.
    :return: The iterable decorator which skips over messages with offset below `lbound`
    """

    def _skip_messages_with_offset_below(iterable: MessageStream):
        for elem in iterable:
            if isinstance(elem, StreamEvent) or elem.offset >= lbound:
                yield elem

    return _skip_messages_with_offset_below


# def stop_at_message_timeout(iterable: EventStream, message_timeout: int) -> EventStream:
#     iterator: Iterator[T] = iter(iterable)
#     while True:
#         try:
#             yield next(iterator)
#         except (StopIteration, EsqueIOEndOfSourceReached):
#             return
#
#
# def read_for_n_seconds(iterable: EventStream, max_read_time: int) -> EventStream:
#     iterator: Iterator[T] = iter(iterable)
#
#     while True:
#         try:
#             yield next(iterator)
#         except (StopIteration, EsqueIOEndOfSourceReached):
#             return
#
#
# class MessageReaderThread(Thread):
#     _last_elem: T
#
#     def __init__(self, iterable: EventStream):
#         super(MessageReaderThread, self).__init__(daemon=True)
#         self._iterable = iterable
#         self._elem_received: Event = Event()
#         self._elem_read: Event = Event()
#
#     def run(self) -> None:
#         """
#         This thread should enable the caller to read messages for (at most) the specified amount of time.
#         The current issues with the class are:
#         - the reading starts as soon as the start() method is called, which might not be the desired outcome
#         - using the threads in combination with the other decorator functions in this module may lead to undesired effects, because of exceptions that may occur and
#         which are not handled by this class
#         :return:
#         """
#         for elem in self._iterable:
#             self._last_elem = elem
#             self._elem_received.set()
#             self._elem_read.wait()
#             self._elem_read.clear()
#
#     def get_next_element(self, timeout: int) -> T:
#         self._elem_received.wait(timeout=timeout)
#         self._elem_received.clear()
#         elem = self._last_elem
#         self._elem_read.set()
#         return elem
