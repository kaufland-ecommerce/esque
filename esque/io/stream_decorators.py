from typing import Iterable, Iterator, TypeVar, Union

from esque.io.stream_events import EndOfStream, NthMessageRead, StreamEvent

T = TypeVar("T")


def skip_stream_events(iterable: Iterable[Union[T, StreamEvent]]) -> Iterable[T]:
    for elem in iterable:
        if isinstance(elem, StreamEvent):
            continue
        yield elem


def stop_at_temporary_end_of_stream(iterable: Iterable[Union[T, StreamEvent]]) -> Iterable[Union[T, StreamEvent]]:
    """
    Enables an iterator to be consumed until an end of stream is reached. Meant to be used with :meth:`BaseHandler.message_stream()`.
    Check the docstring for :meth:`BaseHandler.message_stream()` for a more thorough definition of a temporary end of stream.
    Since the permanent end of stream cannot be followed by a temporary end, this function also stops at the permanent end of stream.
    The :class:`EndOfStream` object will still be yielded as final element of the iterable.

    :param iterable: The iterable to be decorated
    :return: The iterable that stops _after_ the underlying Iterator yielded :class:`EndOfStream`
    """
    for elem in iterable:
        yield elem
        if isinstance(elem, EndOfStream):
            break


def stop_after_nth_message(iterable: Iterable[Union[T, StreamEvent]], n: int) -> Iterable[Union[T, StreamEvent]]:
    i = 0
    for elem in iterable:
        yield elem
        if not isinstance(elem, StreamEvent):
            i += 1
        if i == n:
            yield NthMessageRead(f"{n} messages have been read.")
            break


# def stop_at_message_timeout(iterable: Iterable[Union[T, StreamEvent]], message_timeout: int) -> Iterable[Union[T, StreamEvent]]:
#     iterator: Iterator[T] = iter(iterable)
#     while True:
#         try:
#             yield next(iterator)
#         except (StopIteration, EsqueIOEndOfSourceReached):
#             return
#
#
# def read_for_n_seconds(iterable: Iterable[Union[T, StreamEvent]], max_read_time: int) -> Iterable[Union[T, StreamEvent]]:
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
#     def __init__(self, iterable: Iterable[Union[T, StreamEvent]]):
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
