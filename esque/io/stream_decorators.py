from abc import abstractmethod
from threading import Event, Thread
from typing import Iterable, Iterator, TypeVar

from esque.io.exceptions import EsqueIOEndOfSourceReached, EsqueIOPermanentEndReached, EsqueIOTemporaryEndReached

T = TypeVar("T")


def stop_at_permanent_end_of_stream(iterable: Iterable[T]) -> Iterable[T]:
    """
    Enables an iterator to be consumed until the permanent end of stream is reached. Meant to be used with :meth:`BaseHandler.message_stream()`.
    Check the docstring for :meth:`BaseHandler.message_stream()` for a more thorough definition of the permanent end of stream.
    The decorator ignores any temporary end of stream. To respond to temporary ends, use the :func:`stop_at_stop_at_temporary_end_of_stream()` decorator instead.
    :param iterable: The iterable to be decorated
    :return: The iterable that stops when the underlying Iterator raises :class:`EsqueIOPermanentEndReached`
    """
    iterator: Iterator[T] = iter(iterable)
    while True:
        try:
            yield next(iterator)
        except (StopIteration, EsqueIOPermanentEndReached):
            return
        except EsqueIOTemporaryEndReached:
            pass


def stop_at_temporary_end_of_stream(iterable: Iterable[T]) -> Iterable[T]:
    """
    Enables an iterator to be consumed until an end of stream is reached. Meant to be used with :meth:`BaseHandler.message_stream()`.
    Check the docstring for :meth:`BaseHandler.message_stream()` for a more thorough definition of a temporary end of stream.
    Since the permanent end of stream cannot be followed by a temporary end, this function also stops at the permanent end of stream. If you need to
    respond only to the permanent end of stream, use :func:`stop_at_stop_at_permanent_end_of_stream()`
    :param iterable: The iterable to be decorated
    :return: The iterable that stops when the underlying Iterator raises :class:`EsqueIOEndOfSourceReached`
    """
    iterator: Iterator[T] = iter(iterable)
    while True:
        try:
            yield next(iterator)
        except (StopIteration, EsqueIOEndOfSourceReached):
            return


# def stop_at_message_timeout(iterable: Iterable[T], message_timeout: int) -> Iterable[T]:
#     iterator: Iterator[T] = iter(iterable)
#     while True:
#         try:
#             yield next(iterator)
#         except (StopIteration, EsqueIOEndOfSourceReached):
#             return
#
#
# def read_for_n_seconds(iterable: Iterable[T], max_read_time: int) -> Iterable[T]:
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
#     def __init__(self, iterable: Iterable[T]):
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
