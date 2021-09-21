import dataclasses
from abc import ABC, abstractmethod
from typing import Any, ClassVar, Dict, Generic, Iterable, List, Tuple, Type, TypeVar, Union

from esque.io.exceptions import EsqueIOHandlerConfigException
from esque.io.messages import BinaryMessage
from esque.io.stream_events import PermanentEndOfStream, StreamEvent

H = TypeVar("H", bound="BaseHandler")
HC = TypeVar("HC", bound="HandlerConfig")


@dataclasses.dataclass(frozen=True)
class HandlerConfig:
    host: str
    path: str
    scheme: str

    def __post_init__(self):
        self._validate()

    def _validate(self):
        problems: List[str] = self._validate_fields()
        if problems:
            raise EsqueIOHandlerConfigException("Handler config validation failed: \n" + "\n".join(problems))

    def _validate_fields(self) -> List[str]:
        problems = []
        if self.host is None:
            problems.append("host cannot be None")
        if self.path is None:
            problems.append("path cannot be None")
        if self.scheme is None:
            problems.append("scheme cannot be None")
        return problems


class BaseHandler(ABC, Generic[HC]):

    config_cls: ClassVar[Type[HC]] = HandlerConfig
    config: HC

    def __init__(self, config: HC):
        """
        Base class for all Esque IO handlers. A handler is responsible for writing and reading messages
        to and from a source. The handler is unaware of the underlying message's format and treats all
        sources as binary. It may support persisting the serializer config for easier data retrieval.

        :param config:
        """
        self.config = config
        self._assert_correct_config_type()

    def _assert_correct_config_type(self):
        if not isinstance(self.config, self.config_cls):
            raise EsqueIOHandlerConfigException(
                f"Invalid type for the handler config. "
                f"Expected: {self.config_cls.__name__}, "
                f"provided: {type(self.config).__name__}"
            )

    @abstractmethod
    def seek(self, position: int):
        """
        Seek the handler's reading position to the given offset for all partitions. The next message that is read will
        be at this offset if it is still available and after this offset if and only if it is not available anymore.

        :param position: The offset to seek to
        """
        raise NotImplementedError

    @abstractmethod
    def get_serializer_configs(self) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """
        Retrieves the serializer config from this handler's source, if possible.
        Implementations should raise an :class:`esque.io.exceptions.EsqueIOSerializerConfigNotSupported`
        if this operation is not supported for a particular
        handler.

        :return: Tuple of dictionaries containing the configs for the key and value serializer
        """
        raise NotImplementedError

    @abstractmethod
    def put_serializer_configs(self, config: Tuple[Dict[str, Any], Dict[str, Any]]) -> None:
        """
        Persists the serializer config in this handler's source, if possible.
        Implementations should raise an :class:`esque.io.exceptions.EsqueIOSerializerConfigNotSupported`
        if this operation is not supported for a particular
        handler.

        :param config: Tuple of dictionaries containing the configs for the key and value serializer
        """
        raise NotImplementedError

    @abstractmethod
    def write_message(self, binary_message: Union[BinaryMessage, StreamEvent]) -> None:
        """
        Write the message from `binary_message` to this handler's source.
        The handler may choose which action to take upon receiving any :class:`StreamEvent`
        instances but mostly the appropriate action is to just ignore them.

        :param binary_message: The message that is supposed to be written.
        """
        raise NotImplementedError

    def write_many_messages(self, message_stream: Iterable[Union[BinaryMessage, StreamEvent]]) -> None:
        """
        Write all messages from the iterable `message_stream` to this handler's source.
        The handler may choose which action to take upon receiving any :class:`StreamEvent`
        instances but mostly the appropriate action is to just ignore them.

        :param message_stream: The messages that are supposed to be written.
        """
        for binary_message in message_stream:
            self.write_message(binary_message)

    @abstractmethod
    def read_message(self) -> Union[BinaryMessage, StreamEvent]:
        """
        Read the next :class:`BinaryMessage` from this handler's source.
        Returns an object of :class:`StreamEvent` to indicate certain events that may happen while reading from the
        source.
        For example if the handler has reached a permanent end, like the end of a file or a closed stream, then
        it will return a :class:`PermanentEndOfStream` object.
        If the handler has reached a temporary end (e.g. the end of a topic was reached but new messages might come in
        at some point) then it will return an object of :class:`TemporaryEndOfStream`.
        Both of these classes are subclasses of :class:`EndOfStream`.

        :return: The next message from this handler's source, or a stream event.
        :raises EsqueIOHandlerReadException: When there was a failure accessing the source. Like a broken pipe.
        """
        raise NotImplementedError

    def binary_message_stream(self) -> Iterable[Union[BinaryMessage, StreamEvent]]:
        """
        Read :class:`BinaryMessage`s from this handler's source until the source's permanent end is reached.
        Yields an object of :class:`StreamEvent` to indicate certain events that may happen while reading from the
        source.
        For example if the handler has reached a permanent end, like the end of a file or a closed stream, then
        it will return a :class:`PermanentEndOfStream` object.
        If the handler has reached a temporary end (e.g. the end of a topic was reached but new messages might come in
        at some point) then it will return an object of :class:`TemporaryEndOfStream`.
        Both of these classes are subclasses of :class:`EndOfStream`.

        The last object returned before the iterable ends is always an instance of :class:`PermanentEndOfStream`.

        :raises EsqueIOHandlerReadException: When there was a failure accessing the source. Like a broken pipe.
        :returns: Iterable yielding all messages from this handler's source until a permanent end was reached.
        """
        while True:
            msg = self.read_message()
            yield msg
            if isinstance(msg, PermanentEndOfStream):
                break

    @abstractmethod
    def close(self) -> None:
        """
        Close all resources that have been opened by this handler.
        """
        raise NotImplementedError
