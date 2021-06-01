import dataclasses
from abc import ABC, abstractmethod
from typing import Any, ClassVar, Dict, Iterable, Optional, Type, TypeVar

from esque.io.exceptions import EsqueIOHandlerConfigException, EsqueIONoMessageLeft
from esque.io.messages import BinaryMessage

H = TypeVar("H", bound="BaseHandler")
HS = TypeVar("HS", bound="HandlerSettings")


@dataclasses.dataclass(frozen=True)
class HandlerSettings:
    host: str
    path: str

    def copy(self: HS) -> HS:
        return dataclasses.replace(self)

    def validate(self):
        problems = []
        if self.host is None:
            problems.append("host cannot be None")
        if self.path is None:
            problems.append("path cannot be None")

        if problems:
            raise EsqueIOHandlerConfigException(
                "One or more mandatory settings don't have a value: \n" + "\n".join(problems)
            )


class BaseHandler(ABC):

    settings_cls: ClassVar[Type[HS]] = HandlerSettings
    settings: HS

    def __init__(self, settings: HS):
        """
        Base class for all Esque IO handlers. A handler is responsible for writing and reading messages
        to and from a source. The handler is unaware of the underlying message's format and treats all
        sources as binary. It may support persisting the serializer settings for easier data retrieval.

        :param settings:
        """
        self.settings = settings.copy()
        self._validate_settings()

    def _validate_settings(self) -> None:
        """
        Check if the provided information is sufficient for the operation of the handler.
        The default version checks if any of the required settings (:meth:`_get_required_field_specs`)
        are missing and if the field types match.
        """
        if not isinstance(self.settings, self.settings_cls):
            raise EsqueIOHandlerConfigException(
                f"Invalid type for the handler settings. "
                f"Expected: {self.settings_cls.__name__}, "
                f"provided: {type(self.settings).__name__}"
            )
        self.settings.validate()

    @abstractmethod
    def get_serializer_settings(self) -> Dict[str, Any]:
        """
        Retrieves the serializer settings from this handler's source, if possible.
        Implementations should raise an :class:`esque.io.exceptions.EsqueIOSerializerSettingsNotSupported`
        if this operation is not supported for a particular
        handler.

        :return: Dictionary of settings.
        """
        raise NotImplementedError

    @abstractmethod
    def put_serializer_settings(self, settings: Dict[str, Any]) -> None:
        """
        Persists the serializer settings in this handler's source, if possible.
        Implementations should raise an :class:`esque.io.exceptions.EsqueIOSerializerSettingsNotSupported`
        if this operation is not supported for a particular
        handler.

        :param settings: Dictionary of serializer settings.
        """
        raise NotImplementedError

    @abstractmethod
    def write_message(self, binary_message: BinaryMessage) -> None:
        """
        Write the message from `binary_message` to this handler's source.

        :param binary_message: The message that is supposed to be written.
        """
        raise NotImplementedError

    def write_many_messages(self, binary_messages: Iterable[BinaryMessage]) -> None:
        """
        Write all messages from the iterable `binary_messages` to this handler's source.

        :param binary_messages: The messages that are supposed to be written.
        """
        for binary_message in binary_messages:
            self.write_message(binary_message)

    @abstractmethod
    def read_message(self) -> Optional[BinaryMessage]:
        """
        Read the next message from this handler's source. If there is no next message raise an exception.

        :return: The next message from this handler's source.
        :raises EsqueIONoMessageLeft: When there is no message left.
        """
        raise NotImplementedError

    def read_many_messages(self) -> Iterable[BinaryMessage]:
        """
        Read messages from this handler's source until there are no more left.

        :returns: Iterable yielding all messages that are left on this handler's source.
        """
        while True:
            try:
                yield self.read_message()
            except EsqueIONoMessageLeft:
                break
