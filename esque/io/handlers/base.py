from abc import ABC, abstractmethod
from typing import Any, Dict, Iterable, List, Optional, Tuple

from esque.io.exceptions import EsqueIOHandlerConfigException, EsqueIONoMessageLeft
from esque.io.messages import BinaryMessage


class BaseHandler(ABC):
    def __init__(self, settings: Dict[str, Any]):
        self._settings = settings.copy()
        self._validate_settings()

    def _validate_settings(self) -> None:
        problems = []
        for key, type_ in self._get_required_field_specs():
            if key not in self._settings:
                problems.append(f"Missing required setting: {key!r}.")
                continue
            value = self._settings[key]
            if not isinstance(value, type_):
                problems.append(f"Setting {key!r} should be {type_.__name__}, not {type(value).__name__}.")

        if len(problems) > 0:
            raise EsqueIOHandlerConfigException(
                f"Invalid handler config for {type(self).__name__}:\n" + "\n  ".join(problems)
            )

    def _get_required_field_specs(self) -> List[Tuple[str, type]]:
        return []

    @abstractmethod
    def get_serializer_settings(self) -> Dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def put_serializer_settings(self, settings: Dict[str, Any]) -> None:
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
