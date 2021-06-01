import json
import sys
from typing import Any, Dict, NoReturn, Optional, TextIO

from esque.io.exceptions import EsqueIOHandlerConfigException, EsqueIOSerializerSettingsNotSupported
from esque.io.handlers.base import BaseHandler, HandlerSettings
from esque.io.messages import BinaryMessage

MARKER = "__esque_msg_marker__"


class PipeHandlerSettings(HandlerSettings):
    pass


class PipeHandler(BaseHandler):
    settings_cls = PipeHandlerSettings

    def __init__(self, settings: PipeHandlerSettings):
        super().__init__(settings)
        self._stream = self._get_stream()

    def _get_stream(self) -> TextIO:
        #
        if self._settings.host == "stdin":
            return sys.stdin
        elif self._settings.host == "stdout":
            return sys.stdout
        elif self._settings.host == "stderr":
            return sys.stderr
        else:
            raise EsqueIOHandlerConfigException(f"Unknown stream {self._settings.host}")

    def get_serializer_settings(self) -> NoReturn:
        raise EsqueIOSerializerSettingsNotSupported

    def put_serializer_settings(self, settings: Dict[str, Any]) -> NoReturn:
        raise EsqueIOSerializerSettingsNotSupported

    def write_message(self, binary_message: BinaryMessage) -> None:
        json.dump(
            {
                "key": binary_message.key.decode(encoding="utf-8"),
                "value": binary_message.value.decode(encoding="utf-8"),
                "partition": binary_message.partition,
                "offset": binary_message.offset,
            },
            self._stream,
        )
        self._stream.write(f"{MARKER}\n")

    def read_message(self) -> Optional[BinaryMessage]:
        lines = []
        while True:
            line = self._stream.readline()
            if line == MARKER:
                break
            lines.append(line)
        return json.loads("".join(lines))
