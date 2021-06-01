import base64
import json
import sys
from enum import Enum
from typing import Any, Dict, NoReturn, Optional, TextIO

from esque.io.exceptions import (
    EsqueIOHandlerConfigException,
    EsqueIOHandlerReadException,
    EsqueIONoMessageLeft,
    EsqueIOSerializerSettingsNotSupported,
)
from esque.io.handlers.base import BaseHandler, HandlerSettings
from esque.io.messages import BinaryMessage

MARKER = "__esque_msg_marker__\n"


class ByteEncoding(Enum):
    BASE64 = "base64"
    UTF_8 = "utf-8"
    HEX = "hex"


class PipeHandlerSettings(HandlerSettings):
    key_encoding: ByteEncoding = ByteEncoding.UTF_8
    value_encoding: ByteEncoding = ByteEncoding.UTF_8


class PipeHandler(BaseHandler):
    settings_cls = PipeHandlerSettings

    def __init__(self, settings: PipeHandlerSettings):
        super().__init__(settings)
        self._stream = self._get_stream()

    def _get_stream(self) -> TextIO:
        # pipe://stdout
        if self.settings.host == "stdin":
            return sys.stdin
        elif self.settings.host == "stdout":
            return sys.stdout
        elif self.settings.host == "stderr":
            return sys.stderr
        else:
            raise EsqueIOHandlerConfigException(f"Unknown stream {self.settings.host}")

    def get_serializer_settings(self) -> NoReturn:
        raise EsqueIOSerializerSettingsNotSupported

    def put_serializer_settings(self, settings: Dict[str, Any]) -> NoReturn:
        raise EsqueIOSerializerSettingsNotSupported

    def write_message(self, binary_message: BinaryMessage) -> None:
        json.dump(
            {
                "key": embed(binary_message.key, self.settings.key_encoding),
                "value": embed(binary_message.value, self.settings.value_encoding),
                "partition": binary_message.partition,
                "offset": binary_message.offset,
                "keyenc": self.settings.key_encoding.value,
                "valueenc": self.settings.value_encoding.value,
            },
            self._stream,
        )
        self._stream.write(f"\n{MARKER}")

    def read_message(self) -> Optional[BinaryMessage]:
        lines = []
        while True:
            line = self._stream.readline()
            if not line:
                if lines:
                    raise EsqueIOHandlerReadException("Premature end of stream, last message incomplete")
                else:
                    raise EsqueIONoMessageLeft("End of pipe reached")
            if line == MARKER:
                break
            lines.append(line)
        deserialized_object: Dict[str, Any] = json.loads("".join(lines))
        key_encoding = ByteEncoding(deserialized_object.get("keyenc", ByteEncoding.UTF_8))
        value_encoding = ByteEncoding(deserialized_object.get("valueenc", ByteEncoding.UTF_8))
        return BinaryMessage(
            key=extract(deserialized_object.get("key"), key_encoding),
            value=extract(deserialized_object.get("value"), value_encoding),
            offset=deserialized_object.get("offset"),
            partition=deserialized_object.get("partition"),
        )


def embed(input_value: Optional[bytes], encoding: ByteEncoding) -> Optional[str]:
    if input_value is None:
        return None
    if encoding == ByteEncoding.UTF_8:
        return input_value.decode(encoding="UTF-8")
    elif encoding == ByteEncoding.BASE64:
        return base64.b64encode(input_value).decode(encoding="UTF-8")
    elif encoding == ByteEncoding.HEX:
        input_value.hex()


def extract(input_value: Optional[str], encoding: ByteEncoding) -> Optional[bytes]:
    if input_value is None:
        return None
    if encoding == ByteEncoding.UTF_8:
        return input_value.encode(encoding="UTF-8")
    elif encoding == ByteEncoding.BASE64:
        return base64.b64decode(input_value.encode(encoding="UTF-8"))
    elif encoding == ByteEncoding.HEX:
        return bytes.fromhex(input_value)
