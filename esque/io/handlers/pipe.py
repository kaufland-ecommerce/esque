import base64
import datetime
import json
import sys
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, NoReturn, Optional, TextIO, Union

from esque.io.exceptions import (
    EsqueIOHandlerConfigException,
    EsqueIOHandlerReadException,
    EsqueIOSerializerConfigNotSupported,
)
from esque.io.handlers.base import BaseHandler, HandlerConfig
from esque.io.messages import BinaryMessage, MessageHeader
from esque.io.stream_events import PermanentEndOfStream, StreamEvent


class ByteEncoding(Enum):
    BASE64 = "base64"
    UTF_8 = "utf-8"
    HEX = "hex"


@dataclass(frozen=True)
class PipeHandlerConfig(HandlerConfig):
    key_encoding: Union[str, ByteEncoding] = ByteEncoding.UTF_8.value
    value_encoding: Union[str, ByteEncoding] = ByteEncoding.UTF_8.value
    pretty_print: str = ""

    def _validate_fields(self) -> List[str]:
        problems = super()._validate_fields()
        try:
            ByteEncoding(self.key_encoding)
        except ValueError:
            problems.append(
                f"Invalid value for key_encoding: {self.key_encoding!r}. Valid values are: {', '.join(ByteEncoding)}"
            )

        try:
            ByteEncoding(self.value_encoding)
        except ValueError:
            problems.append(
                f"Invalid value for value_encoding: {self.value_encoding!r}. Valid values are: {', '.join(ByteEncoding)}"
            )

        return problems


class PipeHandler(BaseHandler[PipeHandlerConfig]):
    config_cls = PipeHandlerConfig

    def __init__(self, config: PipeHandlerConfig):
        super().__init__(config)
        self._stream = self._get_stream()
        self._lbound = -1

    def _get_stream(self) -> TextIO:
        # pipe://stdout
        if self.config.host == "stdin":
            return sys.stdin
        elif self.config.host == "stdout":
            return sys.stdout
        elif self.config.host == "stderr":
            return sys.stderr
        else:
            raise EsqueIOHandlerConfigException(f"Unknown stream {self.config.host}")

    def get_serializer_configs(self) -> NoReturn:
        raise EsqueIOSerializerConfigNotSupported

    def put_serializer_configs(self, config: Dict[str, Any]) -> NoReturn:
        raise EsqueIOSerializerConfigNotSupported

    def write_message(self, binary_message: Union[BinaryMessage, StreamEvent]) -> None:
        if isinstance(binary_message, StreamEvent):
            return
        json.dump(
            {
                "key": embed(binary_message.key, self.config.key_encoding),
                "value": embed(binary_message.value, self.config.value_encoding),
                "partition": binary_message.partition,
                "offset": binary_message.offset,
                "timestamp": binary_message.timestamp.timestamp(),
                "headers": [{"key": h.key, "value": h.value} for h in binary_message.headers],
                "keyenc": str(self.config.key_encoding),
                "valueenc": str(self.config.value_encoding),
            },
            self._stream,
            indent=2 if self.config.pretty_print else None,
        )
        self._stream.write("\n")
        self._stream.flush()

    def read_message(self) -> Union[StreamEvent, BinaryMessage]:
        while True:
            msg = self._next_message()
            if isinstance(msg, StreamEvent) or msg.offset >= self._lbound:
                return msg

    def _next_message(self) -> Union[StreamEvent, BinaryMessage]:
        line = ""
        while not line.strip():
            line = self._stream.readline()
            if line == "":
                return PermanentEndOfStream("End of pipe reached")

        try:
            deserialized_object: Dict[str, Any] = json.loads(line)
        except ValueError as e:
            raise EsqueIOHandlerReadException(
                "Error parsing JSON object from input. "
                f"Make sure json objects are single-line and not pretty printed. Original Error: {e}"
            )

        key_encoding = deserialized_object.get("keyenc", self.config.key_encoding)
        value_encoding = deserialized_object.get("valueenc", self.config.value_encoding)
        return BinaryMessage(
            key=extract(deserialized_object.get("key"), key_encoding),
            value=extract(deserialized_object.get("value"), value_encoding),
            offset=deserialized_object.get("offset", -1),
            partition=deserialized_object.get("partition", -1),
            timestamp=datetime.datetime.fromtimestamp(
                deserialized_object.get("timestamp", 0), tz=datetime.timezone.utc
            ),
            headers=[MessageHeader(h["key"], h.get("value")) for h in deserialized_object.get("headers", [])],
        )

    def seek(self, position: int):
        self._lbound = position

    def close(self) -> None:
        pass  # stdin or stdout don't have to be closed


def embed(input_value: Optional[bytes], encoding: Union[str, ByteEncoding]) -> Any:
    encoding = ByteEncoding(encoding)

    if input_value is None:
        return None
    if encoding == ByteEncoding.UTF_8:
        return input_value.decode(encoding="UTF-8")
    elif encoding == ByteEncoding.BASE64:
        return base64.b64encode(input_value).decode(encoding="UTF-8")
    elif encoding == ByteEncoding.HEX:
        return input_value.hex()


def extract(input_value: Optional[str], encoding: Union[str, ByteEncoding]) -> Optional[bytes]:
    encoding = ByteEncoding(encoding)

    if input_value is None:
        return None
    if encoding == ByteEncoding.UTF_8:
        return input_value.encode(encoding="UTF-8")
    elif encoding == ByteEncoding.BASE64:
        return base64.b64decode(input_value.encode(encoding="UTF-8"))
    elif encoding == ByteEncoding.HEX:
        return bytes.fromhex(input_value)
