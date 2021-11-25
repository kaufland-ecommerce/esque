import pathlib
import pickle
from dataclasses import dataclass
from typing import Any, BinaryIO, Dict, Iterable, List, Tuple, Union

from esque.io.exceptions import EsqueIOHandlerWriteException, EsqueIOSerializerConfigNotSupported
from esque.io.handlers import BaseHandler, HandlerConfig
from esque.io.messages import BinaryMessage
from esque.io.stream_decorators import skip_stream_events
from esque.io.stream_events import PermanentEndOfStream, StreamEvent


@dataclass(frozen=True)
class PathHandlerConfig(HandlerConfig):
    def _validate_fields(self) -> List[str]:
        problems = super()._validate_fields()
        if self.path_obj.exists() and not self.path_obj.is_dir():
            problems.append(f"Path {self.path!r} must be a directory.")
        return problems

    @property
    def path_obj(self) -> pathlib.Path:
        return pathlib.Path(self.path)


class PathHandler(BaseHandler[PathHandlerConfig]):
    config_cls = PathHandlerConfig

    def __init__(self, config: PathHandlerConfig):
        super().__init__(config)
        self._directory_exists = config.path_obj.exists()
        self._data_file = self.config.path_obj / "data.pkl"
        self._io_mode = None
        self._io_stream = None
        self._lbound = 0

    @property
    def _file_output(self) -> BinaryIO:
        if self._io_mode == "o":
            return self._io_stream
        elif self._io_mode == "i":
            raise EsqueIOHandlerWriteException("Handler is already in input Mode, cannot use it for writing!")

        if self._data_file.exists():
            raise EsqueIOHandlerWriteException(
                "Directory already contains a data file. "
                "Please delete the directory and its content first or use a different one to avoid mixing "
                "existing and new data."
            )

        self._create_directory_if_necessary()
        self._io_stream = self._data_file.open("ab")
        self._io_mode = "o"
        return self._io_stream

    @property
    def _file_input(self) -> BinaryIO:
        if self._io_mode == "i":
            return self._io_stream
        elif self._io_mode == "o":
            raise EsqueIOHandlerWriteException("Handler is already in output Mode, cannot use it for reading!")

        self._create_directory_if_necessary()
        self._io_stream = self._data_file.open("rb")
        self._io_mode = "i"
        return self._io_stream

    def get_serializer_configs(self) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        raise EsqueIOSerializerConfigNotSupported

    def put_serializer_configs(self, config: Tuple[Dict[str, Any], Dict[str, Any]]) -> None:
        raise EsqueIOSerializerConfigNotSupported

    def write_message(self, binary_message: Union[BinaryMessage, StreamEvent]) -> None:
        if isinstance(binary_message, StreamEvent):
            return

        pickle.dump(binary_message, self._file_output)
        self._file_output.flush()

    def write_many_messages(self, message_stream: Iterable[Union[BinaryMessage, StreamEvent]]) -> None:
        self._create_directory_if_necessary()
        for message in skip_stream_events(message_stream):
            pickle.dump(message, self._file_output)
        self._file_output.flush()

    def _create_directory_if_necessary(self):
        if self._directory_exists:
            return

        self.config.path_obj.mkdir(parents=True)
        self._directory_exists = True

    def read_message(self) -> Union[BinaryMessage, StreamEvent]:
        while True:
            msg = self._next_message()
            if isinstance(msg, StreamEvent) or msg.offset >= self._lbound:
                return msg

    def _next_message(self) -> Union[BinaryMessage, StreamEvent]:
        try:
            return pickle.load(self._file_input)
        except EOFError:
            return PermanentEndOfStream("Reached end of File.", PermanentEndOfStream.ALL_PARTITIONS)

    def seek(self, position: int):
        self._lbound = position

    def close(self) -> None:
        if self._io_stream is not None:
            self._io_stream.close()
            self._io_stream = None
            self._io_mode = None
