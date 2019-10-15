import pathlib
import shutil

import click

from esque.errors import NoConfirmationPossibleException


# private function which we can mock
def _isatty(stream) -> bool:
    return stream.isatty()


def isatty(stream) -> bool:
    return _isatty(stream)


def ensure_approval(question: str, *, no_verify: bool = False) -> bool:
    if no_verify:
        return True

    if not isatty(click.get_text_stream("stdin")):
        raise NoConfirmationPossibleException

    return click.confirm(question)


class HandleFileOnFinished:
    def __init__(self, dir_: pathlib.Path, keep_file: bool):
        self.keep_file = keep_file
        self._dir = dir_
        self._dir.mkdir(parents=True)

    def __enter__(self) -> pathlib.Path:
        return self._dir

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self.keep_file and self._dir.exists():
            shutil.rmtree(self._dir)
