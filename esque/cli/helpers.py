import pathlib
import shutil
import sys
from typing import Callable, Dict, List, Optional, Tuple, Union

import click
import yaml
from click import MissingParameter
from yaml.scanner import ScannerError

from esque.errors import EditCanceled, NoConfirmationPossibleException, ValidationException


# private function, which we can mock
def _isatty(stream) -> bool:
    return stream.isatty()


def isatty(stream) -> bool:
    return _isatty(stream)


def get_piped_stdin_arguments() -> List[str]:
    arguments: List[str] = []
    if not isatty(sys.stdin):
        arguments += [argument.replace("\n", "") for argument in sys.stdin.readlines()]
    return arguments


def ensure_approval(question: str, *, no_verify: bool = False, default_answer=False) -> bool:
    if no_verify:
        return True

    if not isatty(click.get_text_stream("stdin")):
        raise NoConfirmationPossibleException()

    return click.confirm(question, default=default_answer)


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


def edit_yaml(yaml_str: str, validator: Optional[Callable[[Dict], None]] = None) -> Tuple[str, Dict]:
    while True:
        yaml_str: Optional[str] = click.edit(yaml_str, extension=".yaml")

        # edit process can be aborted, ex. in vim via :q!
        if yaml_str is None:
            raise EditCanceled()
        try:
            # TODO: check for duplicate keys, might have to change yaml parser for that
            config_data = yaml.safe_load(yaml_str)
            if validator:
                validator(config_data)
        except (ScannerError, ValidationException) as e:
            _handle_edit_exception(e)
        else:
            break
    return yaml_str, config_data


def _handle_edit_exception(e: Union[ScannerError, ValidationException]) -> None:
    if isinstance(e, ScannerError):
        click.echo("Error parsing yaml:")
    else:
        click.echo("Error validating yaml:")
    click.echo(str(e))
    if not ensure_approval("Continue Editing?", default_answer=True):
        raise EditCanceled()


def fallback_to_stdin(ctx, args, value):
    if value:
        return value

    stdin = click.get_text_stream("stdin")
    if not isatty(stdin):
        value = stdin.readline().strip()

    if not value:
        raise MissingParameter("No value specified!")

    return value
