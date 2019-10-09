import pathlib
import shutil
import sys

import click


def ensure_approval(question: str, *, no_verify: bool = False) -> bool:
    if no_verify:
        return True

    if not sys.__stdin__.isatty():
        click.echo(
            "You are running this command in a non-interactive mode. To do this you must use the --no-verify option."
        )
        sys.exit(1)

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
