import pathlib
import shutil

import click


def ensure_approval(question: str, *, no_verify: bool = False) -> bool:
    if no_verify:
        return True
    return click.confirm(question)


class DeleteOnFinished:
    def __init__(self, dir_: pathlib.Path):
        self._dir = dir_
        self._dir.mkdir(parents=True)

    def __enter__(self) -> pathlib.Path:
        return self._dir

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._dir.exists():
            shutil.rmtree(self._dir)
