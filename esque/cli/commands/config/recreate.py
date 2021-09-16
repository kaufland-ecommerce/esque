from shutil import copyfile

import click

from esque.cli.helpers import ensure_approval
from esque.cli.options import State, default_options
from esque.config import config_dir, config_path, sample_config_path


@click.command("recreate")
@default_options
def config_recreate(state: State):
    """(Re)create esque config.

    Overwrites the existing esque config file with the sample config. If no esque config file already exists,
    create one with the sample config."""
    config_dir().mkdir(exist_ok=True)
    if ensure_approval(f"Should the current config in {config_dir()} get replaced?", no_verify=state.no_verify):
        copyfile(sample_config_path().as_posix(), config_path())
