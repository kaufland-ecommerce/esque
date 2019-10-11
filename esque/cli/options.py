import sys
from functools import wraps
from shutil import copyfile

import click
from click import make_pass_decorator, option

from esque.cli.helpers import ensure_approval
from esque.cluster import Cluster
from esque.config import config_dir, config_path, sample_config_path, Config
from esque.errors import ConfigNotExistsException, ExceptionWithMessage


class State(object):
    def __init__(self):
        self.no_verify = False

        try:
            self.config = Config()
        except ConfigNotExistsException:
            click.echo(f"No config provided in {config_dir()}")
            if ensure_approval(f"Should a sample file be created in {config_dir()}"):
                config_dir().mkdir(exist_ok=True)
                copyfile(sample_config_path().as_posix(), config_path())
            else:
                raise
            if ensure_approval("Do you want to modify the config file now?"):
                click.edit(filename=config_path().as_posix())
            self.config = Config()
        self._cluster = None

    @property
    def cluster(self):
        if not self._cluster:
            self._cluster = Cluster()
        return self._cluster


pass_state = make_pass_decorator(State, ensure=True)


def no_verify_option(f):
    def callback(ctx, param, value):
        state = ctx.ensure_object(State)
        state.no_verify = value

    return option(
        "--no-verify",
        type=bool,
        help="Skips all verification dialogs and answers them with yes.",
        required=False,
        is_flag=True,
        expose_value=False,
        default=False,
        callback=callback,
    )(f)


output_format_option = click.option(
    "-o",
    "--output-format",
    type=click.Choice(["yaml", "json"], case_sensitive=False),
    help="Format of the output",
    required=False,
)


def error_handler(f):
    @click.option("-v", "--verbose", help="More detailed information.", default=False, is_flag=True)
    @wraps(f)
    def wrapper(*args, **kwargs):
        verbose = kwargs["verbose"]
        del kwargs["verbose"]
        try:
            f(*args, **kwargs)
        except Exception as e:
            if verbose:
                raise

            if isinstance(e, ExceptionWithMessage):
                click.echo(click.style(str(e), fg="red"))
            else:
                click.echo(
                    click.style(
                        f"An Exception of type {type(e).__name__} occurred. Use verbose mode with '--verbose' "
                        f"to see more information.",
                        fg="red",
                    )
                )
            sys.exit(1)

    return wrapper
