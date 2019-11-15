import sys
from functools import wraps
from shutil import copyfile

import click
from click import make_pass_decorator, option

from esque.cli import environment
from esque.cli.helpers import ensure_approval
from esque.cluster import Cluster
from esque.config import Config, config_dir, config_path, sample_config_path
from esque.errors import ConfigNotExistsException


class State(object):
    def __init__(self):
        self.no_verify = False
        self._verbose = False
        self._cluster = None
        self._config = None

    @property
    def config(self) -> Config:
        if self._config is None:
            self._create_config()
        return self._config

    def _create_config(self):
        try:
            self._config = Config.get_instance()
        except ConfigNotExistsException:
            click.echo(f"No config provided in {config_dir()}")
            if ensure_approval(f"Should a sample file be created in {config_dir()}"):
                config_dir().mkdir(exist_ok=True)
                copyfile(sample_config_path().as_posix(), config_path())
            else:
                raise
            if ensure_approval("Do you want to modify the config file now?"):
                click.edit(filename=config_path().as_posix())
            self._config = Config.get_instance()

    @property
    def cluster(self):
        if not self._cluster:
            self._cluster = Cluster()
        return self._cluster

    def _get_verbose(self) -> bool:
        if environment.ESQUE_VERBOSE is not None:
            return True
        return self._verbose

    def _set_verbose(self, verbose):
        self._verbose = verbose

    verbose = property(_get_verbose, _set_verbose)


pass_state = make_pass_decorator(State, ensure=True)


def verbose_callback(context, _: str, verbose=False):
    state = context.ensure_object(State)
    state.verbose = verbose


verbose_option = click.option(
    "-v",
    "--verbose",
    is_flag=True,
    is_eager=True,
    callback=verbose_callback,
    expose_value=False,
    help="Return stack trace on error.",
)


def default_options(f):
    defaults = [no_verify_option, verbose_option, error_handler, pass_state]
    for decorator in defaults:
        f = decorator(f)
    return f


def no_verify_option(f):
    def callback(ctx, param, value):
        state = ctx.ensure_object(State)
        state.no_verify = value

    return option(
        "--no-verify",
        type=bool,
        help="Skip all verification dialogs and answer them with yes.",
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
    help="Format of the output.",
    required=False,
)


def error_handler(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        state = args[0]
        if not isinstance(state, State):
            raise TypeError(
                "First argument is not a state, make sure that the `error_handler` decorator comes below `pass_state`"
            )
        try:
            f(*args, **kwargs)
        except Exception as e:
            if state.verbose:
                raise
            _silence_exception(e)

    return wrapper


def _silence_exception(e: Exception):
    if hasattr(e, "format_message"):
        click.echo(e.format_message())
    elif isinstance(e, (KeyError, ValueError)):
        click.echo(f"{type(e).__name__}: {str(e)}")
    else:
        click.echo(f"Exception of type {type(e).__name__} occurred.")
    click.echo("Run with `--verbose` for complete error.")
    sys.exit(1)
