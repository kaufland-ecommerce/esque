import sys
from shutil import copyfile

import click
from click import ClickException, make_pass_decorator, option
from pykafka.exceptions import NoBrokersAvailableError

from esque.cli.helpers import ensure_approval
from esque.cluster import Cluster
from esque.config import Config, config_dir, config_path, sample_config_path
from esque.errors import ConfigNotExistsException


class State(object):
    def __init__(self):
        self.no_verify = False

        try:
            self.config = Config()
        except ConfigNotExistsException:
            click.echo(f"No config provided in {config_dir()}")
            config_dir().mkdir(exist_ok=True)
            if ensure_approval(f"Should a sample file be created in {config_dir()}"):
                copyfile(sample_config_path().as_posix(), config_path())
            if ensure_approval("Do you want to modify the config file now?"):
                click.edit(filename=config_path().as_posix())
            sys.exit(0)
        self._cluster = None

    @property
    def cluster(self):
        try:
            if not self._cluster:
                self._cluster = Cluster()
            return self._cluster
        except NoBrokersAvailableError:
            raise ClickException(
                f"Could not reach Kafka Brokers {self.config.bootstrap_servers}"
            )


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
