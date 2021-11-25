import click

from esque.cli.options import State, default_options

from .offsets import set_offsets


@click.group(name="set", help="Set resource attributes.")
@default_options
def set_(state: State):
    pass


set_.add_command(set_offsets)
