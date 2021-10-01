import click
from click import version_option

from esque import __version__
from esque.cli import environment
from esque.cli.commands.apply import apply
from esque.cli.commands.config import config
from esque.cli.commands.consume import consume
from esque.cli.commands.create import create
from esque.cli.commands.ctx import ctx
from esque.cli.commands.delete import delete
from esque.cli.commands.describe import describe
from esque.cli.commands.edit import edit
from esque.cli.commands.get import get
from esque.cli.commands.io import io
from esque.cli.commands.ping import ping
from esque.cli.commands.produce import produce
from esque.cli.commands.set_ import set_
from esque.cli.commands.transfer import transfer
from esque.cli.commands.urlencode import urlencode
from esque.cli.options import State, default_options


@click.group(invoke_without_command=True, no_args_is_help=True)
@version_option(__version__)
@default_options
def esque(state: State):
    """esque - an operational kafka tool.

    In the Kafka world nothing is easy, but esque (pronounced esk) is an attempt at it.
    """
    if environment.ESQUE_PROFILE:
        import atexit
        import cProfile
        import pstats

        print("Profiling...")
        pr = cProfile.Profile()
        pr.enable()

        def stop_profiling():
            pr.disable()
            pstats.Stats(pr).dump_stats("./esque.pstat")
            print("profiling completed, data dumped to esque.pstat")

        atexit.register(stop_profiling)


esque.add_command(apply)
esque.add_command(config)
esque.add_command(consume)
esque.add_command(create)
esque.add_command(ctx)
esque.add_command(delete)
esque.add_command(describe)
esque.add_command(edit)
esque.add_command(get)
esque.add_command(io)
esque.add_command(ping)
esque.add_command(produce)
esque.add_command(set_)
esque.add_command(transfer)
esque.add_command(urlencode)
