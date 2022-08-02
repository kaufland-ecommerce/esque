import click

from esque.cli.autocomplete import list_brokers
from esque.cli.helpers import fallback_to_stdin
from esque.cli.options import State, default_options, output_format_option
from esque.cli.output import format_output
from esque.errors import ValidationException
from esque.resources.broker import Broker


@click.command("broker", short_help="Describe a broker.")
@click.argument("broker", metavar="BROKER", callback=fallback_to_stdin, shell_complete=list_brokers, required=False)
@output_format_option
@default_options
def describe_broker(state: State, broker: str, output_format: str):
    """Return configuration options for broker BROKER. BROKER can be given with broker id (integer),
    the host name (if hostname is unique), or socket address ('hostname:port')"""
    if broker.isdigit():
        broker = Broker.from_id(state.cluster, broker).describe()
    elif ":" not in broker:
        broker = Broker.from_host(state.cluster, broker).describe()
    else:
        try:
            host, port = broker.split(":")
            broker = Broker.from_host_and_port(state.cluster, host, int(port)).describe()
        except ValueError:
            raise ValidationException("BROKER must either be the broker id, the hostname, or in the form 'host:port'")

    click.echo(format_output(broker, output_format))
