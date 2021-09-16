import click

from esque.cli.options import State, default_options, output_format_option
from esque.cli.output import format_output
from esque.resources.broker import Broker


@click.command("brokers")
@output_format_option
@default_options
def get_brokers(state: State, output_format: str):
    """List all brokers.

    Return the broker id's and socket addresses of all the brokers in the kafka cluster defined in the current context.
    """
    brokers = Broker.get_all(state.cluster)
    broker_ids_and_hosts = [f"{broker.broker_id}: {broker.host}:{broker.port}" for broker in brokers]
    click.echo(format_output(broker_ids_and_hosts, output_format))
