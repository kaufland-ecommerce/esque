import click

from esque.cli.autocomplete import list_topics
from esque.cli.options import State, default_options, output_format_option
from esque.cli.output import format_output


@click.command("topics")
@click.option("-p", "--prefix", type=click.STRING, shell_complete=list_topics)
@click.option(
    "--hide-internal",
    type=click.BOOL,
    help="Hide internal topics in the output",
    required=False,
    default=False,
    is_flag=True,
)
@output_format_option
@default_options
def get_topics(state: State, prefix: str, hide_internal: bool, output_format: str):
    """List all topics."""
    topics = state.cluster.topic_controller.list_topics(
        search_string=prefix, get_topic_objects=False, hide_internal=hide_internal
    )
    topic_names = [topic.name for topic in topics]
    click.echo(format_output(topic_names, output_format))
