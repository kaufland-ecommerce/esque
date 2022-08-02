import click

from esque.cli.autocomplete import list_topics
from esque.cli.options import State, default_options, output_format_option
from esque.cli.output import format_output


@click.command("watermarks", short_help="Return watermarks by topic.")
@click.option(
    "-t", "--topic-name", metavar="<topic_name>", required=False, type=click.STRING, shell_complete=list_topics
)
@output_format_option
@default_options
def get_watermarks(state: State, topic_name: str, output_format: str):
    """Returns the high and low watermarks for <topic_name>, or if not specified, all topics."""
    # TODO: Gathering of all watermarks takes super long
    topics = state.cluster.topic_controller.list_topics(search_string=topic_name)

    watermarks = {topic.name: max(v for v in topic.watermarks.values()) for topic in topics}

    click.echo(format_output(watermarks, output_format))
