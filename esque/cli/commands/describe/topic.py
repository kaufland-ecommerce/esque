import click

from esque.cli.autocomplete import list_topics
from esque.cli.helpers import fallback_to_stdin
from esque.cli.options import State, default_options, output_format_option
from esque.cli.output import format_output, red_bold
from esque.controller.consumergroup_controller import ConsumerGroupController


@click.command("topic")
@click.argument(
    "topic-name",
    metavar="TOPIC_NAME",
    callback=fallback_to_stdin,
    required=False,
    type=click.STRING,
    shell_complete=list_topics,
)
@click.option(
    "--consumers",
    "-c",
    required=False,
    is_flag=True,
    default=False,
    help="Will output the consumer groups reading from this topic."
    f" {red_bold('Beware! This can be a really expensive operation.')}",
)
@click.option(
    "--last-timestamp",
    required=False,
    is_flag=True,
    default=False,
    help="Will output the last message's timestamp for each partition"
    f" {red_bold('Beware! This can be a really expensive operation.')}",
)
@output_format_option
@default_options
def describe_topic(state: State, topic_name: str, consumers: bool, last_timestamp: bool, output_format: str):
    """Describe a topic.

    Returns information on a given topic and its partitions, with the option of including
    all consumer groups that read from the topic.
    """
    topic = state.cluster.topic_controller.get_cluster_topic(topic_name, retrieve_last_timestamp=last_timestamp)

    output_dict = {
        "topic": topic_name,
        "partitions": [partition.as_dict() for partition in topic.partitions],
        "config": topic.config,
    }

    if consumers:
        consumergroup_controller = ConsumerGroupController(state.cluster)
        groups = consumergroup_controller.list_consumer_groups()

        consumergroups = [
            group_name
            for group_name in groups
            if topic_name in consumergroup_controller.get_consumer_group(group_name).topics
        ]

        output_dict["consumergroups"] = consumergroups
    click.echo(format_output(output_dict, output_format))
