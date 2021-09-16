import click

from esque.cli.autocomplete import list_topics
from esque.cli.helpers import ensure_approval, fallback_to_stdin
from esque.cli.options import State, default_options
from esque.resources.topic import Topic


@click.command("topic")
@click.argument("topic-name", metavar="TOPIC_NAME", callback=fallback_to_stdin, required=False)
@click.option(
    "-l",
    "--like",
    metavar="<template_topic>",
    help="Topic to use as template.",
    autocompletion=list_topics,
    required=False,
)
@default_options
def create_topic(state: State, topic_name: str, like: str):
    """Create a topic.

    Create a topic called TOPIC_NAME with the option of providing a template topic, <template_topic>,
    from which all the configuration options will be copied.
    """
    if not ensure_approval("Are you sure?", no_verify=state.no_verify):
        click.echo(click.style("Aborted!", bg="red"))
        return

    topic_controller = state.cluster.topic_controller
    if like:
        template_config = topic_controller.get_cluster_topic(like)
        topic = Topic(
            topic_name, template_config.num_partitions, template_config.replication_factor, template_config.config
        )
    else:
        topic = Topic(topic_name)
    topic_controller.create_topics([topic])
    click.echo(click.style(f"Topic with name '{topic.name}' successfully created.", fg="green"))
