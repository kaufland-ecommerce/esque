from typing import Optional

import click

from esque.cli.autocomplete import list_topics
from esque.cli.helpers import ensure_approval, fallback_to_stdin
from esque.cli.options import State, default_options
from esque.cli.output import blue_bold
from esque.errors import ValidationException
from esque.resources.topic import Topic


@click.command("topic")
@click.argument("topic-name", metavar="TOPIC_NAME", callback=fallback_to_stdin, required=False)
@click.option("-l", "--like", metavar="<template_topic>", help="Topic to use as template.", autocompletion=list_topics)
@click.option(
    "-p",
    "--partitions",
    metavar="<partitions>",
    help="Amount of partitions the new topic should get. If not given, tries to find value by checking the "
    "following places in the given order: 1. esque config, 2. cluster, 3. fixed default of min(3, broker-count).",
    default=None,
    type=int,
)
@click.option(
    "-r",
    "--replication-factor",
    metavar="<replication-factor>",
    help="Amount of replicas topic's partitions should get. If not given, tries to find value by checking the "
    "following places in the given order: 1. esque config, 2. cluster, 3. fixed default of 1.",
    default=None,
    type=int,
)
@default_options
def create_topic(
    state: State, topic_name: str, like: str, partitions: Optional[int], replication_factor: Optional[int]
):
    """Create a topic.

    Create a topic called TOPIC_NAME with the option of providing a template topic, <template_topic>,
    from which all the configuration options will be copied.
    If both <template_topic> and any of the <partitions> or <replication-factor> options are given, then <partitions>
    or <replication-factor> takes precedence over corresponding attributes of <template_topic>.
    """
    topic_controller = state.cluster.topic_controller
    if topic_controller.topic_exists(topic_name):
        raise ValidationException(f"Topic {topic_name!r} already exists.")

    if like:
        template_config = topic_controller.get_cluster_topic(like)
        partitions = template_config.num_partitions if partitions is None else partitions
        replication_factor = template_config.replication_factor if replication_factor is None else replication_factor
        config = template_config.config
    else:
        if partitions is None:
            partitions = state.config.default_num_partitions
        if replication_factor is None:
            replication_factor = state.config.default_replication_factor
        config = None

    topic = Topic(topic_name, num_partitions=partitions, replication_factor=replication_factor, config=config)

    if not ensure_approval(
        f"Create topic {blue_bold(topic.name)} "
        + f"with replication factor {blue_bold(str(topic.replication_factor))} "
        + f"and {blue_bold(str(topic.num_partitions))} partition"
        + ("s" if partitions != 1 else "")
        + f" in context {blue_bold(state.config.current_context)}?",
        no_verify=state.no_verify,
    ):
        click.echo(click.style("Aborted!", bg="red"))
        return

    topic_controller.create_topics([topic])
    click.echo(click.style(f"Topic with '{topic.name}' successfully created.", fg="green"))
