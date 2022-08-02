from typing import Optional

import click

from esque.cli.autocomplete import list_topics
from esque.cli.helpers import ensure_approval, fallback_to_stdin
from esque.cli.options import State, default_options
from esque.cli.output import blue_bold
from esque.controller.topic_controller import TopicController
from esque.errors import ValidationException
from esque.resources.topic import Topic


@click.command("topic")
@click.argument("topic-name", metavar="TOPIC_NAME", callback=fallback_to_stdin, required=False)
@click.option(
    "-l",
    "--like",
    "template_topic",
    metavar="<template_topic>",
    help="Topic to use as template.",
    shell_complete=list_topics,
)
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
    state: State, topic_name: str, template_topic: str, partitions: Optional[int], replication_factor: Optional[int]
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

    if template_topic:
        topic = topic_from_template(template_topic, partitions, replication_factor, topic_controller, topic_name)
    else:
        topic = topic_with_defaults(partitions, replication_factor, state, topic_name)

    if not ensure_approval(
        f"Create topic {blue_bold(topic.name)} "
        + f"with replication factor {blue_bold(str(topic.replication_factor))} "
        + f"and {blue_bold(str(topic.num_partitions))} partition"
        + ("s" if topic.num_partitions != 1 else "")
        + f" in context {blue_bold(state.config.current_context)}?",
        no_verify=state.no_verify,
    ):
        click.echo(click.style("Aborted!", bg="red"))
        return

    topic_controller.create_topics([topic])
    click.echo(click.style(f"Topic with '{topic.name}' successfully created.", fg="green"))


def topic_with_defaults(
    partitions: Optional[int], replication_factor: Optional[int], state: State, topic_name: str
) -> Topic:
    if partitions is None:
        partitions = state.config.default_num_partitions

    if replication_factor is None:
        replication_factor = state.config.default_replication_factor

    topic = Topic(topic_name, num_partitions=partitions, replication_factor=replication_factor)
    return topic


def topic_from_template(
    template_topic: str,
    partitions: Optional[int],
    replication_factor: Optional[int],
    topic_controller: TopicController,
    topic_name: str,
) -> Topic:
    template_config = topic_controller.get_cluster_topic(template_topic)

    if partitions is None:
        partitions = template_config.num_partitions

    if replication_factor is None:
        replication_factor = template_config.replication_factor

    config = template_config.config

    topic = Topic(topic_name, num_partitions=partitions, replication_factor=replication_factor, config=config)
    return topic
