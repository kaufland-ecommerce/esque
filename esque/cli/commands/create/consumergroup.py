import re
from typing import List

import click
from confluent_kafka import TopicPartition

from esque.cli.helpers import ensure_approval, fallback_to_stdin
from esque.cli.options import State, default_options
from esque.controller.consumergroup_controller import ConsumerGroupController
from esque.errors import ValidationException
from esque.resources.consumergroup import ConsumerGroup


@click.command("consumergroup")
@click.argument("consumergroup-id", callback=fallback_to_stdin, required=True, type=click.STRING, nargs=1)
@click.argument("topics", callback=fallback_to_stdin, required=True, type=click.STRING, nargs=-1)
@default_options
def create_consumergroup(state: State, consumergroup_id: str, topics: str):
    """
    Create consumer group for several topics using format <topic_name>[partition]=offset.
    [partition] and offset are optional.
    Default value for offset is 0.
    If there is no partition, consumer group will be assigned to all topic partitions.
    """
    pattern = re.compile(r"(?P<topic_name>[\w.-]+)(?:\[(?P<partition>\d+)\])?(?:=(?P<offset>\d+))?")
    topic_controller = state.cluster.topic_controller
    clean_topics: List[TopicPartition] = []
    msg = ""
    for topic in topics:
        match = pattern.match(topic)
        if not match:
            raise ValidationException("Topic name should be present")
        topic = match.group("topic_name")
        partition_match = match.group("partition")
        offset_match = match.group("offset")
        offset = int(offset_match) if offset_match else 0
        if not partition_match:
            topic_config = topic_controller.get_cluster_topic(topic)
            watermarks = topic_config.watermarks
            for part, wm in watermarks.items():
                offset = offset if wm.high >= offset else 0
                clean_topics.append(TopicPartition(topic=topic, partition=part, offset=offset))
                msg += f"{topic}[{part}]={offset}\n"
        else:
            partition = int(partition_match)
            clean_topics.append(TopicPartition(topic=topic, partition=partition, offset=offset))
            msg += f"{topic}[{partition}]={offset}\n"
    if not ensure_approval(
        f"This will create the consumer group '{consumergroup_id}' with initial offsets:\n" + msg + "\nAre you sure?",
        no_verify=state.no_verify,
    ):
        click.echo(click.style("Aborted!", bg="red"))
        return

    consumergroup_controller: ConsumerGroupController = ConsumerGroupController(state.cluster)
    created_consumergroup: ConsumerGroup = consumergroup_controller.create_consumer_group(
        consumergroup_id, offsets=clean_topics
    )
    click.echo(click.style(f"Consumer group '{created_consumergroup.id}' was successfully created", fg="green"))
