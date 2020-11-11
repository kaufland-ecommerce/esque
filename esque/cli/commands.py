import getpass
import logging
import pwd
import sys
import time
from pathlib import Path
from shutil import copyfile
from time import sleep
from typing import List, Tuple

import click
import yaml
from click import MissingParameter, version_option

from esque import __version__, validation
from esque.cli.helpers import attrgetter, edit_yaml, ensure_approval, get_piped_stdin_arguments, isatty
from esque.cli.options import State, default_options, output_format_option
from esque.cli.output import (
    blue_bold,
    bold,
    format_output,
    green_bold,
    pretty,
    pretty_new_topic_configs,
    pretty_offset_plan,
    pretty_topic_diffs,
    pretty_unchanged_topic_configs,
    red_bold,
)
from esque.clients.consumer import ConsumerFactory, consume_to_file_ordered, consume_to_files
from esque.clients.producer import PingProducer, ProducerFactory
from esque.config import ESQUE_GROUP_ID, PING_TOPIC, Config, config_dir, config_path, migration, sample_config_path
from esque.controller.consumergroup_controller import ConsumerGroupController
from esque.errors import TopicAlreadyExistsException, TopicDoesNotExistException, ValidationException
from esque.resources.broker import Broker
from esque.resources.topic import Topic, copy_to_local


@click.group(invoke_without_command=True, no_args_is_help=True)
@version_option(__version__)
@default_options
def esque(state: State):
    """esque - an operational kafka tool.

    In the Kafka world nothing is easy, but esque (pronounced esk) is an attempt at it.
    """
    pass


@esque.group(help="Get a quick overview of different resources.", no_args_is_help=True)
@default_options
def get(state: State):
    pass


@esque.group(help="Get detailed information about a resource.", no_args_is_help=True)
@default_options
def describe(state: State):
    pass


@esque.group(help="Create a new instance of a resource.", no_args_is_help=True)
@default_options
def create(state: State):
    pass


@esque.group(help="Delete a resource.", no_args_is_help=True)
@default_options
def delete(state: State):
    pass


@esque.group(help="Edit a resource.", no_args_is_help=True)
@default_options
def edit(state: State):
    pass


@esque.group(help="Configuration-related options.", no_args_is_help=True)
@default_options
def config(state: State):
    pass


@esque.group(name="set", help="Set resource attributes.")
@default_options
def set_(state: State):
    pass


def list_brokers(ctx, args, incomplete):
    state = ctx.ensure_object(State)
    all_broker_hosts_names = [f"{broker.host}:{broker.port}" for broker in Broker.get_all(state.cluster)]
    return [broker for broker in all_broker_hosts_names if broker.startswith(incomplete)]


def list_consumergroups(ctx, args, incomplete):
    state = ctx.ensure_object(State)
    return [
        group
        for group in ConsumerGroupController(state.cluster).list_consumer_groups()
        if group.startswith(incomplete)
    ]


def list_contexts(ctx, args, incomplete):
    state = ctx.ensure_object(State)
    return [context for context in state.config.available_contexts if context.startswith(incomplete)]


def list_topics(ctx, args, incomplete):
    state = ctx.ensure_object(State)
    cluster = state.cluster
    return [
        topic.name for topic in cluster.topic_controller.list_topics(search_string=incomplete, get_topic_objects=False)
    ]


def fallback_to_stdin(ctx, args, value):
    stdin = click.get_text_stream("stdin")
    if not value and not isatty(stdin):
        stdin_arg = stdin.readline().strip()
    else:
        stdin_arg = value
    if not stdin_arg:
        raise MissingParameter("No value specified!")

    return stdin_arg


@esque.command("ctx")
@click.argument("context", required=False, default=None, autocompletion=list_contexts)
@default_options
def ctx(state: State, context: str):
    """List contexts and switch between them.

    \b
    USAGE:
    esque ctx               : list available contexts
    esque ctx CONTEXT       : switch to context CONTEXT
    """
    if not context:
        for c in state.config.available_contexts:
            if c == state.config.current_context:
                click.echo(bold(c))
            else:
                click.echo(c)
    if context:
        state.config.context_switch(context)
        state.config.save()
        click.echo(f"Switched to context: {context}.")


@config.command("recreate")
@default_options
def config_recreate(state: State):
    """(Re)create esque config.

    Overwrites the existing esque config file with the sample config. If no esque config file already exists,
    create one with the sample config."""
    config_dir().mkdir(exist_ok=True)
    if ensure_approval(f"Should the current config in {config_dir()} get replaced?", no_verify=state.no_verify):
        copyfile(sample_config_path().as_posix(), config_path())


@config.command("fix")
@default_options
def config_fix(state: State):
    """Fix simple errors in esque config.

    Fixes simple errors like wrong current_contexts in the esque config when the configs was tampered with manually."""
    try:
        state.config.context_switch(state.config.current_context)
        click.echo("Your config seems fine. 🎉")
    except ValidationException:
        _cfg: Config = Config(disable_validation=True)
        if _cfg.current_context not in _cfg.available_contexts:
            click.echo(f"Found invalid current context. Switching context to state {_cfg.available_contexts[0]}.")
            _cfg.context_switch(_cfg.available_contexts[0])
            Config.set_instance(_cfg)
            state.config.save()
        else:
            click.echo("Can't fix this configuration error try fixing it manually.")


@config.command("autocomplete")
@default_options
def config_autocomplete(state: State):
    """Configure esque autocompletion functionality.

    Generate the autocompletion script based on the current shell and
    give instructions to install it into the current environment.
    """
    directory = config_dir()
    config_file_name = "autocomplete.sh"
    config_file: Path = directory / config_file_name
    current_shell = pwd.getpwnam(getpass.getuser()).pw_shell.split("/")[-1]
    source_designator = "source" if current_shell in ["bash", "sh"] else "source_zsh"
    default_environment = ".bashrc" if current_shell in ["bash", "sh"] else ".zshrc"
    with open(config_file.absolute(), "w") as config_fd:
        config_fd.write('eval "$(_ESQUE_COMPLETE=' + source_designator + ' esque)"')
    click.echo("Autocompletion script generated to " + green_bold(str(config_file.absolute())))
    click.echo(
        "To use the autocompletion feature, simply source the contents of the script into your environment, e.g."
    )
    click.echo(
        '\t\techo -e "\\nsource '
        + str(config_file.absolute())
        + '" >> '
        + str(pwd.getpwnam(getpass.getuser()).pw_dir)
        + "/"
        + default_environment
    )


@config.command("edit", short_help="Edit esque config file.")
@default_options
def config_edit(state: State):
    """Opens the user's esque config file in the default editor."""
    old_yaml = config_path().read_text()
    new_yaml, _ = edit_yaml(old_yaml, validator=validation.validate_esque_config)
    config_path().write_text(new_yaml)


@config.command("migrate")
@default_options
def config_migrate(state: State):
    """Migrate esque config to current version.

    If the user's esque config file is from a previous version, migrate it to the current version.
    A backup of the original config file is created with the same name and the extension .bak"""
    new_path, backup = migration.migrate(config_path())
    click.echo(f"Your config has been migrated and is now at {new_path}. A backup has been created at {backup}.")


@create.command("topic")
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
        click.echo("Aborted!")
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


@edit.command("topic")
@click.argument("topic-name", required=True, autocompletion=list_topics)
@default_options
def edit_topic(state: State, topic_name: str):
    """Edit a topic.

    Open the topic's configuration in the default editor. If the user saves upon exiting the editor,
    all the given changes will be applied to the topic.
    """
    controller = state.cluster.topic_controller
    topic = state.cluster.topic_controller.get_cluster_topic(topic_name)

    _, new_conf = edit_yaml(topic.to_yaml(only_editable=True), validator=validation.validate_editable_topic_config)

    local_topic = copy_to_local(topic)
    local_topic.update_from_dict(new_conf)
    diff = controller.diff_with_cluster(local_topic)
    if not diff.has_changes:
        click.echo("Nothing changed.")
        return

    click.echo(pretty_topic_diffs({topic_name: diff}))
    if ensure_approval("Are you sure?"):
        controller.alter_configs([local_topic])
    else:
        click.echo("Canceled!")


@set_.command("offsets")
@click.argument("consumer-id", callback=fallback_to_stdin, type=click.STRING, required=True)
@click.option(
    "-t",
    "--topic-name",
    help="Regular expression describing the topic name (default: all subscribed topics)",
    type=click.STRING,
    required=False,
)
@click.option("--offset-to-value", help="Set offset to the specified value", type=click.INT, required=False)
@click.option("--offset-by-delta", help="Shift offset by specified value", type=click.INT, required=False)
@click.option(
    "--offset-to-timestamp",
    help="Set offset to that of the first message with timestamp on or after the specified timestamp in format "
    "YYYY-MM-DDTHH:mm:ss, i.e. skip all messages before this timestamp.",
    type=click.STRING,
    required=False,
)
@click.option(
    "--offset-from-group", help="Copy all offsets from an existing consumer group.", type=click.STRING, required=False
)
@default_options
def set_offsets(
    state: State,
    consumer_id: str,
    topic_name: str,
    offset_to_value: int,
    offset_by_delta: int,
    offset_to_timestamp: str,
    offset_from_group: str,
):
    """Set consumer group offsets.

    Change or set the offset of a consumer group for a topic, i.e. the message number the consumer group will read next.
    This can be done by specifying an explicit offset (--offset-to-value), a delta to shift the current offset forwards
    or backwards (--offset-by-delta), a timestamp in which the offset of the first message on or after the timestamp is
    taken (--offset-by-timestamp), or a group from which to copy the offsets from. In the case that the consumer group
    reads from more than one topic, a regular expression can be given to specify the offset of which topic to change.
    NOTE: the default is to change the offset for all topics."""
    logger = logging.getLogger(__name__)
    consumergroup_controller = ConsumerGroupController(state.cluster)
    offset_plan = consumergroup_controller.create_consumer_group_offset_change_plan(
        consumer_id=consumer_id,
        topic_name=topic_name if topic_name else ".*",
        offset_to_value=offset_to_value,
        offset_by_delta=offset_by_delta,
        offset_to_timestamp=offset_to_timestamp,
        offset_from_group=offset_from_group,
    )

    if offset_plan and len(offset_plan) > 0:
        click.echo(green_bold("Proposed offset changes: "))
        pretty_offset_plan(offset_plan)
        if ensure_approval("Are you sure?", no_verify=state.no_verify):
            consumergroup_controller.edit_consumer_group_offsets(consumer_id=consumer_id, offset_plan=offset_plan)
    else:
        logger.info("No changes proposed.")
        return


@edit.command("offsets")
@click.argument("consumer-id", callback=fallback_to_stdin, type=click.STRING, required=True)
@click.option(
    "-t",
    "--topic-name",
    help="Regular expression describing the topic name (default: all subscribed topics)",
    type=click.STRING,
    required=False,
)
@default_options
def edit_offsets(state: State, consumer_id: str, topic_name: str):
    """Edit a topic.

    Open the offsets of the consumer group in the default editor. If the user saves upon exiting the editor,
    all the offsets will be set to the given values.
    """
    logger = logging.getLogger(__name__)
    consumergroup_controller = ConsumerGroupController(state.cluster)
    consumer_group_state, offset_plans = consumergroup_controller.read_current_consumer_group_offsets(
        consumer_id=consumer_id, topic_name_expression=topic_name if topic_name else ".*"
    )
    if consumer_group_state != "Empty":
        logger.error(
            "Consumergroup {} is not empty. Setting offsets is only allowed for empty consumer groups.".format(
                consumer_id
            )
        )
    sorted_offset_plan = list(offset_plans.values())
    sorted_offset_plan.sort(key=attrgetter("topic_name", "partition_id"))
    offset_plan_as_yaml = {
        "offsets": [
            {"topic": element.topic_name, "partition": element.partition_id, "offset": element.current_offset}
            for element in sorted_offset_plan
        ]
    }
    _, new_conf = edit_yaml(str(offset_plan_as_yaml), validator=validation.validate_offset_config)

    for new_offset in new_conf["offsets"]:
        plan_key: str = f"{new_offset['topic']}::{new_offset['partition']}"
        if plan_key in offset_plans:
            final_value, error, message = ConsumerGroupController.select_new_offset_for_consumer(
                requested_offset=new_offset["offset"], offset_plan=offset_plans[plan_key]
            )
            if error:
                logger.error(message)
            offset_plans[plan_key].proposed_offset = final_value

    if offset_plans and len(offset_plans) > 0:
        click.echo(green_bold("Proposed offset changes: "))
        pretty_offset_plan(list(offset_plans.values()))
        if ensure_approval("Are you sure?", no_verify=state.no_verify):
            consumergroup_controller.edit_consumer_group_offsets(
                consumer_id=consumer_id, offset_plan=list(offset_plans.values())
            )
    else:
        logger.info("No changes proposed.")
        return


@delete.command("consumergroup")
@click.argument("consumergroup-id", required=False, type=click.STRING, autocompletion=list_consumergroups, nargs=-1)
@default_options
def delete_consumer_group(state: State, consumergroup_id: Tuple[str]):
    """Delete consumer groups
    """
    consumer_groups = list(consumergroup_id) + get_piped_stdin_arguments()
    consumergroup_controller: ConsumerGroupController = ConsumerGroupController(state.cluster)
    current_consumergroups = consumergroup_controller.list_consumer_groups()
    existing_consumer_groups: List[str] = []
    for group in consumer_groups:
        if group in current_consumergroups:
            click.echo(f"Deleting {click.style(group, fg='green')}")
            existing_consumer_groups.append(group)
        else:
            click.echo(f"Skipping {click.style(group, fg='yellow')} — does not exist")
    if not existing_consumer_groups:
        click.echo(click.style("The provided list contains no existing consumer groups.", fg="red"))
    else:
        if ensure_approval("Are you sure?", no_verify=state.no_verify):
            consumergroup_controller.delete_consumer_groups(existing_consumer_groups)
            current_consumergroups = consumergroup_controller.list_consumer_groups()
            assert all(consumer_group not in current_consumergroups for consumer_group in existing_consumer_groups)
        click.echo(click.style(f"Consumer groups '{existing_consumer_groups}' successfully deleted.", fg="green"))


@delete.command("topic")
@click.argument(
    "topic-name", metavar="TOPIC_NAME", required=False, type=click.STRING, autocompletion=list_topics, nargs=-1
)
@default_options
def delete_topic(state: State, topic_name: str):
    """Delete a topic

    WARNING: This command cannot be undone, and all data in the topic will be lost.
    """
    topic_names = list(topic_name) + get_piped_stdin_arguments()
    topic_controller = state.cluster.topic_controller
    current_topics = [topic.name for topic in topic_controller.list_topics(get_topic_objects=False)]
    existing_topics: List[str] = []
    for topic in topic_names:
        if topic in current_topics:
            click.echo(f"Deleting {click.style(topic, fg='green')}")
            existing_topics.append(topic)
        else:
            click.echo(f"Skipping {click.style(topic, fg='yellow')} — does not exist")
    if not existing_topics:
        click.echo(click.style("The provided list contains no existing topics.", fg="red"))
    else:
        if ensure_approval("Are you sure?", no_verify=state.no_verify):
            for topic_name in existing_topics:
                topic_controller.delete_topic(Topic(topic_name))
                assert topic_name not in (t.name for t in topic_controller.list_topics(get_topic_objects=False))
            click.echo(click.style(f"Topics '{existing_topics}' successfully deleted.", fg="green"))


@esque.command("apply")
@click.option("-f", "--file", metavar="<file>", help="Config file path.", required=True)
@default_options
def apply(state: State, file: str):
    """Apply a set of topic configurations.

    Create new topics and apply changes to existing topics, as specified in the config yaml file <file>.
    """

    # Get topic data based on the YAML
    yaml_topic_configs = yaml.safe_load(open(file)).get("topics")
    yaml_topics = [Topic.from_dict(conf) for conf in yaml_topic_configs]
    yaml_topic_names = [t.name for t in yaml_topics]
    if not len(yaml_topic_names) == len(set(yaml_topic_names)):
        raise ValidationException("Duplicate topic names in the YAML!")

    # Get topic data based on the cluster state
    topic_controller = state.cluster.topic_controller
    cluster_topics = topic_controller.list_topics(search_string="|".join(yaml_topic_names))
    cluster_topic_names = [t.name for t in cluster_topics]

    # Calculate changes
    to_create = [yaml_topic for yaml_topic in yaml_topics if yaml_topic.name not in cluster_topic_names]
    to_edit = [
        yaml_topic
        for yaml_topic in yaml_topics
        if yaml_topic not in to_create and topic_controller.diff_with_cluster(yaml_topic).has_changes
    ]
    to_edit_diffs = {t.name: topic_controller.diff_with_cluster(t) for t in to_edit}
    to_ignore = [yaml_topic for yaml_topic in yaml_topics if yaml_topic not in to_create and yaml_topic not in to_edit]

    # Sanity check - the 3 groups of topics should be complete and have no overlap
    assert (
        set(to_create).isdisjoint(set(to_edit))
        and set(to_create).isdisjoint(set(to_ignore))
        and set(to_edit).isdisjoint(set(to_ignore))
        and len(to_create) + len(to_edit) + len(to_ignore) == len(yaml_topics)
    )

    # Print diffs so the user can check
    click.echo(pretty_unchanged_topic_configs(to_ignore))
    click.echo(pretty_new_topic_configs(to_create))
    click.echo(pretty_topic_diffs(to_edit_diffs))

    # Check for actionable changes
    if len(to_edit) + len(to_create) == 0:
        click.echo("No changes detected, aborting!")
        return

    # Warn users & abort when replication & num_partition changes are attempted
    if any(not diff.is_valid for _, diff in to_edit_diffs.items()):
        click.echo(
            "Changes to `replication_factor` and `num_partitions` can not be applied on already existing topics."
        )
        click.echo("Cancelling due to invalid changes")
        return

    # Get approval
    if not ensure_approval("Apply changes?", no_verify=state.no_verify):
        click.echo("Cancelling changes")
        return

    # apply changes
    topic_controller.create_topics(to_create)
    topic_controller.alter_configs(to_edit)

    # output confirmation
    changes = {"unchanged": len(to_ignore), "created": len(to_create), "changed": len(to_edit)}
    click.echo(click.style(pretty({"Successfully applied changes": changes}), fg="green"))


@describe.command("topic")
@click.argument(
    "topic-name",
    metavar="TOPIC_NAME",
    callback=fallback_to_stdin,
    required=False,
    type=click.STRING,
    autocompletion=list_topics,
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


@get.command("watermarks", short_help="Return watermarks by topic.")
@click.option(
    "-t", "--topic-name", metavar="<topic_name>", required=False, type=click.STRING, autocompletion=list_topics
)
@output_format_option
@default_options
def get_watermarks(state: State, topic_name: str, output_format: str):
    """Returns the high and low watermarks for <topic_name>, or if not specified, all topics."""
    # TODO: Gathering of all watermarks takes super long
    topics = state.cluster.topic_controller.list_topics(search_string=topic_name)

    watermarks = {topic.name: max(v for v in topic.watermarks.values()) for topic in topics}

    click.echo(format_output(watermarks, output_format))


@describe.command("broker", short_help="Describe a broker.")
@click.argument("broker", metavar="BROKER", callback=fallback_to_stdin, autocompletion=list_brokers, required=False)
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


@describe.command("consumergroup", short_help="Describe a consumer group.")
@click.argument("consumergroup-id", callback=fallback_to_stdin, autocompletion=list_consumergroups, required=True)
@click.option(
    "--all-partitions",
    help="List status for all topic partitions instead of just summarizing each topic.",
    default=False,
    is_flag=True,
)
@output_format_option
@default_options
def describe_consumergroup(state: State, consumergroup_id: str, all_partitions: bool, output_format: str):
    """Return information on group coordinator, offsets, watermarks, lag, and various metadata
    for consumer group CONSUMER_GROUP."""
    consumer_group = ConsumerGroupController(state.cluster).get_consumer_group(consumergroup_id)
    consumer_group_desc = consumer_group.describe(verbose=all_partitions)

    click.echo(format_output(consumer_group_desc, output_format))


@get.command("brokers")
@output_format_option
@default_options
def get_brokers(state: State, output_format: str):
    """List all brokers.

    Return the broker id's and socket addresses of all the brokers in the kafka cluster defined in the current context.
    """
    brokers = Broker.get_all(state.cluster)
    broker_ids_and_hosts = [f"{broker.broker_id}: {broker.host}:{broker.port}" for broker in brokers]
    click.echo(format_output(broker_ids_and_hosts, output_format))


@get.command("consumergroups")
@output_format_option
@default_options
def get_consumergroups(state: State, output_format: str):
    """List all consumer groups."""
    groups = ConsumerGroupController(state.cluster).list_consumer_groups()
    click.echo(format_output(groups, output_format))


@get.command("topics")
@click.option("-p", "--prefix", type=click.STRING, autocompletion=list_topics)
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


@esque.command("consume")
@click.argument("topic", autocompletion=list_topics)
@click.option(
    "-d", "--directory", metavar="<directory>", help="Sets the directory to write the messages to.", type=click.STRING
)
@click.option(
    "-f",
    "--from",
    "from_context",
    metavar="<source_ctx>",
    help="Source context. If not provided, the current context will be used.",
    autocompletion=list_contexts,
    type=click.STRING,
    required=False,
)
@click.option(
    "-n", "--number", metavar="<n>", help="Number of messages.", type=click.INT, default=sys.maxsize, required=False
)
@click.option(
    "-m",
    "--match",
    metavar="<filter_expression>",
    help="Message filtering expression.",
    type=click.STRING,
    required=False,
)
@click.option("--last/--first", help="Start consuming from the earliest or latest offset in the topic.", default=False)
@click.option("-a", "--avro", help="Set this flag if the topic contains avro data.", default=False, is_flag=True)
@click.option(
    "-c",
    "--consumergroup",
    metavar="<consumer_group>",
    help="Consumer group to store the offset in.",
    type=click.STRING,
    autocompletion=list_consumergroups,
    default=None,
    required=False,
)
@click.option(
    "--preserve-order",
    help="Preserve the order of messages, regardless of their partition.",
    default=False,
    is_flag=True,
)
@click.option("--stdout", "write_to_stdout", help="Write messages to STDOUT.", default=False, is_flag=True)
@default_options
def consume(
    state: State,
    topic: str,
    from_context: str,
    number: int,
    match: str,
    last: bool,
    avro: bool,
    directory: str,
    consumergroup: str,
    preserve_order: bool,
    write_to_stdout: bool,
):
    """Consume messages from a topic.

    Read messages from a given topic in a given context. These messages can either be written
    to files in an automatically generated directory (default behavior), or to STDOUT.

    \b
    EXAMPLES:
    # Consume the first 10 messages from TOPIC in the current context and print them to STDOUT in order.
    esque consume --first -n 10 --preserve-order --stdout TOPIC

    \b
    # Consume <n> messages, starting from the 10th, from TOPIC in the <source_ctx> context and write them to files.
    esque consume --match "message.offset > 9" -n <n> TOPIC -f <source_ctx>

    \b
    # Copy source_topic in first context to destination_topic in second-context.
    esque consume -f first-context --stdout source_topic | esque produce -t second-context --stdin destination_topic
    """
    current_timestamp_milliseconds = int(round(time.time() * 1000))

    if directory and write_to_stdout:
        raise ValueError("Cannot write to a directory and STDOUT, please pick one!")

    if not from_context:
        from_context = state.config.current_context
    state.config.context_switch(from_context)

    if topic not in map(attrgetter("name"), state.cluster.topic_controller.list_topics(get_topic_objects=False)):
        raise TopicDoesNotExistException(f"Topic {topic} does not exist!", -1)

    if not consumergroup:
        consumergroup = ESQUE_GROUP_ID
    if not directory:
        directory = Path() / "messages" / topic / str(current_timestamp_milliseconds)
    output_directory = Path(directory)

    if not write_to_stdout:
        click.echo(f"Creating directory {blue_bold(str(output_directory))} if it does not exist.")
        output_directory.mkdir(parents=True, exist_ok=True)
        click.echo(f"Start consuming from topic {blue_bold(topic)} in source context {blue_bold(from_context)}.")
    if preserve_order:
        partitions = [
            partition.partition_id for partition in state.cluster.topic_controller.get_cluster_topic(topic).partitions
        ]
        total_number_of_consumed_messages = consume_to_file_ordered(
            output_directory=output_directory,
            topic=topic,
            group_id=consumergroup,
            partitions=partitions,
            desired_message_count=number,
            avro=avro,
            match=match,
            last=last,
            write_to_stdout=write_to_stdout,
        )
    else:
        total_number_of_consumed_messages = consume_to_files(
            output_directory=output_directory,
            topic=topic,
            group_id=consumergroup,
            desired_message_count=number,
            avro=avro,
            match=match,
            last=last,
            write_to_stdout=write_to_stdout,
        )

    if not write_to_stdout:
        click.echo(f"Output generated to {blue_bold(str(output_directory))}")
        if total_number_of_consumed_messages == number or number == sys.maxsize:
            click.echo(blue_bold(str(total_number_of_consumed_messages)) + " messages consumed.")
        else:
            click.echo(
                "Only found "
                + bold(str(total_number_of_consumed_messages))
                + " messages in topic, out of "
                + blue_bold(str(number))
                + " required."
            )


@esque.command("produce")
@click.argument("topic", autocompletion=list_topics)
@click.option(
    "-d",
    "--directory",
    metavar="<directory>",
    help="Directory containing Kafka messages.",
    type=click.STRING,
    required=False,
)
@click.option(
    "-t",
    "--to",
    "to_context",
    metavar="<destination_ctx>",
    help="Destination context.",
    type=click.STRING,
    autocompletion=list_contexts,
    required=False,
)
@click.option(
    "-m",
    "--match",
    metavar="<filter_expresion>",
    help="Message filtering expression.",
    type=click.STRING,
    required=False,
)
@click.option("-a", "--avro", help="Set this flag if the topic contains avro data.", default=False, is_flag=True)
@click.option(
    "--stdin", "read_from_stdin", help="Read messages from STDIN instead of a directory.", default=False, is_flag=True
)
@click.option(
    "-y",
    "--ignore-errors",
    "ignore_stdin_errors",
    help="Only when reading from STDIN. If JSON validation fails, write the malformed JSON as a string in message value"
    " (without key and specified partition assignment).",
    default=False,
    is_flag=True,
)
@default_options
def produce(
    state: State,
    topic: str,
    to_context: str,
    directory: str,
    avro: bool,
    match: str = None,
    read_from_stdin: bool = False,
    ignore_stdin_errors: bool = False,
):
    """Produce messages to a topic.

    Write messages to a given topic in a given context. These messages can come from either a directory <directory>
    containing files corresponding to the different partitions or from STDIN.

    \b
    EXAMPLES:
    # Write all messages from the files in <directory> to TOPIC in the <destination_ctx> context.
    esque produce -d <directory> -t <destination_ctx> TOPIC

    \b
    # Start environment in terminal to write messages to TOPIC in the <destination_ctx> context.
    esque produce --stdin -f <destination_ctx> -y TOPIC

    \b
    # Copy source_topic to destination_topic.
    esque consume -f first-context --stdout source_topic | esque produce -t second-context --stdin destination_topic
    """
    if directory is None and not read_from_stdin:
        raise ValueError("You have to provide a directory or use the --stdin flag.")

    if directory is not None:
        input_directory = Path(directory)
        if not input_directory.exists():
            raise ValueError(f"Directory {directory} does not exist!")

    if not to_context:
        to_context = state.config.current_context
    state.config.context_switch(to_context)

    topic_controller = state.cluster.topic_controller
    if topic not in map(attrgetter("name"), topic_controller.list_topics(get_topic_objects=False)):
        click.echo(f"Topic {blue_bold(topic)} does not exist in context {blue_bold(to_context)}.")
        if ensure_approval("Would you like to create it now?"):
            topic_controller.create_topics([Topic(topic)])
        else:
            raise TopicDoesNotExistException(f"Topic {topic} does not exist!", -1)

    stdin = click.get_text_stream("stdin")
    if read_from_stdin and isatty(stdin):
        click.echo(
            "Type the messages to produce, "
            + ("in JSON format, " if not ignore_stdin_errors else "")
            + blue_bold("one per line")
            + ". End with "
            + blue_bold("CTRL+D")
            + "."
        )
    elif read_from_stdin and not isatty(stdin):
        click.echo(f"Reading messages from an external source, {blue_bold('one per line')}).")
    else:
        click.echo(
            f"Producing from directory {blue_bold(str(directory))} to topic {blue_bold(topic)}"
            f" in target context {blue_bold(to_context)}"
        )
    producer = ProducerFactory().create_producer(
        topic_name=topic,
        input_directory=input_directory if not read_from_stdin else None,
        avro=avro,
        match=match,
        ignore_stdin_errors=ignore_stdin_errors,
    )
    total_number_of_messages_produced = producer.produce()
    click.echo(
        green_bold(str(total_number_of_messages_produced))
        + " messages successfully produced to topic "
        + blue_bold(topic)
        + " in context "
        + blue_bold(to_context)
        + "."
    )


@esque.command("ping")
@click.option("-t", "--times", help="Number of pings.", default=10)
@click.option("-w", "--wait", help="Seconds to wait between pings.", default=1)
@default_options
def ping(state: State, times: int, wait: int):
    """Test the connection to the kafka cluster.

    Ping the kafka cluster by writing messages to and reading messages from it.
    After the specified number of "pings", return the minimum, maximum, and average time for the round trip.
    """
    topic_controller = state.cluster.topic_controller
    deltas = []
    try:
        try:
            topic_controller.create_topics([Topic(PING_TOPIC)])
        except TopicAlreadyExistsException:
            pass
        producer = PingProducer(PING_TOPIC)
        consumer = ConsumerFactory().create_ping_consumer(group_id=ESQUE_GROUP_ID, topic_name=PING_TOPIC)
        click.echo(f"Pinging with {state.cluster.bootstrap_servers}.")

        for i in range(times):
            producer.produce()
            _, delta = consumer.consume()
            deltas.append(delta)
            click.echo(f"m_seq={i} time={delta:.2f}ms")
            sleep(wait)
    except KeyboardInterrupt:
        return
    topic_controller.delete_topic(Topic(PING_TOPIC))
    click.echo("--- statistics ---")
    click.echo(f"{len(deltas)} messages sent/received.")
    click.echo(f"min/avg/max = {min(deltas):.2f}/{(sum(deltas) / len(deltas)):.2f}/{max(deltas):.2f} ms")
