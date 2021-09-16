import click
import yaml

from esque.cli.helpers import ensure_approval
from esque.cli.options import State, default_options
from esque.cli.output import pretty, pretty_new_topic_configs, pretty_topic_diffs, pretty_unchanged_topic_configs
from esque.errors import ValidationException
from esque.resources.topic import Topic


@click.command("apply")
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
    cluster_topics = state.cluster.topic_controller.list_topics(
        search_string="|".join(yaml_topic_names), get_partitions=False
    )
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
        exit(1)

    # Get approval
    if not ensure_approval("Apply changes?", no_verify=state.no_verify):
        click.echo(click.style("Cancelling changes", bg="red"))
        return

    # apply changes
    topic_controller.create_topics(to_create)
    topic_controller.alter_configs(to_edit)

    # output confirmation
    changes = {"unchanged": len(to_ignore), "created": len(to_create), "changed": len(to_edit)}
    click.echo(click.style(pretty({"Successfully applied changes": changes}), fg="green"))
