import click

from esque import validation
from esque.cli.autocomplete import list_topics
from esque.cli.helpers import edit_yaml, ensure_approval
from esque.cli.options import State, default_options
from esque.cli.output import pretty_topic_diffs
from esque.resources.topic import copy_to_local


@click.command("topic")
@click.argument("topic-name", required=True, shell_complete=list_topics)
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
