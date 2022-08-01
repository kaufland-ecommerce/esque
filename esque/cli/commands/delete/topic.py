import click

from esque.cli.autocomplete import list_topics
from esque.cli.helpers import ensure_approval
from esque.cli.options import State, default_options
from esque.resources.topic import Topic


@click.command("topic")
@click.argument("topic-name", metavar="TOPIC_NAME", required=False, type=click.STRING, shell_complete=list_topics)
@default_options
def delete_topic(state: State, topic_name: str):
    """Delete a single topic

    WARNING: This command cannot be undone, and all data in the topic will be lost.
    """
    topic_controller = state.cluster.topic_controller
    current_topics = [topic.name for topic in topic_controller.list_topics(get_topic_objects=False)]
    if topic_name not in current_topics:
        click.echo(click.style(f"Topic [{topic_name}] doesn't exist on the cluster.", fg="red"))
    else:
        click.echo(f"Deleting {click.style(topic_name, fg='green')}")
        if ensure_approval("Are you sure?", no_verify=state.no_verify):
            topic_controller.delete_topics([Topic(topic_name)])
            click.echo(click.style(f"Topic '{topic_name}' successfully deleted.", fg="green"))
