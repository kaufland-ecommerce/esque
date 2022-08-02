from typing import List, Tuple

import click

from esque.cli.autocomplete import list_topics
from esque.cli.helpers import ensure_approval, get_piped_stdin_arguments
from esque.cli.options import State, default_options
from esque.resources.topic import Topic


@click.command("topics")
@click.argument("topic-list", metavar="TOPIC_LIST", required=False, shell_complete=list_topics, nargs=-1)
@default_options
def delete_topics(state: State, topic_list: Tuple[str]):
    """Delete multiple topics

    WARNING: This command cannot be undone, and all data in the topics will be lost.
    """
    topic_names = list(topic_list) + get_piped_stdin_arguments()
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
            topic_controller.delete_topics([Topic(topic_name) for topic_name in existing_topics])
            click.echo(click.style(f"Topics '{existing_topics}' successfully deleted.", fg="green"))
