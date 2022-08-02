from typing import List, Tuple

import click

from esque.cli.autocomplete import list_consumergroups
from esque.cli.helpers import ensure_approval, get_piped_stdin_arguments
from esque.cli.options import State, default_options
from esque.controller.consumergroup_controller import ConsumerGroupController


@click.command("consumergroup")
@click.argument("consumergroup-id", required=False, type=click.STRING, shell_complete=list_consumergroups, nargs=-1)
@default_options
def delete_consumergroup(state: State, consumergroup_id: Tuple[str]):
    """Delete consumer groups"""
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
