import logging
from operator import attrgetter

import click

from esque import validation
from esque.cli.helpers import edit_yaml, ensure_approval, fallback_to_stdin
from esque.cli.options import State, default_options
from esque.cli.output import green_bold, pretty_offset_plan
from esque.controller.consumergroup_controller import ConsumerGroupController


@click.command("offsets")
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
