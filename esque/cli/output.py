import json
from collections import OrderedDict
from functools import partial
from itertools import groupby
from typing import Any, Dict, List, MutableMapping, Tuple

import click
import pendulum
import yaml
from yaml import SafeDumper, ScalarNode, SequenceNode
from yaml.representer import SafeRepresenter

from esque.cli.helpers import attrgetter
from esque.controller.consumergroup_controller import ConsumerGroupOffsetPlan
from esque.resources.topic import Topic, TopicDiff, Watermark

C_MAX_INT = 2 ** 31 - 1
MILLISECONDS_PER_YEAR = 1000 * 3600 * 24 * 365


def _indent(level: int):
    return "  " * level


def pretty(value, *, break_lists=False) -> str:
    if isinstance(value, dict):
        value_str = pretty_dict(value)
    elif isinstance(value, (list, tuple)):
        value_str = pretty_list(value, break_lists=break_lists)
    elif type(value) in TYPE_MAPPING:
        value_str = TYPE_MAPPING[type(value)](value)
    else:
        value_str = str(value)

    if len(value_str.strip()) == 0:
        return "''"
    return value_str


def pretty_list(
    ugly_list: List[Any], *, break_lists=False, list_separator: str = ", ", broken_list_separator: str = "- "
) -> str:
    if len(ugly_list) == 0:
        return "[]"

    list_output = [pretty(value) for value in ugly_list]

    if any("\n" in list_element for list_element in list_output):
        break_lists = True

    if break_lists:
        sub_elements = ("\n  ".join(elem.splitlines(keepends=False)) for elem in list_output)
        return broken_list_separator + f"\n{broken_list_separator}".join(sub_elements)
    else:
        return list_separator.join(list_output)


def pretty_dict(d: MutableMapping[str, Any]) -> str:
    if len(d) == 0:
        return "{}"

    max_col_length = max([len(str(key)) for key in d.keys()])

    lines = []
    for key, value in d.items():
        if is_scalar(value):
            row = f"{pretty(key)}:".ljust(max_col_length + 2)

            unit = str(key).split(".")[-1]
            row += get_value(unit, value)
        else:
            row = f"{pretty(key)}:\n  "
            sub_lines = pretty(value).splitlines(keepends=False)
            row += "\n  ".join(sub_lines)

        if key in STYLE_MAPPING:
            row = STYLE_MAPPING[key](row)

        lines.append(row)

    return "\n".join(lines)


def get_value(unit: str, value: Any) -> str:
    if isinstance(value, str) and " -> " in value:
        values = value.split(" -> ")
        return (
            click.style(get_value(unit, values[0]), fg="red")
            + " -> "
            + click.style(get_value(unit, values[1]), fg="green")
        )

    if unit in CONVERSION_MAPPING:
        return f"{CONVERSION_MAPPING[unit](value)}"
    return pretty(value)


def is_scalar(value: Any) -> bool:
    return (value is None) or (not isinstance(value, (list, dict, OrderedDict)))


def pretty_pendulum(value: pendulum.DateTime) -> str:
    return value.to_datetime_string()


def pretty_float(value: float) -> str:
    return f"{value:0.2f}"


def pretty_int(value: int) -> str:
    return f"{value}"


def pretty_bytes(value: bytes) -> str:
    return value.decode("UTF-8")


def pretty_duration(value: Any, *, multiplier: int = 1) -> str:
    if not value:
        return ""

    if type(value) != int:
        value = int(value)

    value *= multiplier

    # Fix for conversion errors of ms > C_MAX_INT in some internal lib
    try:
        years, millis = divmod(value, MILLISECONDS_PER_YEAR)
        return pendulum.duration(years=years, milliseconds=millis).in_words()
    except OverflowError:
        return "unlimited"


def pretty_topic_diffs(topics_config_diff: Dict[str, TopicDiff]) -> str:
    output = []
    for name, diff in topics_config_diff.items():
        config_diff_attributes = {}
        for attr, old, new in diff.changes():
            config_diff_attributes[attr] = f"{old} -> {new}"
        output.append({click.style(name, bold=True, fg="yellow"): {"Config Diff": config_diff_attributes}})

    return pretty({"Configuration changes": output})


def pretty_new_topic_configs(new_topics: List[Topic]) -> str:
    new_topic_configs = []
    for topic in new_topics:
        new_topic_config = {
            "num_partitions: ": topic.num_partitions,
            "replication_factor: ": topic.replication_factor,
            "config": topic.config,
        }
        new_topic_configs.append({click.style(topic.name, bold=True, fg="green"): new_topic_config})

    return pretty({"New topics to create": new_topic_configs})


def pretty_unchanged_topic_configs(new_topics: List[Topic]) -> str:
    new_topic_configs = []
    for topic in new_topics:
        new_topic_config = {
            "num_partitions: ": topic.num_partitions,
            "replication_factor: ": topic.replication_factor,
            "config": topic.config,
        }
        new_topic_configs.append({click.style(topic.name, bold=True, fg="blue"): new_topic_config})

    return pretty({"No changes": new_topic_configs})


def pretty_offset_plan(offset_plan: List[ConsumerGroupOffsetPlan]):
    offset_plan.sort(key=attrgetter("topic_name", "partition_id"))
    for topic_name, group in groupby(offset_plan, attrgetter("topic_name")):
        group = list(group)
        max_proposed = max(len(str(elem.proposed_offset)) for elem in group)
        max_current = max(len(str(elem.current_offset)) for elem in group)
        for plan_element in group:
            new_offset = str(plan_element.proposed_offset).rjust(max_proposed)
            format_args = dict(
                topic_name=plan_element.topic_name,
                partition_id=plan_element.partition_id,
                current_offset=plan_element.current_offset,
                new_offset=new_offset if plan_element.offset_equal else bold(click.style(new_offset, fg="red")),
                max_current=max_current,
            )
            click.echo(
                "Topic: {topic_name}, partition {partition_id:2}, current offset: {current_offset:{max_current}}, new offset: {new_offset}".format(
                    **format_args
                )
            )


def pretty_size(value: Any) -> str:
    if type(value) != int:
        value = int(value)
    units = [
        ("Eib", 1024 ** 6),
        ("Pib", 1024 ** 5),
        ("Tib", 1024 ** 4),
        ("Gib", 1024 ** 3),
        ("Mib", 1024 ** 2),
        ("kib", 1024 ** 1),
        ("b", 1024 ** 0),
    ]
    for sign, size in units:
        if value >= size:
            return f"{pretty_float(value / size)} {sign}"


def bold(s: str) -> str:
    return click.style(s, bold=True)


def blue_bold(s: str) -> str:
    return bold(click.style(s, fg="blue"))


def green_bold(s: str) -> str:
    return bold(click.style(s, fg="green"))


def red_bold(s: str) -> str:
    return bold(click.style(s, fg="red"))


STYLE_MAPPING = {
    "topic": green_bold,
    "cleanup.policy": bold,
    "flush.ms": bold,
    "delete.retention.ms": bold,
    "retention.ms": bold,
    "low_watermark": blue_bold,
    "high_watermark": blue_bold,
    "member_id": bold,
}

CONVERSION_MAPPING = {
    "ms": pretty_duration,
    "seconds": partial(pretty_duration, multiplier=1000),
    "bytes": pretty_size,
}

TYPE_MAPPING = {
    pendulum.DateTime: pretty_pendulum,
    float: pretty_float,
    int: pretty_int,
    dict: pretty_dict,
    OrderedDict: pretty_dict,
    list: pretty_list,
    bytes: pretty_bytes,
}


def tuple_representer(dumper: SafeRepresenter, data: Watermark) -> SequenceNode:
    return dumper.represent_list(list(data))


def watermark_representer(dumper: SafeRepresenter, data: Watermark) -> SequenceNode:
    return dumper.represent_list(list(data))


def bytes_representer(dumper: SafeRepresenter, data: bytes) -> ScalarNode:
    return dumper.represent_str(data.decode("UTF-8"))


SafeDumper.add_representer(Tuple, tuple_representer)
SafeDumper.add_representer(bytes, bytes_representer)
SafeDumper.add_representer(Watermark, watermark_representer)


class BytesEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, bytes):
            return obj.decode("UTF-8")
            # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)


def format_output(output: Any, output_format: str) -> str:
    if output_format == "yaml":
        return yaml.dump(output, default_flow_style=False, sort_keys=False, Dumper=yaml.SafeDumper)
    elif output_format == "json":
        return json.dumps(output, indent=4, cls=BytesEncoder)
    elif isinstance(output, list):
        return pretty_list(output, break_lists=True, broken_list_separator="")
    else:
        return pretty(output, break_lists=True)
