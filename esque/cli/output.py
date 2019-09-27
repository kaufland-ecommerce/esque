from collections import OrderedDict
from functools import partial
from typing import Any, Dict, List, MutableMapping, Tuple

import click
import pendulum

from esque.resources.consumergroup import ConsumerGroup
from esque.resources.topic import Topic, TopicDiff

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


def pretty_list(l: List[Any], *, break_lists=False, list_separator: str = ", ") -> str:
    if len(l) == 0:
        return "[]"

    list_output = [pretty(value) for value in l]

    if any("\n" in list_element for list_element in list_output):
        break_lists = True

    if break_lists:
        sub_elements = ("\n  ".join(elem.splitlines(keepends=False)) for elem in list_output)
        return "- " + "\n- ".join(sub_elements)
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


def color_code_float(value: float, color_codes: List[Tuple[float, str]]):
    str_value = f"{value:.2f}"
    result = str_value
    for color_code in color_codes:
        if value >= color_code[0]:
            result = f"{click.style(str_value, bold=True, fg=color_code[1])}"
    return result


def pretty_duration(value: Any, *, multiplier: int = 1) -> str:
    if not value:
        return ""

    if type(value) != int:
        value = int(value)

    value *= multiplier

    # Fix for conversion errors of ms > C_MAX_INT in some internal lib
    if value > MILLISECONDS_PER_YEAR:
        value = int(value / MILLISECONDS_PER_YEAR)
        return pendulum.duration(years=value).in_words()

    return pendulum.duration(milliseconds=value).in_words()


def pretty_topic_diffs(topics_config_diff: Dict[str, TopicDiff]) -> str:
    output = []
    for name, diff in topics_config_diff.items():
        config_diff_attributes = {}
        for attr, old, new in diff.changes():
            config_diff_attributes[attr] = f"{old} -> {new}"
        output.append({click.style(name, bold=True, fg="yellow"): {"Config Diff": config_diff_attributes}})

    return pretty({"Configuration changes": output})


def pretty_consumergroup_simple_overview(group: ConsumerGroup) -> str:
    def highlight(x):
        return click.style(str(x), bold=True, fg="green")

    def green_to_red(x):
        return color_code_float(x, [(0.0, "green"), (75.0, "yellow"), (90.0, "bright_red"), (95.0, "red")])

    output = f"""ConsumerGroup {highlight(group.group_id)}
        active members: {highlight(len(group.members))}
        topics: {highlight(group.topics)}
        partitions: {highlight(group.partition_amount)}
        offsets: {highlight(group.offset_overview)}
        relative lag: {green_to_red(100)} %
        total lag: {highlight(group.total_lag)}"""
    return pretty(output)


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


STYLE_MAPPING = {
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
