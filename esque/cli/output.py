from collections import OrderedDict
from functools import partial
from typing import Any, List, MutableMapping, Dict, Tuple

import click
import pendulum

from esque.topic import Topic

C_MAX_INT = 2 ** 31 - 1


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
        sub_elements = (
            "\n  ".join(elem.splitlines(keepends=False)) for elem in list_output
        )
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


def pretty_duration(orig_value: Any, *, multiplier: int = 1) -> str:
    if not orig_value:
        return ""

    if type(orig_value) != int:
        value = int(orig_value)
    else:
        value = orig_value

    value *= multiplier

    # Fix for conversion errors of ms > C_MAX_INT in some internal lib
    if value > C_MAX_INT:
        value = int(value / 1000 / 3600 / 24 / 365)
        return f"{orig_value} ({pendulum.duration(years=value).in_words()})"

    return f"{orig_value} ({pendulum.duration(milliseconds=value).in_words()})"


def get_output_topic_diffs(
    topics_config_diff: Dict[str, Dict[str, Tuple[str, str]]]
) -> str:
    output = []
    for name, diff in topics_config_diff.items():
        config_diff_attributes = {}
        for attribute, value in diff.items():
            config_diff_attributes[attribute] = value[0] + " -> " + value[1]
        output.append(
            {
                click.style(name, bold=True, fg="yellow"): {
                    "Config Diff": config_diff_attributes
                }
            }
        )

    return pretty({"Topics to change": output})


def get_output_new_topics(new_topics: List[Topic]) -> str:
    new_topic_configs = []
    for topic in new_topics:
        new_topic_config = {
            "num_partitions: ": topic.num_partitions,
            "replication_factor: ": topic.replication_factor,
            "config": topic.config,
        }
        new_topic_configs.append(
            {click.style(topic.name, bold=True, fg="green"): new_topic_config}
        )

    return pretty({"New topics to create": new_topic_configs})


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
            return f"{value} ({pretty_float(value/size)} {sign})"


def bold(s: str) -> str:
    return click.style(s, bold=True)


def blue_bold(s: str) -> str:
    return click.style(s, fg="blue", bold=True)


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
