import datetime


def zero_or_more(element):
    return "(" + element + ")*"


def one_or_more(element):
    return "(" + element + ")+"


def zero_or_one(element):
    return "(" + element + ")?"


def exactly(element, count):
    return "(" + element + "){" + str(count) + "}"


def either_of(iterable):
    return "((" + ")|(".join(iterable) + "))"


def is_float(input_string: str):
    try:
        float(input_string)
        return True
    except ValueError:
        return False


def is_int(input_string: str):
    try:
        int(input_string)
        return True
    except ValueError:
        return False


def is_any_numeric_type(input_string: str):
    return is_float(input_string) or is_int(input_string)


def is_date_string(input_string: str):
    try:
        datetime.datetime.strptime(input_string, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def is_date_time_string(input_string: str):
    try:
        datetime.datetime.strptime(input_string, "%Y-%m-%dT%H:%M:%S")
        return True
    except ValueError:
        return False


def is_boolean(input_string: str):
    return to_boolean(input_string) is not None


def to_boolean(input_string: str):
    """Returns True, False or None, based on the contents of the input_string (any form of capitalization is allowed)"""
    if input_string.lower() == "true":
        return True
    elif input_string.lower() == "false":
        return False
    else:
        return None


def string_like(haystack: str, needle: str):
    if needle.startswith("%") and needle.endswith("%"):
        return haystack.find(needle.replace("%", "")) != -1
    elif needle.startswith("%"):
        return haystack.endswith(needle.replace("%", ""))
    elif needle.endswith("%"):
        return haystack.startswith(needle.replace("%", ""))
    else:
        return haystack == needle.replace("%", "")


def string_not_like(haystack: str, needle: str):
    return not string_like(haystack, needle)
