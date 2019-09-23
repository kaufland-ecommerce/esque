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
        datetime.datetime.strptime(input_string, "%Y-%m-%dT%H%M%S")
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


def to_date_time(input_string: str):
    """This method will try to convert the string into a datetime object even if it is shorter than necessary.
        If the string is missing the time portion, it will be set to 00:00:00. Likewise, if it missing the seconds value (:00), it will be set to 0.
        The full format of the date time string is YYYY-MM-DDTHH:MM:SS, but the function tolerates values with less than the required number of digits.
    """
    try:
        main_segments = input_string.split(sep="T")
        result = datetime.datetime.now()
        if len(main_segments) == 0:
            return None
        else:
            date_segments = main_segments[0].split("-")
            if len(date_segments) < 3:
                return None
            else:
                year = int(date_segments[0])
                if year < 100:
                    result = result.replace(year=year + 2000)
                else:
                    result = result.replace(year=year)
                result = result.replace(month=int(date_segments[1]), day=int(date_segments[2]))
            if len(main_segments) == 2:
                time_segments = main_segments[1].split(":")
                try:
                    result = result.replace(hour=int(time_segments[0]))
                except ValueError:
                    return None
                if len(time_segments) > 1:
                    try:
                        result = result.replace(minute=int(time_segments[1]))
                    except ValueError:
                        return None
                else:
                    result = result.replace(minute=0)
                if len(time_segments) > 2:
                    try:
                        result = result.replace(second=int(time_segments[2]))
                    except ValueError:
                        return None
                else:
                    result = result.replace(second=0)
            else:
                result = result.replace(hour=0, minute=0, second=0)
    except:
        raise ValueError("Datetime string is malformed.")
    return result
