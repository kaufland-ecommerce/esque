import re
from functools import wraps

from yamale.util import isstr
from yamale.validators import Boolean, DefaultValidators, Enum, Integer, Number, Validator


def all_validators() -> dict:
    validators = DefaultValidators.copy()
    validators[StringBool.tag] = StringBool
    validators[StringInt.tag] = StringInt
    validators[StringFloat.tag] = StringFloat
    validators[ReplicaList.tag] = ReplicaList
    validators[StringEnum.tag] = StringEnum

    return validators


def catch_value_errors(validate):
    """method decorator to catch ValueErrors for casts and return an error"""

    @wraps(validate)
    def wrapper(self, value):
        try:
            return validate(self, value)
        except ValueError:
            return [f"'{value}' could not be casted to {self.tag[2:]}"]

    return wrapper


class StringBool(Boolean):
    tag = "s_bool"

    @catch_value_errors
    def validate(self, value):
        value = str(value)
        if value.lower() not in ["false", "true"]:
            raise ValueError
        return super().validate(bool(value))


class StringInt(Integer):
    tag = "s_int"

    @catch_value_errors
    def validate(self, value):
        return super().validate(int(value))


class StringFloat(Number):
    tag = "s_float"

    @catch_value_errors
    def validate(self, value):
        return super().validate(float(value))


class StringEnum(Enum):
    tag = "s_enum"

    def __init__(self, *args, case_sensitive: bool = True, **kwargs):
        if not case_sensitive:
            args = [arg.lower() for arg in args]
        super().__init__(*args, **kwargs)

    @catch_value_errors
    def validate(self, value):
        if not isinstance(value, str):
            raise TypeError(f"Value {value} has to be a string, but is {type(value).__name__}")
        return super().validate(value.lower())


class ReplicaList(Validator):
    """
    Validates a list of replicas in the form of '<broker_id>:<partition>' (e.g. `'0:0,1:1,2:2'`).
    Empty string for empty list or '*' for all replicas area also valid values.
    """

    tag = "replica_list"

    def _is_valid(self, value) -> bool:
        if not isstr(value):
            return False
        if value == "" or value == "*":
            return True
        for pair in value.split(","):
            if not re.fullmatch(r"\d+:\d+", pair):
                return False
        return True

    def fail(self, value):
        return f"could not build dict from this: {value}"
