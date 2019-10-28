import re
from yamale.validators import Boolean, DefaultValidators, Integer, Number, Validator
from yamale.util import isstr


def all_validators() -> dict:
    validators = DefaultValidators.copy()
    validators[StringBool.tag] = StringBool
    validators[StringInt.tag] = StringInt
    validators[StringFloat.tag] = StringFloat
    validators[StringDictionary.tag] = StringDictionary

    return validators


def catch_value_errors(validate):
    """method decorator to catch ValueErrors for casts and return an error"""

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


class StringDictionary(Validator):
    """ validates colon seperated key/value pairs chained with commas, also emptystring ('') e.g.: '0:0,1:1,2:2' """

    tag = "s_dict"

    def _is_valid(self, value) -> bool:
        if not isstr(value):
            return False
        if value == "":
            return True
        for pair in value.split(","):
            if not re.fullmatch(r"\d+:\d+", pair):
                return False
        return True

    def fail(self, value):
        return f"could not build dict from this: {value}"
