from abc import ABC
from enum import Enum
from typing import Callable, List, NamedTuple

import pendulum

import esque.ruleparser.helpers as h


class OperatorType(Enum):
    ARITHMETIC = (1,)
    COMPARISON = (2,)
    PARENTHESIS = 3


class OperandType(Enum):
    NUMERIC = (1,)
    NUMERIC_INT = (2,)
    BOOLEAN = (3,)
    DATETIME = (4,)
    STRING = 5


class GenericOperator(NamedTuple):
    name: str
    type: OperatorType
    regex: str
    input_priority: int
    stack_priority: int
    is_unary: bool
    calculation_function: Callable
    operand_types: List[OperandType]


class Operator(ABC):
    generic_operator: GenericOperator = None
    literal: str = ""

    def __init__(self, literal: str, generic_operator: GenericOperator):
        self.literal = literal.replace("\\", "")
        self.generic_operator = generic_operator

    FIELDS = {
        "MESSAGE_KEY": "message\\.key",
        "MESSAGE_LENGTH": "message\\.length",
        "MESSAGE_PARTITION": "message\\.partition",
        "MESSAGE_HEADER": "message\\.header\\.[a-zA-Z0-9_]+",
        "MESSAGE_OFFSET": "message\\.offset",
        "MESSAGE_TIMESTAMP": "message\\.timestamp",
        "SYSTEM_TIMESTAMP": "system\\.timestamp",
    }
    OPERATORS = {
        "ADDITION": GenericOperator(
            "ADDITION", OperatorType.ARITHMETIC, "\\+", 20, 20, False, (lambda o1, o2: o1 + o2), [OperandType.NUMERIC]
        ),
        "SUBTRACTION": GenericOperator(
            "SUBTRACTION", OperatorType.ARITHMETIC, "-", 20, 20, False, (lambda o1, o2: o1 - o2), [OperandType.NUMERIC]
        ),
        "MULTIPLICATION": GenericOperator(
            "MULTIPLICATION",
            OperatorType.ARITHMETIC,
            "\\*",
            30,
            30,
            False,
            (lambda o1, o2: o1 * o2),
            [OperandType.NUMERIC],
        ),
        "DIVISION": GenericOperator(
            "DIVISION", OperatorType.ARITHMETIC, "\\/", 30, 30, False, (lambda o1, o2: o1 / o2), [OperandType.NUMERIC]
        ),
        "REMAINDER": GenericOperator(
            "REMAINDER",
            OperatorType.ARITHMETIC,
            "mod",
            20,
            20,
            False,
            (lambda o1, o2: o1 % o2),
            [OperandType.NUMERIC_INT],
        ),
        "BINARY_AND": GenericOperator(
            "BINARY_AND",
            OperatorType.ARITHMETIC,
            "&",
            20,
            20,
            False,
            (lambda o1, o2: o1 & o2),
            [OperandType.NUMERIC_INT],
        ),
        "BINARY_OR": GenericOperator(
            "BINARY_OR",
            OperatorType.ARITHMETIC,
            "\\|",
            20,
            20,
            False,
            (lambda o1, o2: o1 | o2),
            [OperandType.NUMERIC_INT],
        ),
        "BINARY_XOR": GenericOperator(
            "BINARY_XOR",
            OperatorType.ARITHMETIC,
            "\\^",
            20,
            20,
            False,
            (lambda o1, o2: o1 ^ o2),
            [OperandType.NUMERIC_INT],
        ),
        "GREATER_THAN": GenericOperator(
            "GREATER_THAN",
            OperatorType.COMPARISON,
            ">",
            15,
            15,
            False,
            (lambda o1, o2: o1 > o2),
            [OperandType.NUMERIC, OperandType.DATETIME],
        ),
        "LESS_THAN": GenericOperator(
            "LESS_THAN",
            OperatorType.COMPARISON,
            "<",
            15,
            15,
            False,
            (lambda o1, o2: o1 < o2),
            [OperandType.NUMERIC, OperandType.DATETIME],
        ),
        "GREATER_OR_EQUAL": GenericOperator(
            "GREATER_OR_EQUAL",
            OperatorType.COMPARISON,
            ">=",
            15,
            15,
            False,
            (lambda o1, o2: o1 >= o2),
            [OperandType.NUMERIC, OperandType.DATETIME],
        ),
        "LESS_OR_EQUAL": GenericOperator(
            "LESS_OR_EQUAL",
            OperatorType.COMPARISON,
            "<=",
            15,
            15,
            False,
            (lambda o1, o2: o1 <= o2),
            [OperandType.NUMERIC, OperandType.DATETIME],
        ),
        "EQUAL": GenericOperator(
            "EQUAL",
            OperatorType.COMPARISON,
            "==",
            15,
            15,
            False,
            (lambda o1, o2: o1 == o2),
            [OperandType.NUMERIC, OperandType.DATETIME, OperandType.BOOLEAN, OperandType.STRING],
        ),
        "NOT_EQUAL": GenericOperator(
            "NOT_EQUAL",
            OperatorType.COMPARISON,
            "!=",
            15,
            15,
            False,
            (lambda o1, o2: o1 != o2),
            [OperandType.NUMERIC, OperandType.DATETIME, OperandType.BOOLEAN, OperandType.STRING],
        ),
        "LOGICAL_AND": GenericOperator(
            "LOGICAL_AND",
            OperatorType.ARITHMETIC,
            "and",
            10,
            10,
            False,
            (lambda o1, o2: o1 and o2),
            [OperandType.BOOLEAN],
        ),
        "LOGICAL_OR": GenericOperator(
            "LOGICAL_OR",
            OperatorType.ARITHMETIC,
            "or",
            10,
            10,
            False,
            (lambda o1, o2: o1 or o2),
            [OperandType.BOOLEAN],
        ),
        "LOGICAL_NOT": GenericOperator(
            "LOGICAL_NOT", OperatorType.COMPARISON, "neg", 100, 100, True, (lambda o1: not o1), [OperandType.BOOLEAN]
        ),
        "LOGICAL_XOR": GenericOperator(
            "LOGICAL_XOR",
            OperatorType.ARITHMETIC,
            "xor",
            10,
            10,
            False,
            (lambda o1, o2: (o1 and not o2) or (not o1 and o2)),
            [OperandType.BOOLEAN],
        ),
        "LIKE": GenericOperator(
            "LIKE",
            OperatorType.COMPARISON,
            "like",
            15,
            15,
            False,
            (lambda o1, o2: h.string_like(haystack=o1, needle=o2)),
            [OperandType.STRING],
        ),
        "NOT_LIKE": GenericOperator(
            "NOT_LIKE",
            OperatorType.COMPARISON,
            "notlike",
            15,
            15,
            False,
            (lambda o1, o2: h.string_not_like(haystack=o1, needle=o2)),
            [OperandType.STRING],
        ),
        "PARENTHESIS_OPEN": GenericOperator(
            "PARENTHESIS_OPEN", OperatorType.PARENTHESIS, "\\(", 10000, 0, False, None, None
        ),
        "PARENTHESIS_CLOSED": GenericOperator(
            "PARENTHESIS_CLOSED", OperatorType.PARENTHESIS, "\\)", 1, 0, False, None, None
        ),
    }


class AbstractBinaryOperator(Operator):
    def evaluate(self, operand1, operand2):
        pass

    def validate_operands(self, operand1, operand2):
        for operand_type in self.generic_operator.operand_types:
            if (
                (operand_type == OperandType.NUMERIC_INT or operand_type == OperandType.NUMERIC)
                and h.is_int(operand1)
                and h.is_int(operand2)
            ):
                return [int(operand1), int(operand2)]
            elif operand_type == OperandType.NUMERIC and h.is_float(operand1) and h.is_float(operand2):
                return [float(operand1), float(operand2)]
            elif (
                operand_type == OperandType.DATETIME
                and (h.is_date_time_string(operand1) or h.is_date_string(operand1))
                and (h.is_date_time_string(operand2) or h.is_date_string(operand2))
            ):
                return [pendulum.parse(operand1), pendulum.parse(operand2)]
            elif operand_type == OperandType.BOOLEAN and h.is_boolean(operand1) and h.is_boolean(operand2):
                return [h.to_boolean(operand1), h.to_boolean(operand2)]
            elif operand_type == OperandType.STRING:
                return [operand1, operand2]
        raise ValueError(
            "One or both operands ({}, {}) are not applicable to this operator ({})".format(
                operand1, operand2, self.literal
            )
        )


class AbstractUnaryOperator(Operator):
    def evaluate(self, operand1):
        pass

    def validate_operands(self, operand1):
        for operand_type in self.generic_operator.operand_types:
            if (operand_type == OperandType.NUMERIC_INT or operand_type == OperandType.NUMERIC) and h.is_int(operand1):
                return [int(operand1)]
            elif operand_type == OperandType.NUMERIC and h.is_float(operand1):
                return [float(operand1)]
            elif operand_type == OperandType.DATETIME and (
                h.is_date_time_string(operand1) or h.is_date_string(operand1)
            ):
                return [pendulum.parse(operand1)]
            elif operand_type == OperandType.BOOLEAN and h.is_boolean(operand1):
                return [h.to_boolean(operand1)]
            elif operand_type == OperandType.STRING:
                return [operand1]
        raise ValueError("Operand ({}) is not applicable to this operator ({})".format(operand1, self.literal))


class ArithmeticBinaryOperator(AbstractBinaryOperator):
    def evaluate(self, operand1: str, operand2: str):
        try:
            [op1_converted, op2_converted] = self.validate_operands(operand1, operand2)
            return self.generic_operator.calculation_function(op1_converted, op2_converted)
        except ValueError:
            raise


class ComparisonBinaryOperator(AbstractBinaryOperator):
    def evaluate(self, operand1: str, operand2: str):
        try:
            [op1_converted, op2_converted] = self.validate_operands(operand1, operand2)
            return self.generic_operator.calculation_function(op1_converted, op2_converted)
        except ValueError:
            raise


class ComparisonUnaryOperator(AbstractUnaryOperator):
    def evaluate(self, operand1: str):
        try:
            [op1_converted] = self.validate_operands(operand1)
            return self.generic_operator.calculation_function(op1_converted)
        except ValueError:
            raise


class ParenthesisOperator(Operator):
    pass


class Literal(ABC):
    def __init__(self, value: str):
        self.value = value.strip()


class Field(ABC):
    def __init__(self, field_name: str):
        self.field_name = field_name.strip()
