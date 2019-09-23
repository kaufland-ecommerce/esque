from abc import ABC
from enum import Enum
from collections import namedtuple
import esque.ruleparser.helpers as h


GenericOperator = namedtuple("GenericOperator", "name type regex input_priority stack_priority is_unary")


class OperatorType(Enum):
    ARITHMETIC = (1,)
    COMPARISON = (2,)
    PARENTHESIS = 3


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
        "ADDITION": GenericOperator("ADDITION", OperatorType.ARITHMETIC, "\\+", 20, 20, False),
        "SUBTRACTION": GenericOperator("SUBTRACTION", OperatorType.ARITHMETIC, "-", 20, 20, False),
        "MULTIPLICATION": GenericOperator("MULTIPLICATION", OperatorType.ARITHMETIC, "\\*", 30, 30, False),
        "DIVISION": GenericOperator("DIVISION", OperatorType.ARITHMETIC, "\\/", 30, 30, False),
        "REMAINDER": GenericOperator("REMAINDER", OperatorType.ARITHMETIC, "mod", 20, 20, False),
        "BINARY_AND": GenericOperator("BINARY_AND", OperatorType.ARITHMETIC, "&", 20, 20, False),
        "BINARY_OR": GenericOperator("BINARY_OR", OperatorType.ARITHMETIC, "\\|", 20, 20, False),
        "BINARY_XOR": GenericOperator("BINARY_XOR", OperatorType.ARITHMETIC, "\\^", 20, 20, False),
        "GREATER_THAN": GenericOperator("GREATER_THAN", OperatorType.COMPARISON, ">", 15, 15, False),
        "LESS_THAN": GenericOperator("LESS_THAN", OperatorType.COMPARISON, "<", 15, 15, False),
        "GREATER_OR_EQUAL": GenericOperator("GREATER_OR_EQUAL", OperatorType.COMPARISON, ">=", 15, 15, False),
        "LESS_OR_EQUAL": GenericOperator("LESS_OR_EQUAL", OperatorType.COMPARISON, "<=", 15, 15, False),
        "EQUAL": GenericOperator("EQUAL", OperatorType.COMPARISON, "==", 15, 15, False),
        "NOT_EQUAL": GenericOperator("NOT_EQUAL", OperatorType.COMPARISON, "!=", 15, 15, False),
        "LOGICAL_AND": GenericOperator("LOGICAL_AND", OperatorType.ARITHMETIC, "and", 10, 10, False),
        "LOGICAL_OR": GenericOperator("LOGICAL_OR", OperatorType.ARITHMETIC, "or", 10, 10, False),
        "LOGICAL_NOT": GenericOperator("LOGICAL_NOT", OperatorType.COMPARISON, "neg", 100, 100, True),
        "LOGICAL_XOR": GenericOperator("LOGICAL_XOR", OperatorType.ARITHMETIC, "xor", 10, 10, False),
        "LIKE": GenericOperator("LIKE", OperatorType.COMPARISON, "like", 15, 15, False),
        "NOT_LIKE": GenericOperator("NOT_LIKE", OperatorType.COMPARISON, "notlike", 15, 15, False),
        "PARENTHESIS_OPEN": GenericOperator("PARENTHESIS_OPEN", OperatorType.PARENTHESIS, "\\(", 10000, 0, False),
        "PARENTHESIS_CLOSED": GenericOperator("PARENTHESIS_CLOSED", OperatorType.PARENTHESIS, "\\)", 1, 0, False),
    }


class AbstractBinaryOperator(Operator):
    def evaluate(self, operand1, operand2):
        pass


class AbstractUnaryOperator(Operator):
    def evaluate(self, operand1):
        pass


class ArithmeticBinaryOperator(AbstractBinaryOperator):
    def evaluate(self, operand1: str, operand2: str):
        op1_converted = None
        op2_converted = None
        integer_operands = False
        boolean_operands = False
        if h.is_int(operand1) and h.is_int(operand2):
            op1_converted = int(operand1)
            op2_converted = int(operand2)
            integer_operands = True
        elif (h.is_float(operand1) and h.is_any_numeric_type(operand2)) or (
            h.is_any_numeric_type(operand1) and h.is_float(operand2)
        ):
            op1_converted = float(operand1)
            op2_converted = float(operand2)
        elif h.is_boolean(operand1) and h.is_boolean(operand2):
            op1_converted = h.to_boolean(operand1)
            op2_converted = h.to_boolean(operand2)
            boolean_operands = True
        if op1_converted is None or op2_converted is None:
            raise TypeError(
                "One or two operands ("
                + operand1
                + ", "
                + operand2
                + ") cannot be converted to any recognizable form."
            )
        if self.generic_operator.name == Operator.OPERATORS["ADDITION"].name:
            return op1_converted + op2_converted
        elif self.generic_operator.name == Operator.OPERATORS["SUBTRACTION"].name:
            return op1_converted - op2_converted
        elif self.generic_operator.name == Operator.OPERATORS["MULTIPLICATION"].name:
            return op1_converted * op2_converted
        elif self.generic_operator.name == Operator.OPERATORS["DIVISION"].name:
            if op2_converted == 0:
                raise ValueError("Division by zero")
            return op1_converted / op2_converted
        elif self.generic_operator.name == Operator.OPERATORS["REMAINDER"].name:
            if integer_operands:
                return op1_converted % op2_converted
            else:
                raise TypeError("Remainder can only be used with integer arguments")
        elif self.generic_operator.name == Operator.OPERATORS["BINARY_AND"].name:
            if integer_operands:
                return op1_converted & op2_converted
            else:
                raise TypeError("Binary operators can only be used with integer arguments")
        elif self.generic_operator.name == Operator.OPERATORS["BINARY_OR"].name:
            if integer_operands:
                return op1_converted | op2_converted
            else:
                raise TypeError("Binary operators can only be used with integer arguments")
        elif self.generic_operator.name == Operator.OPERATORS["BINARY_XOR"].name:
            if integer_operands:
                return op1_converted ^ op2_converted
            else:
                raise TypeError("Binary operators can only be used with integer arguments")
        elif self.generic_operator.name == Operator.OPERATORS["LOGICAL_AND"].name:
            if boolean_operands:
                return op1_converted and op2_converted
            else:
                raise TypeError("Logical operators can only be used with boolean values")
        elif self.generic_operator.name == Operator.OPERATORS["LOGICAL_OR"].name:
            if boolean_operands:
                return op1_converted or op2_converted
            else:
                raise TypeError("Logical operators can only be used with boolean values")
        elif self.generic_operator.name == Operator.OPERATORS["LOGICAL_XOR"].name:
            if boolean_operands:
                return op1_converted ^ op2_converted
            else:
                raise TypeError("Logical operators can only be used with boolean values")


class ComparisonBinaryOperator(AbstractBinaryOperator):
    def evaluate(self, operand1: str, operand2: str):
        op1_converted = None
        op2_converted = None
        if h.is_boolean(operand1) and h.is_boolean(operand2):
            op1_converted = h.to_boolean(operand1)
            op2_converted = h.to_boolean(operand2)
        if h.is_float(operand1) and h.is_float(operand2):
            # if they are numeric, it doesn't matter if they are integer or not
            op1_converted = float(operand1)
            op2_converted = float(operand2)
        elif (h.is_date_string(operand1) or h.to_date_time(operand1)) and (
            h.is_date_string(operand2) or h.is_date_time_string(operand2)
        ):
            # maybe they are date strings
            op1_converted = h.to_date_time(operand1)
            op2_converted = h.to_date_time(operand2)
        else:
            # otherwise, we'll test them as strings
            op1_converted = operand1
            op2_converted = operand2
        if op1_converted is None or op2_converted is None:
            raise TypeError("The operands (" + operand1 + ", " + operand2 + ") cannot be compared.")
        if self.generic_operator.name == Operator.OPERATORS["GREATER_THAN"].name:
            return op1_converted > op2_converted
        elif self.generic_operator.name == Operator.OPERATORS["LESS_THAN"].name:
            return op1_converted < op2_converted
        elif self.generic_operator.name == Operator.OPERATORS["GREATER_OR_EQUAL"].name:
            return op1_converted >= op2_converted
        elif self.generic_operator.name == Operator.OPERATORS["LESS_OR_EQUAL"].name:
            return op1_converted <= op2_converted
        elif self.generic_operator.name == Operator.OPERATORS["EQUAL"].name:
            return op1_converted == op2_converted
        elif self.generic_operator.name == Operator.OPERATORS["NOT_EQUAL"].name:
            return op1_converted != op2_converted
        elif self.generic_operator.name == Operator.OPERATORS["LIKE"].name:
            if operand2.startswith("%") and operand2.endswith("%"):
                return operand1.find(operand2.replace("%", "")) != -1
            elif operand2.endswith("%"):
                return operand1.startswith(operand2.replace("%", ""))
            elif operand2.startswith("%"):
                return operand1.endswith(operand2.replace("%", ""))
            else:
                return operand1 == operand2.replace("%", "")
        elif self.generic_operator.name == Operator.OPERATORS["NOT_LIKE"].name:
            if operand2.startswith("%") and operand2.endswith("%"):
                return operand1.find(operand2.replace("%", "")) == -1
            elif operand2.endswith("%"):
                return not operand1.startswith(operand2.replace("%", ""))
            elif operand2.startswith("%"):
                return not operand1.endswith(operand2.replace("%", ""))
            else:
                return operand1 != operand2.replace("%", "")


class ComparisonUnaryOperator(AbstractUnaryOperator):
    def evaluate(self, operand1: str):
        op1_converted = None
        if h.is_boolean(operand1):
            op1_converted = h.to_boolean(operand1)
        if op1_converted is None:
            raise TypeError("The operand (" + operand1 + ") is not a valid boolean value")
        if self.generic_operator.name == Operator.OPERATORS["LOGICAL_NOT"].name:
            return not op1_converted


class ParenthesisOperator(Operator):
    pass


class Literal(ABC):
    def __init__(self, value: str):
        self.value = value.strip()


class Field(ABC):
    def __init__(self, field_name: str):
        self.field_name = field_name.strip()
