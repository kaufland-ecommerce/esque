import re
from abc import ABC

import esque.ruleparser.exception as ex
import esque.ruleparser.helpers as h
from esque.ruleparser.exception import ExpressionSyntaxError, MalformedExpressionError, RuleTreeInvalidError
from esque.ruleparser.expressionelement import (
    ArithmeticBinaryOperator,
    ComparisonBinaryOperator,
    ComparisonUnaryOperator,
    Field,
    GenericOperator,
    Literal,
    Operator,
    OperatorType,
    ParenthesisOperator,
)
from esque.ruleparser.fieldeval import FieldEval


class RuleTree:
    __postfix_expression = []
    __infix_expression = []
    __tree_valid = False
    __expression_string = ""

    def __init__(self, expression_string: str):
        self.__infix_expression = []
        self.__postfix_expression = []
        self.__tree_valid = False
        self.__expression_string = expression_string
        parser = ExpressionParser(self.__expression_string)
        self.__infix_expression = parser.parse_expression()
        self.__convert_infix_to_postfix()

    def __convert_infix_to_postfix(self):
        """
        Responsible for converting the infix expression notation (i.e. a+b-c) to postfix expression notation (i.e. ab+c-).
        :return: No return value.
        """
        operator_stack = []
        parenthesis_balance_counter = 0
        rank = 0
        element: ABC
        for element in self.__infix_expression:
            if isinstance(element, ParenthesisOperator):
                operator: Operator = element
                if operator.literal == "(":
                    operator_stack.append(operator)
                    parenthesis_balance_counter = parenthesis_balance_counter + 1
                else:
                    try:
                        while operator_stack[-1].literal != "(":
                            self.__postfix_expression.append(operator_stack.pop())
                        operator_stack.pop()
                        parenthesis_balance_counter = parenthesis_balance_counter - 1
                    except IndexError:
                        raise ExpressionSyntaxError("Malformed parentheses in pattern")
            elif isinstance(element, Operator):
                operator: Operator = element
                while (
                    len(operator_stack) > 0
                    and operator_stack[-1].generic_operator.stack_priority >= operator.generic_operator.input_priority
                ):
                    self.__postfix_expression.append(operator_stack.pop())
                    rank = rank - 1
                if rank < 0:
                    raise ExpressionSyntaxError("Malformed pattern at element " + operator.literal)
                operator_stack.append(operator)
            else:
                self.__postfix_expression.append(element)
                rank = rank + 1
        while len(operator_stack) > 0:
            if operator_stack[-1].generic_operator.type != OperatorType.PARENTHESIS:
                self.__postfix_expression.append(operator_stack.pop())
            elif operator_stack[-1].literal == "(":
                parenthesis_balance_counter = parenthesis_balance_counter + 1
            elif operator_stack[-1].literal == ")":
                parenthesis_balance_counter = parenthesis_balance_counter - 1
        if parenthesis_balance_counter != 0:
            raise ExpressionSyntaxError("Malformed parentheses in pattern")
        self.__tree_valid = True

    def evaluate(self, message):
        """
        Evaluates the parsed expression against a Kafka message (if required).
        :param message: The Kafka message used for evaluation. If the evaluated expression contains no references to the message, this parameter can be set to None.
        :return:
        The final value. If any problems are encountered, this method raises a MalformedExpressionError.
        """
        operand_stack = []
        field_evaluator = FieldEval(message)
        if not self.__tree_valid:
            raise RuleTreeInvalidError(
                "The rule tree is not valid. The expression must be parsed before evaluating the tree."
            )
        for tree_element in self.__postfix_expression:
            if isinstance(tree_element, ArithmeticBinaryOperator) or isinstance(
                tree_element, ComparisonBinaryOperator
            ):
                # we need to have 2 operands on stack at this point, otherwise the expression is malformed
                if len(operand_stack) < 2:
                    raise MalformedExpressionError("Operator " + tree_element.literal + " requires 2 operands.")
                evaluation_result = tree_element.evaluate(str(operand_stack[-2]), str(operand_stack[-1]))
                operand_stack.pop()
                operand_stack.pop()
                operand_stack.append(evaluation_result)
            elif isinstance(tree_element, ComparisonUnaryOperator):
                # we need to have 1 operand on stack at this point, otherwise the expression is malformed
                if len(operand_stack) < 1:
                    raise MalformedExpressionError("Operator " + tree_element.literal + " requires 1 operand.")
                evaluation_result = tree_element.evaluate(str(operand_stack[-1]))
                operand_stack.pop()
                operand_stack.append(evaluation_result)
            elif isinstance(tree_element, Literal):
                operand_stack.append(tree_element.value)
            elif isinstance(tree_element, Field):
                operand_stack.append(field_evaluator.evaluate_field(tree_element.field_name))
        # in the end, if we have only one element on the stack, that is our result
        # otherwise, the expression is malformed
        if len(operand_stack) == 1:
            return operand_stack[0]
        else:
            raise MalformedExpressionError("The expression is malformed.")


class ExpressionParser:

    RE_SPACE: str = h.zero_or_more('[" "]|\t')
    RE_LITERALS = [
        "[1-9][0-9]{3}(-([0-9]{2})){2}T([0-9]{2})(:([0-9]{2})){2}",  # DATE TIME STAMP: YYYY-MM-DDTHH:mm:ss
        "[1-9][0-9]{3}(-([0-9]{2})){2}",  # DATESTAMP: YYYY-MM-DD
        "[-+]?[1-9]+[0-9]*",  # INTEGER
        "[+-]?[0-9]+.[0-9]*[eE]*[+-]?[0-9]*",  # FLOATING POINT
        "(true|false)",  # BOOLEAN
        "[a-zA-Z0-9\\-\\._\\%]+",  # STRING
    ]

    expression: str = ""
    arithmetic_operators_pattern = None
    comparison_operators_pattern = None
    parenthesis_pattern = None
    fields_pattern = None
    literals_pattern = None

    def __init__(self, expression: str = ""):
        self.expression = expression.strip()
        self.arithmetic_operators_pattern = re.compile(
            ExpressionParser.RE_SPACE
            + h.either_of(
                list(
                    map(
                        lambda x: x.regex,
                        filter(lambda y: y.type == OperatorType.ARITHMETIC, Operator.OPERATORS.values()),
                    )
                )
            ),
            flags=re.IGNORECASE | re.UNICODE,
        )
        self.comparison_operators_pattern = re.compile(
            ExpressionParser.RE_SPACE
            + h.either_of(
                list(
                    map(
                        lambda x: x.regex,
                        filter(lambda y: y.type == OperatorType.COMPARISON, Operator.OPERATORS.values()),
                    )
                )
            ),
            flags=re.IGNORECASE | re.UNICODE,
        )
        self.parenthesis_pattern = re.compile(
            ExpressionParser.RE_SPACE
            + h.either_of(
                list(
                    map(
                        lambda x: x.regex,
                        filter(lambda y: y.type == OperatorType.PARENTHESIS, Operator.OPERATORS.values()),
                    )
                )
            ),
            flags=re.IGNORECASE | re.UNICODE,
        )
        self.fields_pattern = re.compile(
            ExpressionParser.RE_SPACE + h.either_of(Operator.FIELDS.values()), flags=re.IGNORECASE | re.UNICODE
        )
        self.literals_pattern = re.compile(
            ExpressionParser.RE_SPACE + h.either_of(ExpressionParser.RE_LITERALS), flags=re.IGNORECASE
        )

    def parse_expression(self):
        """
        Parses the input expression and returns a list of discovered elements.
        :return: The list of discovered (parsed elements). The list is populated in the order the elements are found (i.e. "infix" notation, a+b+c+d)
        """
        parsed_elements = []
        if self.expression.strip() == "":
            raise ex.EmptyExpressionError("The expression string contains no expression.")
        previous_position = 0
        match_end = 0

        while previous_position < len(self.expression):
            match_arithmetic_operator = self.arithmetic_operators_pattern.match(
                string=self.expression, pos=previous_position
            )
            match_comparison_operator = self.comparison_operators_pattern.match(
                string=self.expression, pos=previous_position
            )
            match_parenthesis_operator = self.parenthesis_pattern.match(string=self.expression, pos=previous_position)
            match_field = self.fields_pattern.match(string=self.expression, pos=previous_position)
            match_literal = self.literals_pattern.match(string=self.expression, pos=previous_position)
            match_found = False
            if match_arithmetic_operator is not None:
                if match_arithmetic_operator.start(0) == previous_position:
                    match_found = True
                    match_end = match_arithmetic_operator.end(0)
                    content = match_arithmetic_operator.group(0).strip()
                    generic_operator: GenericOperator = list(
                        filter(lambda x: x.regex.replace("\\", "") == content, Operator.OPERATORS.values())
                    )[0]
                    parsed_elements.append(
                        ArithmeticBinaryOperator(literal=content, generic_operator=generic_operator)
                    )
            if not match_found and match_comparison_operator is not None:
                if match_comparison_operator.start(0) == previous_position:
                    match_found = True
                    match_end = match_comparison_operator.end(0)
                    content = match_comparison_operator.group(0).strip()
                    generic_operator: GenericOperator = list(
                        filter(lambda x: x.regex.replace("\\", "") == content, Operator.OPERATORS.values())
                    )[0]
                    if generic_operator.is_unary:
                        parsed_elements.append(
                            ComparisonUnaryOperator(literal=content, generic_operator=generic_operator)
                        )
                    else:
                        parsed_elements.append(
                            ComparisonBinaryOperator(literal=content, generic_operator=generic_operator)
                        )
            if not match_found and match_parenthesis_operator is not None:
                if match_parenthesis_operator.start(0) == previous_position:
                    match_found = True
                    match_end = match_parenthesis_operator.end(0)
                    content = match_parenthesis_operator.group(0).strip()
                    generic_operator: GenericOperator = list(
                        filter(lambda x: x.regex.replace("\\", "") == content, Operator.OPERATORS.values())
                    )[0]
                    parsed_elements.append(ParenthesisOperator(literal=content, generic_operator=generic_operator))
            if not match_found and match_field is not None:
                if match_field.start(0) == previous_position:
                    match_found = True
                    match_end = match_field.end(0)
                    content = match_field.group(0).strip()
                    parsed_elements.append(Field(field_name=content))
            if not match_found and match_literal is not None:
                if match_literal.start(0) == previous_position:
                    match_found = True
                    match_end = match_literal.end(0)
                    content = match_literal.group(0).strip()
                    parsed_elements.append(Literal(value=content))
            if not match_found:
                raise ex.ExpressionSyntaxError("Unrecognized symbols at position: " + str(previous_position))
            else:
                previous_position = match_end
        return parsed_elements
