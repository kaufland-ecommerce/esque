class NonNumericOperandError(TypeError):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class EmptyExpressionError(ValueError):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class ExpressionSyntaxError(ValueError):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class MalformedExpressionError(ValueError):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class RuleTreeInvalidError(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)
