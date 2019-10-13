from unittest import TestCase

from esque.ruleparser.expressionelement import ArithmeticBinaryOperator
from esque.ruleparser.expressionelement import ComparisonUnaryOperator
from esque.ruleparser.expressionelement import Operator
from esque.ruleparser.ruleengine import RuleTree


class OperatorCalculationsTest(TestCase):
    def test_addition(self):
        operator = ArithmeticBinaryOperator(literal="+", generic_operator=Operator.OPERATORS["ADDITION"])
        self.assertEqual(operator.evaluate("5", "3"), 8)
        self.assertAlmostEqual(operator.evaluate("5", "3.1"), 8.1)
        self.assertAlmostEqual(operator.evaluate("5.1", "3"), 8.1)
        self.assertAlmostEqual(operator.evaluate("5.1", "3.2"), 8.3)
        with self.assertRaises(ValueError):
            operator.evaluate("a", "3")

    def test_subtraction(self):
        operator = ArithmeticBinaryOperator(literal="-", generic_operator=Operator.OPERATORS["SUBTRACTION"])
        self.assertEqual(operator.evaluate("5", "3"), 2)
        self.assertAlmostEqual(operator.evaluate("5", "3.1"), 1.9)
        self.assertAlmostEqual(operator.evaluate("5.1", "3"), 2.1)
        self.assertAlmostEqual(operator.evaluate("5.1", "3.2"), 1.9)
        with self.assertRaises(ValueError):
            operator.evaluate("a", "3")

    def test_division(self):
        operator = ArithmeticBinaryOperator(literal="/", generic_operator=Operator.OPERATORS["DIVISION"])
        self.assertEqual(operator.evaluate("5", "2"), 2.5)
        self.assertAlmostEqual(operator.evaluate("2", "4"), 0.5)
        with self.assertRaises(ZeroDivisionError):
            operator.evaluate("5", "0")

    def test_negation(self):
        operator = ComparisonUnaryOperator(literal="neg", generic_operator=Operator.OPERATORS["LOGICAL_NOT"])
        self.assertFalse(operator.evaluate("True"))

    def test_remainder(self):
        operator = ArithmeticBinaryOperator(literal="mod", generic_operator=Operator.OPERATORS["REMAINDER"])
        self.assertEqual(operator.evaluate(5, 2), 1)


class ExpressionTests(TestCase):
    def test1(self):
        rule_tree = RuleTree("5 +  7>2")
        self.assertTrue(rule_tree.evaluate(message=None))

    def test2(self):
        rule_tree = RuleTree("5 +  7<2")
        self.assertFalse(rule_tree.evaluate(message=None))

    def test3(self):
        rule_tree = RuleTree("(5 +  7)*3-(1/2)")
        self.assertEqual(rule_tree.evaluate(message=None), 35.5)

    def test4(self):
        rule_tree = RuleTree("(320 + 5)*2-(1-2) < 1")
        self.assertFalse(rule_tree.evaluate(message=None))

    def test5(self):
        rule_tree = RuleTree("(320 + 5)*2-(1-2) > 1")
        self.assertTrue(rule_tree.evaluate(message=None))

    def test6(self):
        rule_tree = RuleTree("system.timestamp < 2018-10-08")
        self.assertFalse(rule_tree.evaluate(message=None))

    def test7(self):
        rule_tree = RuleTree("(320 + 5)*2-(1-2) < 1 or system.timestamp < 2018-10-08")
        self.assertFalse(rule_tree.evaluate(message=None))

    def test8(self):
        rule_tree = RuleTree("(320 + 5)*2-(1-2) < 1 or system.timestamp > 2018-10-08")
        self.assertTrue(rule_tree.evaluate(message=None))

    def test9(self):
        rule_tree = RuleTree("neg(5>9)")
        self.assertTrue(rule_tree.evaluate(message=None))

    def test10(self):
        rule_tree = RuleTree("prefixstr like pre%")
        self.assertTrue(rule_tree.evaluate(message=None))

    def test11(self):
        rule_tree = RuleTree("prefixstr like %pre")
        self.assertFalse(rule_tree.evaluate(message=None))

    def test12(self):
        rule_tree = RuleTree("prefixstr like %efi%")
        self.assertTrue(rule_tree.evaluate(message=None))

    def test13(self):
        rule_tree = RuleTree("prefixstr notlike pre%")
        self.assertFalse(rule_tree.evaluate(message=None))

    def test14(self):
        rule_tree = RuleTree("315 +1 mod 9")
        self.assertEqual(rule_tree.evaluate(message=None), 1)

    def test15(self):
        rule_tree = RuleTree("system.timestamp > 2019-08-08T20:21:22")
        self.assertTrue(rule_tree.evaluate(message=None))
