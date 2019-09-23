from unittest import TestCase

import esque.ruleparser.helpers as h


class HelperDateFunctionsTest(TestCase):

    def test_only_date(self):
        parsed_date = h.to_date_time("2010-09-08")
        self.assertEqual(parsed_date.month, 9)
        self.assertEqual(parsed_date.day, 8)
        self.assertEqual(parsed_date.hour, 0)

    def test_date_with_hour(self):
        parsed_date = h.to_date_time("2010-09-08T16")
        self.assertEqual(parsed_date.month, 9)
        self.assertEqual(parsed_date.day, 8)
        self.assertEqual(parsed_date.hour, 16)

    def test_date_without_seconds(self):
        parsed_date = h.to_date_time("2010-09-08T22:30")
        self.assertEqual(parsed_date.month, 9)
        self.assertEqual(parsed_date.day, 8)
        self.assertEqual(parsed_date.hour, 22)
        self.assertEqual(parsed_date.minute, 30)

    def test_date_full(self):
        parsed_date = h.to_date_time("2010-09-08T22:30:12")
        self.assertEqual(parsed_date.month, 9)
        self.assertEqual(parsed_date.day, 8)
        self.assertEqual(parsed_date.hour, 22)
        self.assertEqual(parsed_date.minute, 30)
        self.assertEqual(parsed_date.second, 12)
