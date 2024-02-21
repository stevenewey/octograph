from datetime import date, datetime
from unittest import TestCase, main

import pytz
from parameterized import parameterized

from app.date_utils import DateUtils
from app.eco_unit_normaliser import EcoUnitNormaliser

LONDON_TZ = pytz.timezone('Europe/London')


class TestNormalise(TestCase):

    @parameterized.expand([2, 8])
    def test_night_only(self, month):
        low_start = 2
        low_end = 9
        normaliser = EcoUnitNormaliser(LONDON_TZ, low_start, low_end)
        input_day_rates = [{
            "valid_from": DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month - 1, 15, 0, 0, 0, 0))),
            "valid_to": DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 15, 0, 0, 0, 0))),
            "rate": 10,
        }]
        input_night_rates = [{
            "valid_from": DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, 1, 15, 0, 0, 0, 0))),
            "valid_to": DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 15, 0, 0, 0, 0))),
            "rate": 5,
        }]
        expected = [{
            "valid_from": DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, 0, 0, 0, 0))),
            "valid_to": DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, low_start, 0, 0, 0))),
            "rate": 10,
        }, {
            "valid_from": DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, low_end, 0, 0, 0))),
            "valid_to": DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 2, low_start, 0, 0, 0))),
            "rate": 10,
        }, {
            "valid_from": DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 2, low_end, 0, 0, 0))),
            "valid_to": DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 3, low_start, 0, 0, 0))),
            "rate": 10,
        }, {
            "valid_from": DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 3, low_end, 0, 0, 0))),
            "valid_to": DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 4, 0, 0, 0, 0))),
            "rate": 10,
        }, {
            "valid_from": DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, low_start, 0, 0, 0))),
            "valid_to": DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, low_end, 0, 0, 0))),
            "rate": 5,
        }, {
            "valid_from": DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 2, low_start, 0, 0, 0))),
            "valid_to": DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 2, low_end, 0, 0, 0))),
            "rate": 5,
        }, {
            "valid_from": DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 3, low_start, 0, 0, 0))),
            "valid_to": DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 3, low_end, 0, 0, 0))),
            "rate": 5,
        }]
        result = normaliser.normalise(input_day_rates, input_night_rates, date(2023, month, 1), date(2023, month, 3))
        for obj in expected:
            with self.subTest(obj=obj):
                self.assertIn(obj, result, f"Object {obj} not found in the expected rows.")
        self.assertEqual(len(expected), len(result), "Arrays have different lengths.")

    @parameterized.expand([2, 8])
    def test_from_midnight(self, month):
        low_start = 0
        low_end = 7
        normaliser = EcoUnitNormaliser(LONDON_TZ, low_start, low_end)
        input_day_rates = [{
            "valid_from": DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month - 1, 15, 0, 0, 0, 0))),
            "valid_to": DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 15, 0, 0, 0, 0))),
            "rate": 10,
        }]
        input_night_rates = [{
            "valid_from": DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, 1, 15, 0, 0, 0, 0))),
            "valid_to": DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 15, 0, 0, 0, 0))),
            "rate": 5,
        }]
        expected = [{
            "valid_from": DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, low_end, 0, 0, 0))),
            "valid_to": DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 2, low_start, 0, 0, 0))),
            "rate": 10,
        }, {
            "valid_from": DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 2, low_end, 0, 0, 0))),
            "valid_to": DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 3, low_start, 0, 0, 0))),
            "rate": 10,
        }, {
            "valid_from": DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 3, low_end, 0, 0, 0))),
            "valid_to": DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 4, 0, 0, 0, 0))),
            "rate": 10,
        }, {
            "valid_from": DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, low_start, 0, 0, 0))),
            "valid_to": DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, low_end, 0, 0, 0))),
            "rate": 5,
        }, {
            "valid_from": DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 2, low_start, 0, 0, 0))),
            "valid_to": DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 2, low_end, 0, 0, 0))),
            "rate": 5,
        }, {
            "valid_from": DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 3, low_start, 0, 0, 0))),
            "valid_to": DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 3, low_end, 0, 0, 0))),
            "rate": 5,
        }]
        result = normaliser.normalise(input_day_rates, input_night_rates, date(2023, month, 1), date(2023, month, 3))
        for obj in expected:
            with self.subTest(obj=obj):
                self.assertIn(obj, result, f"Object {obj} not found in the expected rows.")
        self.assertEqual(len(expected), len(result), "Arrays have different lengths.")

    @parameterized.expand([2, 8])
    def test_cross_midnight(self, month):
        low_start = 23
        low_end = 6
        normaliser = EcoUnitNormaliser(LONDON_TZ, low_start, low_end)
        input_day_rates = [{
            "valid_from": DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month - 1, 15, 0, 0, 0, 0))),
            "valid_to": DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 15, 0, 0, 0, 0))),
            "rate": 10,
        }]
        input_night_rates = [{
            "valid_from": DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, 1, 15, 0, 0, 0, 0))),
            "valid_to": DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 15, 0, 0, 0, 0))),
            "rate": 5,
        }]
        expected = [{
            "valid_from": DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, low_end, 0, 0, 0))),
            "valid_to": DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, low_start, 0, 0, 0))),
            "rate": 10,
        }, {
            "valid_from": DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 2, low_end, 0, 0, 0))),
            "valid_to": DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 2, low_start, 0, 0, 0))),
            "rate": 10,
        }, {
            "valid_from": DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 3, low_end, 0, 0, 0))),
            "valid_to": DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 3, low_start, 0, 0, 0))),
            "rate": 10,
        }, {
            "valid_from": DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, 0, 0, 0, 0))),
            "valid_to": DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, low_end, 0, 0, 0))),
            "rate": 5,
        }, {
            "valid_from": DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, low_start, 0, 0, 0))),
            "valid_to": DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 2, low_end, 0, 0, 0))),
            "rate": 5,
        }, {
            "valid_from": DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 2, low_start, 0, 0, 0))),
            "valid_to": DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 3, low_end, 0, 0, 0))),
            "rate": 5,
        }, {
            "valid_from": DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 3, low_start, 0, 0, 0))),
            "valid_to": DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 4, 0, 0, 0, 0))),
            "rate": 5,
        }]
        result = normaliser.normalise(input_day_rates, input_night_rates, date(2023, month, 1), date(2023, month, 3))
        for obj in expected:
            with self.subTest(obj=obj):
                self.assertIn(obj, result, f"Object {obj} not found in the expected rows.")
        self.assertEqual(len(expected), len(result), "Arrays have different lengths.")


if __name__ == '__main__':
    main()
