import unittest
from datetime import datetime

import pytz
from parameterized import parameterized

from octopus_to_influxdb import DateUtils, SeriesMaker

LONDON_TZ = pytz.timezone('Europe/London')


class TestNormalise(unittest.TestCase):

    def assert_dicts_equal(self, dict1, dict2):
        # Check that both dictionaries have the same keys
        self.assertEqual(set(dict1.keys()), set(dict2.keys()), "Dictionaries do not have the same keys.")

        # Check that both dictionaries have the same values
        for key in dict1.keys():
            value1, value2 = dict1[key], dict2[key]
            if isinstance(value1, dict) and isinstance(value2, dict):
                # Recursively check nested dictionaries
                self.assert_dicts_equal(value1, value2)
            else:
                # Use standard equality check for non-dictionary values
                self.assertEqual(value1, value2, f"Values for key '{key}' do not match.")

    @parameterized.expand([2, 8])
    def test_make_series(self, month):
        maker = SeriesMaker(30)
        input_rows = [{
            "valid_from": DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, 0, 0, 0, 0))),
            "valid_to": DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, 2, 0, 0, 0))),
            "value_exc_vat": 10,
            "tariff_code": "example",
        }, {
            "valid_from": DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, 2, 0, 0, 0))),
            "valid_to": DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, 9, 0, 0, 0))),
            "value_exc_vat": 5,
            "tariff_code": "example",
        }, {
            "valid_from": DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, 9, 0, 0, 0))),
            "valid_to": DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 2, 2, 0, 0, 0))),
            "value_exc_vat": 10,
            "tariff_code": "example",
        }, {
            "valid_from": DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 2, 2, 0, 0, 0))),
            "valid_to": DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 2, 9, 0, 0, 0))),
            "value_exc_vat": 5,
            "tariff_code": "example",
        }, {
            "valid_from": DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 2, 9, 0, 0, 0))),
            "valid_to": DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 3, 0, 0, 0, 0))),
            "value_exc_vat": 10,
            "tariff_code": "example",
        }]
        expected = {
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, 0, 0, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, 0, 30, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, 1, 0, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, 1, 30, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, 2, 0, 0, 0))): {"value_exc_vat": 5, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, 2, 30, 0, 0))): {"value_exc_vat": 5, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, 3, 0, 0, 0))): {"value_exc_vat": 5, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, 3, 30, 0, 0))): {"value_exc_vat": 5, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, 4, 0, 0, 0))): {"value_exc_vat": 5, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, 4, 30, 0, 0))): {"value_exc_vat": 5, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, 5, 0, 0, 0))): {"value_exc_vat": 5, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, 5, 30, 0, 0))): {"value_exc_vat": 5, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, 6, 0, 0, 0))): {"value_exc_vat": 5, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, 6, 30, 0, 0))): {"value_exc_vat": 5, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, 7, 0, 0, 0))): {"value_exc_vat": 5, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, 7, 30, 0, 0))): {"value_exc_vat": 5, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, 8, 0, 0, 0))): {"value_exc_vat": 5, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, 8, 30, 0, 0))): {"value_exc_vat": 5, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, 9, 0, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, 9, 30, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, 10, 0, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, 10, 30, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, 11, 0, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, 11, 30, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, 12, 0, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, 12, 30, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, 13, 0, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, 13, 30, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, 14, 0, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, 14, 30, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, 15, 0, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, 15, 30, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, 16, 0, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, 16, 30, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, 17, 0, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, 17, 30, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, 18, 0, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, 18, 30, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, 19, 0, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, 19, 30, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, 20, 0, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, 20, 30, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, 21, 0, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, 21, 30, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, 22, 0, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, 22, 30, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, 23, 0, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 1, 23, 30, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 2, 0, 0, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 2, 0, 30, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 2, 1, 0, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 2, 1, 30, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 2, 2, 0, 0, 0))): {"value_exc_vat": 5, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 2, 2, 30, 0, 0))): {"value_exc_vat": 5, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 2, 3, 0, 0, 0))): {"value_exc_vat": 5, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 2, 3, 30, 0, 0))): {"value_exc_vat": 5, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 2, 4, 0, 0, 0))): {"value_exc_vat": 5, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 2, 4, 30, 0, 0))): {"value_exc_vat": 5, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 2, 5, 0, 0, 0))): {"value_exc_vat": 5, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 2, 5, 30, 0, 0))): {"value_exc_vat": 5, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 2, 6, 0, 0, 0))): {"value_exc_vat": 5, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 2, 6, 30, 0, 0))): {"value_exc_vat": 5, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 2, 7, 0, 0, 0))): {"value_exc_vat": 5, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 2, 7, 30, 0, 0))): {"value_exc_vat": 5, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 2, 8, 0, 0, 0))): {"value_exc_vat": 5, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 2, 8, 30, 0, 0))): {"value_exc_vat": 5, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 2, 9, 0, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 2, 9, 30, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 2, 10, 0, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 2, 10, 30, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 2, 11, 0, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 2, 11, 30, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 2, 12, 0, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 2, 12, 30, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 2, 13, 0, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 2, 13, 30, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 2, 14, 0, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 2, 14, 30, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 2, 15, 0, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 2, 15, 30, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 2, 16, 0, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 2, 16, 30, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 2, 17, 0, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 2, 17, 30, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 2, 18, 0, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 2, 18, 30, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 2, 19, 0, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 2, 19, 30, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 2, 20, 0, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 2, 20, 30, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 2, 21, 0, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 2, 21, 30, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 2, 22, 0, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 2, 22, 30, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 2, 23, 0, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
            DateUtils.iso8601(LONDON_TZ.localize(datetime(2023, month, 2, 23, 30, 0, 0))): {"value_exc_vat": 10, "tariff_code": "example"},
        }
        result = maker.make_series(input_rows)
        self.assert_dicts_equal(expected, result)


if __name__ == '__main__':
    unittest.main()
