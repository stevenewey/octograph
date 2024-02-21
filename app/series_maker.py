from datetime import datetime, timedelta

from app.date_utils import DateUtils


class SeriesMaker:
    def __init__(self, interval_minutes: int):
        self.interval_minutes = interval_minutes

    def make_series(self, rows):
        series = {}
        for r in rows:
            dt = datetime.fromisoformat(r.get('valid_from', r.get('interval_start')))
            to = datetime.fromisoformat(r.get('valid_to', r.get('interval_end')))
            while dt < to:
                series[DateUtils.iso8601(dt)] = self.copy_object_with_keys(r, ['tariff_code', 'value_exc_vat', 'value_inc_vat', 'consumption'])
                if 'consumption' in r and DateUtils.minutes_between(dt, to) > self.interval_minutes:
                    series[DateUtils.iso8601(dt)]['consumption'] /= DateUtils.minutes_between(dt, to)/self.interval_minutes
                dt = dt + timedelta(minutes=self.interval_minutes)
        return series

    @staticmethod
    def copy_object_with_keys(original_object, keys_to_copy):
        return {key: original_object[key] for key in keys_to_copy if key in original_object}
