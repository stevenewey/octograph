from datetime import date, timedelta, datetime

from pytz import tzinfo

from app.date_utils import DateUtils


class EcoUnitNormaliser:
    def __init__(self, tz: tzinfo, unit_low_start: int, unit_low_end: int):
        self.unit_low_start = unit_low_start
        self.unit_low_end = unit_low_end
        self.tz = tz

    def normalise(self, day_rates, night_rates, start_limit: date, end_limit: date):
        start_limit_time = DateUtils.naive_midnight(start_limit).astimezone(self.tz)
        end_limit_time = (DateUtils.naive_midnight(end_limit) + timedelta(days=1)).astimezone(self.tz)
        normalised_rows = []
        for r in day_rates:
            normalised_rows += self._normalise_rate(r, self.unit_low_end, self.unit_low_start, start_limit_time, end_limit_time)
        for r in night_rates:
            normalised_rows += self._normalise_rate(r, self.unit_low_start, self.unit_low_end, start_limit_time, end_limit_time)
        return normalised_rows

    def _normalise_rate(self, r, start_hour: int, end_hour: int, start_limit_time:datetime, end_limit_time: datetime):
        r_from = max(datetime.fromisoformat(r['valid_from']), start_limit_time)
        r_to = min(datetime.fromisoformat(r['valid_to']), end_limit_time)
        midnight = DateUtils.at_midnight(r_from.astimezone(self.tz))
        normalised_rows = []
        if start_hour < end_hour:
            while midnight < r_to:
                nl_from = midnight.replace(hour=start_hour)
                nl_to = min(midnight.replace(hour=end_hour), r_to)
                midnight = DateUtils.local_midnight((midnight.date() + timedelta(days=1)), self.tz)
                normalised_rows.append(r | {"valid_from": DateUtils.iso8601(nl_from), "valid_to": DateUtils.iso8601(nl_to)})
        else:
            if end_hour != 0:
                nl_from = midnight
                nl_to = midnight.replace(hour=end_hour)
                normalised_rows.append(r | {"valid_from": DateUtils.iso8601(nl_from), "valid_to": DateUtils.iso8601(nl_to)})
            while midnight < r_to:
                nl_from = midnight.replace(hour=start_hour)
                midnight = DateUtils.local_midnight((midnight.date() + timedelta(days=1)), self.tz)
                nl_to = min(midnight.replace(hour=end_hour), r_to)
                normalised_rows.append(r | {"valid_from": DateUtils.iso8601(nl_from), "valid_to": DateUtils.iso8601(nl_to)})
        return normalised_rows
