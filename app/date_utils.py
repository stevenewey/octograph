from datetime import datetime, timedelta, timezone, date
from math import floor

from pytz import tzinfo


class DateUtils:

    @staticmethod
    def yesterday_date_string(tz: tzinfo) -> str:
        return (datetime.now(tz).date() - timedelta(days=1)).isoformat()

    @staticmethod
    def iso8601(dt: datetime) -> str:
        return dt.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

    @staticmethod
    def naive_midnight(d: date) -> datetime:
        return datetime(d.year, d.month, d.day, 0, 0, 0, 0)

    @staticmethod
    def at_midnight(dt: datetime) -> datetime:
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)

    @staticmethod
    def local_midnight(d: date, tz: tzinfo) -> datetime:
        return DateUtils.at_midnight(DateUtils.naive_midnight(d).astimezone(tz))

    @staticmethod
    def local_date(dt: datetime, tz: tzinfo) -> date:
        local_dt = dt.astimezone(tz)
        return date(local_dt.year, local_dt.month, local_dt.day)

    @staticmethod
    def minutes_between(a: datetime, b: datetime) -> int:
        time_difference = b - a
        return floor(time_difference.total_seconds() / 60)
