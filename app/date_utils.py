from datetime import datetime, timedelta, timezone, date

from pytz import tzinfo


class DateUtils:

    @staticmethod
    def yesterday_date_string(tz: tzinfo):
        return (datetime.now(tz).date() - timedelta(days=1)).isoformat()

    @staticmethod
    def iso8601(dt: datetime):
        return dt.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

    @staticmethod
    def naive_midnight(d: date):
        return datetime(d.year, d.month, d.day, 0, 0, 0, 0)

    @staticmethod
    def at_midnight(dt: datetime):
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)

    @staticmethod
    def local_midnight(d: date, tz: tzinfo):
        return DateUtils.at_midnight(DateUtils.naive_midnight(d).astimezone(tz))

    @staticmethod
    def minutes_between(a: datetime, b: datetime):
        time_difference = b - a
        return time_difference.total_seconds() / 60
