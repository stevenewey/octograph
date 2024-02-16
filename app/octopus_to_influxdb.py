#!/usr/bin/env python
from configparser import ConfigParser
from datetime import datetime, date, timedelta

import click
import pytz
from influxdb_client import InfluxDBClient
from influxdb_client.client.write.point import Point
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.domain.write_precision import WritePrecision

from app.date_utils import DateUtils
from app.eco_unit_normaliser import EcoUnitNormaliser
from app.octopus_api_client import OctopusApiClient
from app.series_maker import SeriesMaker


class OctopusToInflux:

    def __init__(self, config_file=None):
        config = ConfigParser()
        config.read(config_file)

        self._account_number = config.get('octopus', 'account_number')
        if not self._account_number:
            raise click.ClickException('No Octopus account number set')

        self._timezone = pytz.timezone(config.get('octopus', 'timezone', fallback='Europe/London'))
        self._payment_method = config.get('octopus', 'payment_method', fallback='DIRECT_DEBIT')
        low_start = config.getint('octopus', 'unit_rate_low_start', fallback=1)
        low_end = config.getint('octopus', 'unit_rate_low_end', fallback=8)
        self._eco_unit_normaliser = EcoUnitNormaliser(self._timezone, low_start, low_end)

        self._resolution_minutes = config.getint('octopus', 'resolution_minutes', fallback=30)
        self._series_maker = SeriesMaker(self._resolution_minutes)

        self._octopus_api = OctopusApiClient(
            config.get('octopus', 'api_prefix', fallback='https://api.octopus.energy/v1'),
            config.get('octopus', 'api_key'),
            self._resolution_minutes,
            config.get('octopus', 'cache_dir', fallback='/tmp/octopus_api_cache') if config.getboolean('octopus', 'enable_cache') else None
        )

        self._influx_bucket = config.get('influxdb', 'bucket', fallback='primary')
        influx_client = InfluxDBClient(
            url=config.get('influxdb', 'url', fallback='http://localhost:8086'),
            token=config.get('influxdb', 'token', fallback=''),
            org=config.get('influxdb', 'org', fallback='primary')
        )
        self._influx_write = influx_client.write_api(write_options=SYNCHRONOUS)
        self._influx_query = influx_client.query_api()

        included_meters_str = config.get('octopus', 'included_meters', fallback=None)
        self._included_meters = included_meters_str.split(',') if included_meters_str else None

        included_tags_str = config.get('influxdb', 'included_tags', fallback=None)
        self._included_tags = included_tags_str.split(',') if included_tags_str else []

        additional_tags_str = config.get('influxdb', 'additional_tags', fallback=None)
        self._additional_tags = dict(item.split('=') for item in additional_tags_str.split(',')) if additional_tags_str else {}

    def find_latest_date(self, measurement: str, field: str, tags: dict[str, str]):
        tf = ' '.join([f' and r["{k}"] == "{v}"' for k, v in tags.items()])
        f = f'r["_measurement"] == "{measurement}" and r._field == "{field}" {tf})'
        q = f'from(bucket: "{self._influx_bucket}") |> range(start: -30d) |> filter(fn: (r) => {f} |> last()'
        r = self._influx_query.query(q)
        if len(r) != 1 or len(r[0].records) != 1:
            raise click.ClickException(f'No data found for {measurement} in last 30 days - try back-filling')
        return r[0].records[0].values['_time']

    def collect(self, from_str: str, to_str: str):
        click.echo(f'Collecting data between {from_str} and {to_str}')
        from_str = DateUtils.yesterday_date_string(self._timezone) if from_str == 'yesterday' else from_str
        to_str = DateUtils.yesterday_date_string(self._timezone) if to_str == 'yesterday' else to_str
        collect_from = None if from_str == 'latest' else DateUtils.local_midnight(date.fromisoformat(from_str), self._timezone)
        collect_to = DateUtils.local_midnight(date.fromisoformat(to_str) + timedelta(days=1), self._timezone)

        account = self._octopus_api.retrieve_account(self._account_number)
        tags = self._additional_tags.copy()
        if 'account_number' in self._included_tags:
            tags['account_number'] = account['number']

        for p in account['properties']:
            self._process_property(p, collect_from, collect_to, tags)

    def _process_property(self, p, collect_from: datetime, collect_to: datetime, base_tags: dict[str, str]):
        tags = base_tags.copy()
        click.echo(f'Processing property: {p["address_line_1"]}, {p["postcode"]}')
        for field in ['address_line_1', 'address_line_2', 'address_line_3', 'town', 'postcode']:
            if f'property_{field}' in self._included_tags:
                tags[f'property_{field}'] = p[field]
        if len(p['electricity_meter_points']) == 0:
            click.echo('No electricity meter points found in property')
        for emp in p['electricity_meter_points']:
            self._process_emp(emp, collect_from, collect_to, tags)
        if len(p['gas_meter_points']) == 0:
            click.echo('No gas meter points found in property')
        for emp in p['gas_meter_points']:
            self._process_gmp(emp, collect_from, collect_to, tags)

    def _process_emp(self, emp, collect_from: datetime, collect_to: datetime, base_tags: dict[str, str]):
        tags = base_tags.copy()
        click.echo(f'Processing electricity meter point: {emp["mpan"]}')
        if 'electricity_mpan' in self._included_tags:
            tags['electricity_mpan'] = emp["mpan"]
        if not collect_from:
            collect_from = self.find_latest_date('electricity_consumption', 'usage_kwh', tags) + timedelta(minutes=self._resolution_minutes)
        if DateUtils.minutes_between(collect_from, collect_to) < self._resolution_minutes:
            click.echo('No new data to collect for EMP')
            return

        pricing_dict = self._get_pricing(emp['agreements'], collect_from, collect_to)
        standard_unit_rows = []
        if 'day_unit_rates' in pricing_dict and 'night_unit_rates' in pricing_dict:
            standard_unit_rows += self._eco_unit_normaliser.normalise(pricing_dict['day_unit_rates'], pricing_dict['night_unit_rates'],
                                                                      DateUtils.local_date(collect_from, self._timezone),
                                                                      DateUtils.local_date(collect_to, self._timezone))
        if 'standard_unit_rates' in pricing_dict:
            standard_unit_rows += pricing_dict['standard_unit_rates']
            pass
        standard_unit_rates = self._series_maker.make_series(standard_unit_rows)
        if 'standing_charges' in pricing_dict:
            standing_charges = self._series_maker.make_series(pricing_dict['standing_charges'])
        else:
            click.echo(f'Could not find pricing for mpan: {emp["mpan"]}')
            standing_charges = {}

        self._store_emp_pricing(standard_unit_rates, standing_charges, tags)

        for em in emp['meters']:
            if self._included_meters and em['serial_number'] not in self._included_meters:
                click.echo(f'Skipping electricity meter {em['serial_number']} as it is not in octopus.included_meters')
            else:
                self._process_em(emp['mpan'], em, collect_from, collect_to, standard_unit_rates, standing_charges, tags)

    def _process_em(self, mpan: str, em, collect_from: datetime, collect_to: datetime, standard_unit_rates, standing_charges, base_tags: dict[str, str]):
        tags = base_tags.copy()
        click.echo(f'Processing electricity meter: {em["serial_number"]}')
        if 'meter_serial_number' in self._included_tags:
            tags['meter_serial_number'] = em['serial_number']
        last_interval_start = DateUtils.iso8601(collect_to - timedelta(minutes=self._resolution_minutes))
        data = self._octopus_api.retrieve_electricity_consumption(mpan, em['serial_number'], DateUtils.iso8601(collect_from), last_interval_start)
        consumption = self._series_maker.make_series(data)
        self._store_em_consumption(consumption, standard_unit_rates, standing_charges, tags)

    def _store_em_consumption(self, consumption: dict, standard_unit_rates, standing_charges, base_tags: [str, str]):
        points = []
        for t, c in consumption.items():
            usage_cost_exc_vat_pence = standard_unit_rates[t]['value_exc_vat'] * c['consumption']
            usage_cost_inc_vat_pence = standard_unit_rates[t]['value_inc_vat'] * c['consumption']
            standing_charge_cost_exc_vat_pence = standing_charges[t]['value_exc_vat'] / (60 * 24 / self._resolution_minutes)
            standing_charge_cost_inc_vat_pence = standing_charges[t]['value_inc_vat'] / (60 * 24 / self._resolution_minutes)
            points.append(Point.from_dict({
                'measurement': 'electricity_consumption',
                'time': t,
                'fields': {
                    'usage_kwh': c['consumption'],
                    'usage_cost_exc_vat_pence': usage_cost_exc_vat_pence,
                    'usage_cost_inc_vat_pence': usage_cost_inc_vat_pence,
                    'standing_charge_cost_exc_vat_pence': standing_charge_cost_exc_vat_pence,
                    'standing_charge_cost_inc_vat_pence': standing_charge_cost_inc_vat_pence,
                    'total_cost_exc_vat_pence': usage_cost_exc_vat_pence + standing_charge_cost_exc_vat_pence,
                    'total_cost_inc_vat_pence': usage_cost_inc_vat_pence + standing_charge_cost_inc_vat_pence,
                },
                'tags': {'tariff_code': standard_unit_rates[t]['tariff_code']} | base_tags,
            }, write_precision=WritePrecision.S))
        click.echo(f'Storing {len(points)} points')
        self._influx_write.write(self._influx_bucket, record=points)

    def _process_gmp(self, gmp, collect_from: datetime, collect_to: datetime, base_tags: dict[str, str]):
        tags = base_tags.copy()
        click.echo(f'Processing gas meter point: {gmp["mprn"]}')
        if 'gas_mprn' in self._included_tags:
            tags['gas_mprn'] = gmp["mprn"]
        click.echo(f'TODO: _store_gmp_pricing')
        for gm in gmp['meters']:
            if self._included_meters and gm['serial_number'] not in self._included_meters:
                click.echo(f'Skipping gas meter {gm['serial_number']} as it is not in octopus.included_meters')
            else:
                self._process_gm(gm, collect_from, collect_to, tags)

    def _process_gm(self, gm, collect_from: datetime, collect_to: datetime, base_tags: dict[str, str]):
        tags = base_tags.copy()
        click.echo(f'Processing gas meter: {gm["serial_number"]}')
        if 'meter_serial_number' in self._included_tags:
            tags['meter_serial_number'] = gm["serial_number"]
        click.echo(f'TODO: _store_gm_consumption')

    def _get_pricing(self, agreements, collect_from: datetime, collect_to: datetime):
        f = collect_from
        t = collect_to
        pricing = {}
        for a in sorted(agreements, key=lambda x: datetime.fromisoformat(x['valid_from'])):
            agreement_valid_from = datetime.fromisoformat(a['valid_from'])
            agreement_valid_to = datetime.fromisoformat(a['valid_to']) if a['valid_to'] else collect_to
            if agreement_valid_from <= f < agreement_valid_to:
                if agreement_valid_to < t:
                    t = agreement_valid_to
                agreement_rates = self._octopus_api.retrieve_tariff_pricing(a['tariff_code'], DateUtils.iso8601(f), DateUtils.iso8601(t))
                for component, results in agreement_rates.items():
                    pricing_rows = []
                    for r in [x for x in results if not x['payment_method'] or x['payment_method'] == self._payment_method]:
                        pricing_rows.append({
                            'tariff_code': a['tariff_code'],
                            'valid_from': DateUtils.iso8601(max(f, datetime.fromisoformat(r['valid_from']))),
                            'valid_to': DateUtils.iso8601(t if not r['valid_to'] else min(t, datetime.fromisoformat(r['valid_to']))),
                            'value_exc_vat': r['value_exc_vat'],
                            'value_inc_vat': r['value_inc_vat'],
                        })
                    if component not in pricing:
                        pricing[component] = []
                    pricing[component] += pricing_rows
                if t < collect_to:
                    f = t
                    t = collect_to
        return pricing

    def _store_emp_pricing(self, standard_unit_rates, standing_charges, base_tags: dict[str, str]):
        points = [
            Point.from_dict({
                'measurement': 'electricity_pricing',
                'time': t,
                'fields': {
                    'unit_price_exc_vat_price': standard_unit_rates[t]['value_exc_vat'],
                    'unit_price_inc_vat_price': standard_unit_rates[t]['value_inc_vat'],
                    'standing_charge_exc_vat_price': standing_charges[t]['value_exc_vat'],
                    'standing_charge_inc_vat_price': standing_charges[t]['value_inc_vat'],
                },
                'tags': {'tariff_code': standard_unit_rates[t]['tariff_code']} | base_tags,
            }, write_precision=WritePrecision.S)
            for t in set(standard_unit_rates.keys()).union(standing_charges.keys())
        ]
        click.echo(len(points))
        self._influx_write.write(self._influx_bucket, record=points)


@click.command()
@click.option(
    '--config-file',
    default="octograph.ini",
    type=click.Path(exists=True, dir_okay=True, readable=True),
)
@click.option('--from-date', default='yesterday', type=click.STRING)
@click.option('--to-date', default='yesterday', type=click.STRING)
def cmd(config_file, from_date, to_date):
    app = OctopusToInflux(config_file)
    app.collect(from_date, to_date)


if __name__ == '__main__':
    cmd()
