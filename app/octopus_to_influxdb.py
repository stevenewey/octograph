#!/usr/bin/env python
from configparser import ConfigParser
from datetime import datetime, date, timedelta

import click
import maya
import pytz
from influxdb_client import InfluxDBClient
from influxdb_client.client.write.point import Point
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.domain.write_precision import WritePrecision

from app.date_utils import DateUtils
from app.eco_unit_normaliser import EcoUnitNormaliser
from app.octopus_api_client import OctopusApiClient
from app.series_maker import SeriesMaker


def store_series(connection, series, metrics, rate_data, additional_tags):
    agile_data = rate_data.get('agile_unit_rates', [])
    agile_rates = {
        point['valid_to']: point['value_inc_vat']
        for point in agile_data
    }

    def active_rate_field(measurement):
        if series == 'gas':
            return 'unit_rate'
        elif not rate_data['unit_rate_low_zone']:  # no low rate
            return 'unit_rate_high'

        low_start_str = rate_data['unit_rate_low_start']
        low_end_str = rate_data['unit_rate_low_end']
        low_zone = rate_data['unit_rate_low_zone']

        measurement_at = maya.parse(measurement['interval_start'])

        low_start = maya.when(
            measurement_at.datetime(to_timezone=low_zone).strftime(
                f'%Y-%m-%dT{low_start_str}'
            ),
            timezone=low_zone
        )
        low_end = maya.when(
            measurement_at.datetime(to_timezone=low_zone).strftime(
                f'%Y-%m-%dT{low_end_str}'
            ),
            timezone=low_zone
        )
        if low_start > low_end:
            # end time is the following day
            low_end_d1 = maya.when(
                measurement_at.datetime(to_timezone=low_zone).strftime(
                    f'%Y-%m-%dT23:59:59'
                ),
                timezone=low_zone
            )
            low_start_d2 = maya.when(
                measurement_at.datetime(to_timezone=low_zone).strftime(
                    f'%Y-%m-%dT00:00:00'
                ),
                timezone=low_zone
            )
            low_period_a = maya.MayaInterval(low_start, low_end_d1)
            low_period_b = maya.MayaInterval(low_start_d2, low_end)
        else:
            low_period_a = low_period_b = maya.MayaInterval(low_start, low_end)
        return \
            'unit_rate_low' if measurement_at in low_period_a \
                               or measurement_at in low_period_b \
                else 'unit_rate_high'

    def fields_for_measurement(measurement):
        consumption = measurement['consumption']
        conversion_factor = rate_data.get('conversion_factor', None)
        if conversion_factor:
            consumption *= conversion_factor
        rate = active_rate_field(measurement)
        rate_cost = rate_data[rate]
        cost = consumption * rate_cost
        standing_charge = rate_data['standing_charge'] / 48  # 30 minute reads
        fields = {
            'consumption': consumption,
            'cost': cost,
            'total_cost': cost + standing_charge,
        }
        if agile_data:
            agile_standing_charge = rate_data['agile_standing_charge'] / 48
            agile_unit_rate = agile_rates.get(
                maya.parse(measurement['interval_end']).iso8601(),
                rate_data[rate]  # cludge, use Go rate during DST changeover
            )
            agile_cost = agile_unit_rate * consumption
            fields.update({
                'agile_rate': agile_unit_rate,
                'agile_cost': agile_cost,
                'agile_total_cost': agile_cost + agile_standing_charge,
            })
        return fields

    def tags_for_measurement(measurement):
        period = maya.parse(measurement['interval_end'])
        time = period.datetime().strftime('%H:%M')
        return {
            'active_rate': active_rate_field(measurement),
            'time_of_day': time,
        }

    measurements = [
        {
            'measurement': series,
            'tags': tags_for_measurement(measurement) | additional_tags,
            'time': measurement['interval_end'],
            'fields': fields_for_measurement(measurement),
        }
        for measurement in metrics
    ]
    # connection.write_points(measurements)


class OctopusToInflux:

    def __init__(self, config_file=None, from_date=None, to_date=None):
        config = ConfigParser()
        config.read(config_file)

        self._account_number = config.get('octopus', 'account_number')
        if not self._account_number:
            raise click.ClickException('No Octopus account number set')

        timezone = pytz.timezone(config.get('octopus', 'timezone', fallback='Europe/London'))
        from_date = date.fromisoformat(DateUtils.yesterday_date_string(timezone) if not from_date or from_date == 'yesterday' else from_date)
        to_date = date.fromisoformat(DateUtils.yesterday_date_string(timezone) if not to_date or to_date == 'yesterday' else to_date)
        self._from = DateUtils.local_midnight(from_date, timezone)
        self._to = DateUtils.local_midnight(to_date + timedelta(days=1), timezone)
        self._payment_method = config.get('octopus', 'payment_method', fallback='DIRECT_DEBIT')

        low_start = config.getint('octopus', 'unit_rate_low_start', fallback=1)
        low_end = config.getint('octopus', 'unit_rate_low_end', fallback=8)
        self._eco_unit_normaliser = EcoUnitNormaliser(from_date, to_date, timezone, low_start, low_end)

        self._resolution_minutes = config.getint('octopus', 'resolution_minutes', fallback=30)
        self._series_maker = SeriesMaker(self._resolution_minutes)

        self._octopus_api = OctopusApiClient(
            config.get('octopus', 'api_prefix', fallback='https://api.octopus.energy/v1'),
            config.get('octopus', 'api_key'),
            self._resolution_minutes,
        )

        influx_client = InfluxDBClient(
            url=config.get('influxdb', 'url', fallback='http://localhost:8086'),
            token=config.get('influxdb', 'token', fallback=''),
            org=config.get('influxdb', 'org', fallback='primary')
        )
        self._influx = influx_client.write_api(write_options=SYNCHRONOUS)
        self._influx_bucket = config.get('influxdb', 'bucket', fallback='primary')

        included_meters_str = config.get('octopus', 'included_meters', fallback=None)
        self._included_meters = included_meters_str.split(',') if included_meters_str else None

        included_tags_str = config.get('influxdb', 'included_tags', fallback=None)
        self._included_tags = included_tags_str.split(',') if included_tags_str else []

        additional_tags_str = config.get('influxdb', 'additional_tags', fallback=None)
        self._additional_tags = dict(item.split('=') for item in additional_tags_str.split(',')) if additional_tags_str else {}

    def run(self):
        account = self._octopus_api.retrieve_account(self._account_number)
        tags = self._additional_tags.copy()
        if 'account_number' in self._included_tags:
            tags['account_number'] = account['number']

        for p in account['properties']:
            self._process_property(p, tags)

    def _process_property(self, p, base_tags: dict[str, str]):
        tags = base_tags.copy()
        click.echo(f'Processing property: {p["address_line_1"]}, {p["postcode"]}')
        for field in ['address_line_1', 'address_line_2', 'address_line_3', 'town', 'postcode']:
            if f'property_{field}' in self._included_tags:
                tags[f'property_{field}'] = p[field]
        if len(p['electricity_meter_points']) == 0:
            click.echo('No electricity meter points found in property')
        for emp in p['electricity_meter_points']:
            self._process_emp(emp, tags)
        if len(p['gas_meter_points']) == 0:
            click.echo('No gas meter points found in property')
        for emp in p['gas_meter_points']:
            self._process_gmp(emp, tags)

    def _process_emp(self, emp, base_tags: dict[str, str]):
        tags = base_tags.copy()
        click.echo(f'Processing electricity meter point: {emp["mpan"]}')
        if 'electricity_mpan' in self._included_tags:
            tags['electricity_mpan'] = emp["mpan"]
        pricing_dict = self._get_pricing(emp['agreements'])
        standard_unit_rows = []
        if 'day_unit_rates' in pricing_dict and 'night_unit_rates' in pricing_dict:
            standard_unit_rows += self._eco_unit_normaliser.normalise(pricing_dict['day_unit_rates'], pricing_dict['night_unit_rates'])
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
                self._process_em(emp['mpan'], em, standard_unit_rates, standing_charges, tags)

    def _process_em(self, mpan: str, em, standard_unit_rates, standing_charges, base_tags: dict[str, str]):
        tags = base_tags.copy()
        click.echo(f'Processing electricity meter: {em["serial_number"]}')
        if 'meter_serial_number' in self._included_tags:
            tags['meter_serial_number'] = em['serial_number']
        last_interval_start = DateUtils.iso8601(self._to - timedelta(minutes=self._resolution_minutes))
        data = self._octopus_api.retrieve_electricity_consumption(mpan, em['serial_number'], DateUtils.iso8601(self._from), last_interval_start)
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
        click.echo(len(points))
        self._influx.write(self._influx_bucket, record=points)

    def _process_gmp(self, gmp, base_tags: dict[str, str]):
        tags = base_tags.copy()
        click.echo(f'Processing gas meter point: {gmp["mprn"]}')
        if 'gas_mprn' in self._included_tags:
            tags['gas_mprn'] = gmp["mprn"]
        for gm in gmp['meters']:
            if self._included_meters and gm['serial_number'] not in self._included_meters:
                click.echo(f'Skipping gas meter {gm['serial_number']} as it is not in octopus.included_meters')
            else:
                self._process_gm(gm, tags)

    def _process_gm(self, gm, base_tags: dict[str, str]):
        tags = base_tags.copy()
        click.echo(f'Processing gas meter: {gm["serial_number"]}')
        if 'meter_serial_number' in self._included_tags:
            tags['meter_serial_number'] = gm["serial_number"]
        click.echo(f'TAGS: {tags}')

    def _get_pricing(self, agreements):
        f = self._from
        t = self._to
        pricing = {}
        for a in sorted(agreements, key=lambda x: datetime.fromisoformat(x['valid_from'])):
            agreement_valid_from = datetime.fromisoformat(a['valid_from'])
            agreement_valid_to = datetime.fromisoformat(a['valid_to']) if a['valid_to'] else self._to
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
                if t < self._to:
                    f = t
                    t = self._to
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
        self._influx.write(self._influx_bucket, record=points)


@click.command()
@click.option(
    '--config-file',
    default="octograph.ini",
    type=click.Path(exists=True, dir_okay=True, readable=True),
)
@click.option('--from-date', default='yesterday', type=click.STRING)
@click.option('--to-date', default='yesterday', type=click.STRING)
def cmd(config_file, from_date, to_date):
    app = OctopusToInflux(config_file, from_date, to_date)
    app.run()

    # rate_data = {}
    #
    # if e_enabled:
    #     e_mpan = config.get('electricity', 'mpan', fallback=None)
    #     e_serial = config.get('electricity', 'serial_number', fallback=None)
    #     if not e_mpan or not e_serial:
    #         raise click.ClickException('No electricity meter identifiers')
    #     e_url = 'https://api.octopus.energy/v1/electricity-meter-points/' \
    #             f'{e_mpan}/meters/{e_serial}/consumption/'
    #     agile_url = config.get('electricity', 'agile_rate_url', fallback=None)
    #     rate_data |= {
    #         'electricity': {
    #             'standing_charge': config.getfloat(
    #                 'electricity', 'standing_charge', fallback=0.0
    #             ),
    #             'unit_rate_high': config.getfloat(
    #                 'electricity', 'unit_rate_high', fallback=0.0
    #             ),
    #             'unit_rate_low': config.getfloat(
    #                 'electricity', 'unit_rate_low', fallback=0.0
    #             ),
    #             'unit_rate_low_start': config.get(
    #                 'electricity', 'unit_rate_low_start', fallback="00:00"
    #             ),
    #             'unit_rate_low_end': config.get(
    #                 'electricity', 'unit_rate_low_end', fallback="00:00"
    #             ),
    #             'unit_rate_low_zone': timezone,
    #             'agile_standing_charge': config.getfloat(
    #                 'electricity', 'agile_standing_charge', fallback=0.0
    #             ),
    #             'agile_unit_rates': [],
    #         },
    #     }
    #     click.echo(
    #         f'Retrieving electricity data for {from_iso} until {to_iso}...',
    #         nl=False
    #     )
    #     e_consumption = retrieve_paginated_data(
    #         api_key, e_url, from_iso, to_iso
    #     )
    #     click.echo(f' {len(e_consumption)} readings.')
    #     click.echo(
    #         f'Retrieving Agile rates for {from_iso} until {to_iso}...',
    #         nl=False
    #     )
    #     rate_data['electricity']['agile_unit_rates'] = retrieve_paginated_data(
    #         api_key, agile_url, from_iso, to_iso
    #     )
    #     click.echo(f' {len(rate_data["electricity"]["agile_unit_rates"])} rates.')
    #     tags = additional_tags.copy()
    #     if include_mpan_tag:
    #         tags |= {'mpan': e_mpan}
    #     if include_serial_number_tag:
    #         tags |= {'serial_number': e_serial}
    #     store_series(influx, 'electricity', e_consumption, rate_data['electricity'], tags)
    # else:
    #     click.echo('Electricity is disabled')
    #
    # if g_enabled:
    #     g_mpan = config.get('gas', 'mpan', fallback=None)
    #     g_serial = config.get('gas', 'serial_number', fallback=None)
    #     g_meter_type = config.get('gas', 'meter_type', fallback=1)
    #     g_vcf = config.get('gas', 'volume_correction_factor', fallback=1.02264)
    #     g_cv = config.get('gas', 'calorific_value', fallback=40)
    #     if not g_mpan or not g_serial:
    #         raise click.ClickException('No gas meter identifiers')
    #     g_url = 'https://api.octopus.energy/v1/gas-meter-points/' \
    #             f'{g_mpan}/meters/{g_serial}/consumption/'
    #     rate_data |= {
    #         'gas': {
    #             'standing_charge': config.getfloat(
    #                 'gas', 'standing_charge', fallback=0.0
    #             ),
    #             'unit_rate': config.getfloat('gas', 'unit_rate', fallback=0.0),
    #             # SMETS1 meters report kWh, SMET2 report m^3 and need converting to kWh first
    #             'conversion_factor': (float(g_vcf) * float(g_cv)) / 3.6 if int(g_meter_type) > 1 else None,
    #         }
    #     }
    #     click.echo(
    #         f'Retrieving gas data for {from_iso} until {to_iso}...',
    #         nl=False
    #     )
    #     g_consumption = retrieve_paginated_data(
    #         api_key, g_url, from_iso, to_iso
    #     )
    #     click.echo(f' {len(g_consumption)} readings.')
    #     tags = additional_tags.copy()
    #     if include_mpan_tag:
    #         tags |= {'mpan': g_mpan}
    #     if include_serial_number_tag:
    #         tags |= {'serial_number': g_serial}
    #     store_series(influx, 'gas', g_consumption, rate_data['gas'], tags)
    # else:
    #     click.echo('Gas is disabled')


if __name__ == '__main__':
    cmd()
