#!/usr/bin/env python
import csv
import json
import os
import re
from configparser import ConfigParser
from datetime import datetime, timedelta, timezone, date
from io import StringIO
from types import MappingProxyType
from urllib import parse
from urllib.parse import urlencode

import click
import maya
import pytz
import requests
from influxdb import InfluxDBClient
from pytz import tzinfo


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


class OctopusApiClient:
    def __init__(self, api_prefix, api_key, group_by=None):
        if not api_key:
            raise click.ClickException('No Octopus API key provided')
        self._api_prefix = api_prefix
        self._api_key = api_key
        self._group_by = group_by

    @staticmethod
    def __generate_cache_filename(url: str, params=None):
        url_with_params = url
        if params:
            query_string = urlencode(params, safe='')
            url_with_params += f"?{query_string}"
        filename = re.sub('[^0-9A-Za-z-]', '_', url_with_params)
        return filename

    def _retrieve_data(self, path: str, args: dict[str, str] = MappingProxyType({})):
        if path.startswith(self._api_prefix):
            url = path
        else:
            url = f'{self._api_prefix}/{path}'

        cache_directory = 'scratch/cache'
        os.makedirs(cache_directory, exist_ok=True)
        filename = self.__generate_cache_filename(url, args)
        cache_path = os.path.join(cache_directory, f"{filename}.json")
        if os.path.exists(cache_path):
            with open(cache_path, 'r', encoding='utf-8') as file:
                cached_data = json.load(file)
            return cached_data

        response = requests.get(url, params=args, auth=(self._api_key, ''))
        response.raise_for_status()
        json_data = response.json()
        with open(cache_path, 'w', encoding='utf-8') as file:
            json.dump(json_data, file)
        return response.json()

    def _retrieve_paginated_data(self, path: str, from_date: str, to_date: str, page: str = None):
        page_size = 25000 if '/consumption/' in path else 1500
        args = {
            'period_from': from_date,
            'period_to': to_date,
            'page_size': page_size,
        }
        if '/consumption/' in path and self._group_by:
            args['group_by'] = self._group_by
        if page:
            args['page'] = page
        data = self._retrieve_data(path, args)
        results = data.get('results', [])
        if data['next']:
            url_query = parse.urlparse(data['next']).query
            next_page = parse.parse_qs(url_query)['page'][0]
            results += self._retrieve_paginated_data(path, from_date, to_date, next_page)
        return results

    def retrieve_account(self, account_number: str):
        return self._retrieve_data(f'accounts/{account_number}/')

    def _retrieve_product(self, code: str):
        return self._retrieve_data(f'products/{code}/')

    def retrieve_tariff_pricing(self, tariff_code: str, from_date: str, to_date: str):
        product_code, _ = self._extract_product_code(tariff_code)
        product = self._retrieve_product(product_code)
        pricing = {}
        for link in self._find_links(product, tariff_code):
            pricing[link['rel']] = self._retrieve_paginated_data(link['href'], from_date, to_date)
        return pricing

    @staticmethod
    def _extract_product_code(tariff_code):
        utility = 'electricity' if tariff_code.startswith('E') else 'gas' if tariff_code.startswith('G') else None
        if not utility:
            raise click.ClickException(f'Tariff code is not electricity or gas: {tariff_code}')
        product_code = '-'.join(tariff_code.split('-')[2:-1])
        return product_code, utility

    @staticmethod
    def _find_links(product, tariff_code):
        for t in ['single_register_electricity_tariffs', 'dual_register_electricity_tariffs', 'single_register_gas_tariffs']:
            if t in product:
                for variant, options in product[t].items():
                    for payment, details in options.items():
                        if details['code'] == tariff_code:
                            return details['links']
        return None


class DateUtils:

    @staticmethod
    def iso8601(dt: datetime):
        return dt.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


class EcoUnitNormaliser:
    def __init__(self, start_limit: date, end_limit: date, tz: tzinfo, unit_low_start: int, unit_low_end: int):
        self.start_limit_date = start_limit
        self.start_limit_time = datetime(start_limit.year, start_limit.month, start_limit.day, 0, 0, 0, 0).astimezone(tz)
        self.end_limit_date = end_limit
        self.end_limit_time = (datetime(end_limit.year, end_limit.month, end_limit.day, 0, 0, 0, 0) + timedelta(days=1)).astimezone(tz)
        self.unit_low_start = unit_low_start
        self.unit_low_end = unit_low_end
        self.tz = tz

    def normalise(self, day_rates, night_rates):
        normalised_rows = []
        for r in day_rates:
            normalised_rows += self.normalise_rate(r, self.unit_low_end, self.unit_low_start)
        for r in night_rates:
            normalised_rows += self.normalise_rate(r, self.unit_low_start, self.unit_low_end)
        return normalised_rows

    def normalise_rate(self, r, start_hour: int, end_hour: int):
        r_from = max(datetime.fromisoformat(r['valid_from']), self.start_limit_time)
        r_to = min(datetime.fromisoformat(r['valid_to']), self.end_limit_time)
        midnight = r_from.astimezone(self.tz).replace(hour=0, minute=0, second=0, microsecond=0)
        normalised_rows = []
        if start_hour < end_hour:
            while midnight < r_to:
                nl_from = midnight.replace(hour=start_hour)
                nl_to = min(midnight.replace(hour=end_hour), r_to)
                tomorrow = (midnight.date() + timedelta(days=1))
                midnight = datetime(tomorrow.year, tomorrow.month, tomorrow.day, 0, 0, 0, 0).astimezone(self.tz).replace(hour=0, minute=0, second=0,
                                                                                                                          microsecond=0)
                normalised_rows.append(r | {"valid_from": DateUtils.iso8601(nl_from), "valid_to": DateUtils.iso8601(nl_to)})
        else:
            if end_hour != 0:
                nl_from = midnight
                nl_to = midnight.replace(hour=end_hour)
                normalised_rows.append(r | {"valid_from": DateUtils.iso8601(nl_from), "valid_to": DateUtils.iso8601(nl_to)})
            while midnight < r_to:
                nl_from = midnight.replace(hour=start_hour)
                tomorrow = (midnight.date() + timedelta(days=1))
                midnight = datetime(tomorrow.year, tomorrow.month, tomorrow.day, 0, 0, 00, 0).astimezone(self.tz).replace(hour=0, minute=0, second=0,
                                                                                                                          microsecond=0)
                nl_to = min(midnight.replace(hour=end_hour), r_to)
                normalised_rows.append(r | {"valid_from": DateUtils.iso8601(nl_from), "valid_to": DateUtils.iso8601(nl_to)})
        return normalised_rows


class SeriesMaker:
    def __init__(self, interval_minutes: int):
        self.interval_minutes = interval_minutes

    def make_series(self, rows):
        series = {}
        for r in rows:
            dt = datetime.fromisoformat(r['valid_from'])
            to = datetime.fromisoformat(r['valid_to'])
            while dt < to:
                series[DateUtils.iso8601(dt)] = self.copy_object_with_keys(r, ['tariff_code', 'value_exc_vat', 'value_inc_vat'])
                dt = dt + timedelta(minutes=self.interval_minutes)
        return series

    @staticmethod
    def copy_object_with_keys(original_object, keys_to_copy):
        return {key: original_object[key] for key in keys_to_copy if key in original_object}


class OctopusToInflux:

    @staticmethod
    def _to_group_by(resolution: int):
        if resolution == 30:
            return None
        if resolution == 60:
            return 'hour'
        if resolution == 60*24:
            return 'day'
        if resolution == 60*24*7:
            return 'week'
        raise click.ClickException(f'Invalid resolution: {resolution}')

    def __init__(self, config_file=None, from_date=None, to_date=None):
        config = ConfigParser()
        config.read(config_file)

        self._resolution_minutes = config.getint('octopus', 'resolution_minutes', fallback=30)
        self._octopus_api = OctopusApiClient(
            config.get('octopus', 'api_prefix', fallback='https://api.octopus.energy/v1'),
            config.get('octopus', 'api_key'),
            self._to_group_by(self._resolution_minutes),
        )

        self._influx = InfluxDBClient(
            host=config.get('influxdb', 'host', fallback="localhost"),
            port=config.getint('influxdb', 'port', fallback=8086),
            username=config.get('influxdb', 'user', fallback=""),
            password=config.get('influxdb', 'password', fallback=""),
            database=config.get('influxdb', 'database', fallback="energy"),
        )

        self._account_number = config.get('octopus', 'account_number')
        if not self._account_number:
            raise click.ClickException('No Octopus account number set')

        self._timezone = config.get('octopus', 'timezone', fallback='Europe/London')
        self._payment_method = config.get('octopus', 'payment_method', fallback='DIRECT_DEBIT')
        # TODO: change to `date` here
        self._from = maya.when(from_date, timezone=self._timezone)
        self._to = maya.when(to_date, timezone=self._timezone)
        self._unit_rate_low_start = config.getint('octopus', 'unit_rate_low_start', fallback=1)
        self._unit_rate_low_end = config.getint('octopus', 'unit_rate_low_end', fallback=8)
        self._eco_unit_normaliser = EcoUnitNormaliser(date.fromisoformat(from_date), date.fromisoformat(from_date), pytz.timezone(self._timezone),
                                                      self._unit_rate_low_start, self._unit_rate_low_end)
        self._resolution_minutes = config.getint('octopus', 'resolution_minutes', fallback=30)
        self._series_maker = SeriesMaker(self._resolution_minutes)

        included_meters_str = config.get('octopus', 'included_meters', fallback=None)
        self._included_meters = included_meters_str.split(',') if included_meters_str else None

        included_tags_str = config.get('influxdb', 'included_tags', fallback=None)
        self._included_tags = included_tags_str.split(',') if included_tags_str else []

        additional_tags_str = config.get('influxdb', 'additional_tags', fallback=None)
        self._additional_tags = dict(item.split('=') for item in additional_tags_str.split(',')) if additional_tags_str else {}

    def run(self):
        account = self._octopus_api.retrieve_account(self._account_number)
        tags = {}
        tags |= self._additional_tags
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
        for component, pricing in {'standard_unit_rates': standard_unit_rates, 'standing_charges': standing_charges}.items():
            click.echo(f'*** {component} ***')
            tsv_output = StringIO()
            timestamps = list(pricing.keys())
            values = list(pricing.values())
            all_keys = set(key for nested_dict in values for key in nested_dict.keys())
            fieldnames = ['timestamp'] + sorted(all_keys)
            tsv_writer = csv.DictWriter(tsv_output, fieldnames=fieldnames, delimiter='\t')
            tsv_writer.writeheader()
            for timestamp, nested_dict in zip(timestamps, values):
                row = {'timestamp': timestamp}
                row.update(nested_dict)
                tsv_writer.writerow(row)
            tsv_string = tsv_output.getvalue()
            click.echo(tsv_string)
            tsv_output.close()

        for em in emp['meters']:
            if self._included_meters and em['serial_number'] not in self._included_meters:
                click.echo(f'Skipping electricity meter {em['serial_number']} as it is not in octopus.included_meters')
            else:
                self._process_em(em, tags)

    def _process_em(self, em, base_tags: dict[str, str]):
        tags = base_tags.copy()
        click.echo(f'Processing electricity meter: {em["serial_number"]}')
        if 'meter_serial_number' in self._included_tags:
            tags['meter_serial_number'] = em["serial_number"]
        click.echo(f'TAGS: {tags}')

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
        for a in sorted(agreements, key=lambda x: maya.parse(x['valid_from'])):
            agreement_valid_from = maya.parse(a['valid_from'])
            agreement_valid_to = maya.parse(a['valid_to']) if a['valid_to'] else self._to
            if agreement_valid_from <= f < agreement_valid_to:
                if agreement_valid_to < t:
                    t = agreement_valid_to
                agreement_rates = self._octopus_api.retrieve_tariff_pricing(a['tariff_code'], f.iso8601(), t.iso8601())
                for component, results in agreement_rates.items():
                    pricing_rows = []
                    for r in [x for x in results if not x['payment_method'] or x['payment_method'] == self._payment_method]:
                        pricing_rows.append({
                            'tariff_code': a['tariff_code'],
                            'valid_from': max(f, maya.parse(r['valid_from'])).iso8601(),
                            'valid_to': (t if not r['valid_to'] else min(t, maya.parse(r['valid_to']))).iso8601(),
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


@click.command()
@click.option(
    '--config-file',
    default="octograph.ini",
    type=click.Path(exists=True, dir_okay=True, readable=True),
)
@click.option('--from-date', default='yesterday midnight', type=click.STRING)
@click.option('--to-date', default='today midnight', type=click.STRING)
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
