#!/usr/bin/env python

from configparser import ConfigParser
from urllib import parse

import click
import maya
import requests
from influxdb import InfluxDBClient


def retrieve_paginated_data(
        api_key, url, from_date, to_date, page=None
):
    args = {
        'period_from': from_date,
        'period_to': to_date,
    }
    if page:
        args['page'] = page
    response = requests.get(url, params=args, auth=(api_key, ''))
    response.raise_for_status()
    data = response.json()
    results = data.get('results', [])
    if data['next']:
        url_query = parse.urlparse(data['next']).query
        next_page = parse.parse_qs(url_query)['page'][0]
        results += retrieve_paginated_data(
            api_key, url, from_date, to_date, next_page
        )
    return results

# GET /v1/products/{product_code}/electricity-tariffs/{tariff_code}/standing-charges/
# GET GET /v1/products/{product_code}/gas-tariffs/{tariff_code}/standing-charges/
def retrieve_standing_charges(api_key, url):
    response = requests.get(url, params=args, auth=(api_key, ''))
    response.raise_for_status()
    data = response.json()


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


@click.command()
@click.option(
    '--config-file',
    default="octograph.ini",
    type=click.Path(exists=True, dir_okay=True, readable=True),
)
@click.option('--from-date', default='yesterday midnight', type=click.STRING)
@click.option('--to-date', default='today midnight', type=click.STRING)
def cmd(config_file, from_date, to_date):
    config = ConfigParser()
    config.read(config_file)

    influx = InfluxDBClient(
        host=config.get('influxdb', 'host', fallback="localhost"),
        port=config.getint('influxdb', 'port', fallback=8086),
        username=config.get('influxdb', 'user', fallback=""),
        password=config.get('influxdb', 'password', fallback=""),
        database=config.get('influxdb', 'database', fallback="energy"),
    )
    include_mpan_tag = config.getboolean('influxdb', 'include_mpan_tag', fallback=False)
    include_serial_number_tag = config.getboolean('influxdb', 'include_serial_number_tag', fallback=False)
    additional_tags_str = config.get('influxdb', 'additional_tags', fallback=None)
    additional_tags = {}
    if additional_tags_str:
        additional_tags |= dict(item.split("=") for item in additional_tags_str.split(","))

    api_key = config.get('octopus', 'api_key')
    if not api_key:
        raise click.ClickException('No Octopus API key set')

    e_enabled = config.getboolean('electricity', 'enabled', fallback=True)
    g_enabled = config.getboolean('electricity', 'enabled', fallback=True)
    timezone = config.get('electricity', 'unit_rate_low_zone', fallback=None)
    from_iso = maya.when(from_date, timezone=timezone).iso8601()
    to_iso = maya.when(to_date, timezone=timezone).iso8601()
    rate_data = {}

    if e_enabled:
        e_mpan = config.get('electricity', 'mpan', fallback=None)
        e_serial = config.get('electricity', 'serial_number', fallback=None)
        if not e_mpan or not e_serial:
            raise click.ClickException('No electricity meter identifiers')
        e_url = 'https://api.octopus.energy/v1/electricity-meter-points/' \
                f'{e_mpan}/meters/{e_serial}/consumption/'
        agile_url = config.get('electricity', 'agile_rate_url', fallback=None)
        rate_data |= {
            'electricity': {
                'standing_charge': config.getfloat(
                    'electricity', 'standing_charge', fallback=0.0
                ),
                'unit_rate_high': config.getfloat(
                    'electricity', 'unit_rate_high', fallback=0.0
                ),
                'unit_rate_low': config.getfloat(
                    'electricity', 'unit_rate_low', fallback=0.0
                ),
                'unit_rate_low_start': config.get(
                    'electricity', 'unit_rate_low_start', fallback="00:00"
                ),
                'unit_rate_low_end': config.get(
                    'electricity', 'unit_rate_low_end', fallback="00:00"
                ),
                'unit_rate_low_zone': timezone,
                'agile_standing_charge': config.getfloat(
                    'electricity', 'agile_standing_charge', fallback=0.0
                ),
                'agile_unit_rates': [],
            },
        }
        click.echo(
            f'Retrieving electricity data for {from_iso} until {to_iso}...',
            nl=False
        )
        e_consumption = retrieve_paginated_data(
            api_key, e_url, from_iso, to_iso
        )
        click.echo(f' {len(e_consumption)} readings.')
        click.echo(
            f'Retrieving Agile rates for {from_iso} until {to_iso}...',
            nl=False
        )
        rate_data['electricity']['agile_unit_rates'] = retrieve_paginated_data(
            api_key, agile_url, from_iso, to_iso
        )
        click.echo(f' {len(rate_data["electricity"]["agile_unit_rates"])} rates.')
        tags = additional_tags.copy()
        if include_mpan_tag:
            tags |= {'mpan': e_mpan}
        if include_serial_number_tag:
            tags |= {'serial_number': e_serial}
        store_series(influx, 'electricity', e_consumption, rate_data['electricity'], tags)
    else:
        click.echo('Electricity is disabled')

    if g_enabled:
        g_mpan = config.get('gas', 'mpan', fallback=None)
        g_serial = config.get('gas', 'serial_number', fallback=None)
        g_meter_type = config.get('gas', 'meter_type', fallback=1)
        g_vcf = config.get('gas', 'volume_correction_factor', fallback=1.02264)
        g_cv = config.get('gas', 'calorific_value', fallback=40)
        if not g_mpan or not g_serial:
            raise click.ClickException('No gas meter identifiers')
        g_url = 'https://api.octopus.energy/v1/gas-meter-points/' \
                f'{g_mpan}/meters/{g_serial}/consumption/'
        rate_data |= {
            'gas': {
                'standing_charge': config.getfloat(
                    'gas', 'standing_charge', fallback=0.0
                ),
                'unit_rate': config.getfloat('gas', 'unit_rate', fallback=0.0),
                # SMETS1 meters report kWh, SMET2 report m^3 and need converting to kWh first
                'conversion_factor': (float(g_vcf) * float(g_cv)) / 3.6 if int(g_meter_type) > 1 else None,
            }
        }
        click.echo(
            f'Retrieving gas data for {from_iso} until {to_iso}...',
            nl=False
        )
        g_consumption = retrieve_paginated_data(
            api_key, g_url, from_iso, to_iso
        )
        click.echo(f' {len(g_consumption)} readings.')
        tags = additional_tags.copy()
        if include_mpan_tag:
            tags |= {'mpan': g_mpan}
        if include_serial_number_tag:
            tags |= {'serial_number': g_serial}
        store_series(influx, 'gas', g_consumption, rate_data['gas'], tags)
    else:
        click.echo('Gas is disabled')


if __name__ == '__main__':
    cmd()
