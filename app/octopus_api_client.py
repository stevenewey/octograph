import json
import os
import re
from types import MappingProxyType
from urllib import parse
from urllib.parse import urlencode

import click
import requests


class OctopusApiClient:
    def __init__(self, api_prefix, api_key, resolution_minutes=30):
        if not api_key:
            raise click.ClickException('No Octopus API key provided')
        self._api_prefix = api_prefix
        self._api_key = api_key
        self._group_by = self._to_group_by(resolution_minutes)

    @staticmethod
    def _to_group_by(resolution: int):
        if resolution == 30:
            return None
        if resolution == 60:
            return 'hour'
        if resolution == 60 * 24:
            return 'day'
        if resolution == 60 * 24 * 7:
            return 'week'
        raise click.ClickException(f'Invalid resolution: {resolution}')

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

    def retrieve_electricity_consumption(self, mpan: str, serial_number: str, from_date: str, to_date: str):
        return self._retrieve_paginated_data(f'electricity-meter-points/{mpan}/meters/{serial_number}/consumption/', from_date, to_date)

    def retrieve_gas_consumption(self, mprn: str, serial_number: str, from_date: str, to_date: str):
        return self._retrieve_paginated_data(f'gas-meter-points/{mprn}/meters/{serial_number}/consumption/', from_date, to_date)

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
