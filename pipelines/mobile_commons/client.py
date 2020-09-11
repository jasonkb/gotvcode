import time
from typing import List, Dict

import pandas as pd
import requests

from mobilecommons.utils import (
    is_error_response,
    xml_response_to_data,
)
from mobilecommons.resources import RESOURCES
from shopify.client import ShopifyAPI

API_BASE_URL = 'https://secure.mcommons.com/api/'
DataBatch = Dict[str, pd.DataFrame]


class MobileCommonsAPIException(Exception):
    pass


class MobileCommonsAPI(ShopifyAPI):
    def __init__(self, api_username, api_password, schema, database):
        self.session = requests.Session()
        self.session.auth = (api_username, api_password)
        self.database = database
        self.schema = schema
        self.base_url = API_BASE_URL
        self.resources = self.resource_defaults(RESOURCES)

    def download(self, resource: str, start: str = None, end: str = None, start_page: int = None, end_page: int = None) -> DataBatch:
        """ Downloads the datasets """

        print(f'Downloading {resource}')
        res = self.resources[resource]

        if start_page or end_page:
            print(f'Downloading page range [{start_page}, {end_page})')
        if start or end:
            print(f'{start} to {end}')

        data = self.download_data(res, start, end, start_page, end_page)
        db = self.data_to_data_batch(data, res)

        for k, v in db.items():
            print(f'{k}: {len(v)} rows')

        return db

    def download_data(self, resource: Dict, start: str = None, end: str = None, start_page: int = 1, end_page: int = None) -> List[Dict]:
        """ Downloads data for a single Mobile Commons resource """

        page = start_page

        all_data = []

        params = resource['params'].copy()
        params['limit'] = self.limit

        if start and resource['from']:
            params[resource['from']] = start

        if end and resource['to']:
            params[resource['to']] = end

        fails = 0

        while True:
            if end_page and page >= end_page:
                break
            time.sleep(fails * self.retry_wait)

            params['page'] = page
            url = self.base_url + resource['endpoint']
            print(f'Requesting {url} with params {params}')

            try:
                resp = self.session.get(url, params=params)
                resp.raise_for_status()
                error = is_error_response(resp)

                if error:
                    raise MobileCommonsAPIException(f'Error with Mobile Commons API. Error code {error}. Response: {resp.text}')

                data = xml_response_to_data(resp.text, resource)

                data = data.get(resource['key'], [])

                all_data.extend(data)

                if len(data) < self.limit:
                    break

                if resp.status_code in [429]:
                    print('429: Rate limit - waiting 2 seconds')
                    time.sleep(2)
                    continue

                resp.raise_for_status()

            except (
                    requests.exceptions.RequestException, MobileCommonsAPIException
                ) as E:
                fails += 1
                print(E)
                if fails > self.retry_limit:
                    raise

            page += 1

        return all_data


    def create_or_update_profile(self, payload):
        url = self.base_url + 'profile_update'
        resp = self.session.post(url, data=payload)

        error = is_error_response(resp)
        if error:
            raise MobileCommonsAPIException(f'Error with Mobile Commons API. Error code {error}. Response: {resp.text}')

        #print(f'Created/update profile: {resp.text}')
