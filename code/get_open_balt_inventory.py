#! ../venv/bin/python
# coding: utf-8
from datetime import datetime, timedelta

# # Open Baltimore Data Inventory

# This script will produce a dataframe of all data layers and other resources hosted
# on Open Baltimore (https://data.baltimorecity.gov/). For each resource, there will
### be information on:
###   title - title
###   URL - landingPage
###   data origin - publisher.name
###   related keywords - keyword
###   create date - issued
###   last updated date - modified
###   API endpoint for the data layer (if exists) - geo_api
###   and more.


### load a few libraries

import requests
import urllib3
import ssl
import pandas as pd
import numpy as np
import re
from multiprocessing import Pool


# Constants
CITY_DATA_URL = 'https://data.baltimorecity.gov/data.json'
NUM_PROCESSES = 10


# this function will isolate ESRI open baltimore api endpoints
# from one if the existing columns. only working ESRI API endpoints
# will be returned, and they need to include 'FeatureServer' in the url
# and end with a number
def getApiUrl(json_ob):
    try:
        info_as_lst = [d['accessURL'] for d in json_ob if d['title'] == "ArcGIS GeoService"]
        api_url = info_as_lst[0]
        # the last character should be a digit, indicating that it's a numbered feature layer
        if (not api_url[-1].isdigit()) or re.search("FeatureServer", api_url) is None:
            raise ValueError('A very specific bad thing happened.')
        return info_as_lst[0] + '/query?outFields=*&where=1%3D1&f=geojson'
    except:
        return np.nan


def is_endpoint_alive(endpoint):
    return fetch_url(endpoint) is not None


# Is the 'modified' datetime column older than 90 days ago?
# t
def is_data_fresh(pd_timestamp):
    now = datetime.utcnow()
    parsed_time = datetime(year=pd_timestamp.year, month=pd_timestamp.month, day=pd_timestamp.day)
    return now > parsed_time + timedelta(days=90)


def alert_open_baltimore():
    print('Alert!')

class CustomHttpAdapter (requests.adapters.HTTPAdapter):
    # "Transport adapter" that allows us to use custom ssl_context.
    def __init__(self, ssl_context=None, **kwargs):
        self.poolmanager = None
        self.ssl_context = ssl_context
        super().__init__(**kwargs)

    def init_poolmanager(self, connections, maxsize, block=False):
        self.poolmanager = urllib3.poolmanager.PoolManager(
            num_pools=connections, maxsize=maxsize,
            block=block, ssl_context=self.ssl_context)


# This adaptor is necessary for the 'requests' library
# to connect to the data servers running old ssl versions (which should really be fixed)

def get_legacy_session():
    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    ctx.options |= 0x4  # OP_LEGACY_SERVER_CONNECT
    session = requests.session()
    session.mount('https://', CustomHttpAdapter(ctx))
    return session

# Try to grab an endpoint via a HEAD request
# Return the response if possible, otherwise return None


def fetch_url(url_string):
    try:
        # Try a HEAD request to minimize data
        response = get_legacy_session().head(url_string)
        response.raise_for_status()  # Raise an HTTPError for bad status codes
        return response
    except requests.exceptions.HTTPError as e:
        # Some servers don't support a HEAD request, so try GET instead....
        if e.response.status_code == 405:
            print("HTTP 405 Method Not Allowed")
            try:
                response = get_legacy_session().get(url_string)
                response.raise_for_status()  # Raise an HTTPError for bad status codes
                return response
            except Exception:
                return None
        else:
            print(f"An HTTP error occurred: {e}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"A request exception occurred: {e}")
        return None


# Parallel process the rows of data, returned the aggregated results
def endpoints_health_check(data):
    # Create a pool of processes
    with Pool(processes=NUM_PROCESSES) as pool:
        # Parallel process the rows of data
        results = pool.map(process_endpoint_health, [row for index, row in data.iterrows()])
    return results


# Function to process a single data endpoint row
def process_endpoint_health(row):
    modified_at = row['modified']
    endpoint = row['geo_api']
    check1 = is_endpoint_alive(endpoint)
    check2 = is_data_fresh(modified_at)
    return [endpoint, check1, check2]


# Handling a given endpoint health result...
# check1 is the endpoint liveness check
# check2 is the data freshness check
def process_endpoint_health_result(endpoint_id, check1, check2):
    if check1 is False or check2 is False:
        print(f'{endpoint_id} failed health check.')
        alert_open_baltimore()


if __name__ == '__main__':
    # get all datasets currently on Open Baltimore
    # start with getting a json of the datasets
    res = requests.get(CITY_DATA_URL)
    d = res.json()

    meta = [
        "@type",
        "accessLevel",
        ["contactPoint", ["@type"]],
        ["contactPoint", "fn"],
        ["contactPoint", "hasEmail"],
        "description",
        ["distribution", "@type"],
        ["distribution", "accessURL"],
        ["distribution", "format"],
        ["distribution", "mediaType"],
        ["distribution", "title"],
        "identifier",
        "issued",
        "keyword",
        "landingPage",
        "license",
        "modified",
        ["publisher", "name"],
        "spatial",
        "theme",
        "title"
    ]

    # convert to pandas dataframe
    ob_inventory = pd.json_normalize(d['dataset'], meta=meta)

    # extract the ESRI ID from arcgis URL for each dataset
    id_url = 'https://www.arcgis.com/home/item.html?id='
    ob_inventory['layer_id'] = [x.replace(id_url, '') for x in ob_inventory['identifier']]
    ob_inventory['layer_id'] = [x.split('&',1)[0] for x in ob_inventory['layer_id']]

    # add column of working API endpoints, value will be null if one doesn't exist
    # for the layer. These endpoint can be further modified with parameters
    # to filter records
    ob_inventory['geo_api'] = ob_inventory['distribution'].map(lambda a: getApiUrl(a))

    # change date fields from strings into datetime
    ob_inventory['modified'] = pd.to_datetime(ob_inventory['modified'].str.slice(0, 10))
    ob_inventory['issued'] = pd.to_datetime(ob_inventory['issued'].str.slice(0, 10))

    # remove columns that aren't that useful
    ob_inventory = ob_inventory.drop(['@type', 'identifier', 'accessLevel', 'contactPoint.@type',
    'contactPoint.hasEmail', 'license'], axis=1)
    # print(ob_inventory)
    # ob_inventory.to_excel('ob_inventory_entire.xlsx', index=False)

    # isolate full, core datasets maintained by Baltimore City
    city_datasets = ob_inventory[pd.notnull(ob_inventory["geo_api"])]
    city_datasets = city_datasets[city_datasets['publisher.name'] == 'Baltimore City']
    # city_datasets.to_excel('ob_inventory_city_datasets_only.xlsx', index=False)

    # Parse data relevant to endpoint health check
    endpoint_data = city_datasets[['modified', 'geo_api']]
    # Get the results for the health check
    health_check_results = endpoints_health_check(endpoint_data)
    # Process the results
    [process_endpoint_health_result(endpoint, check1, check2) for [endpoint, check1, check2] in health_check_results]


