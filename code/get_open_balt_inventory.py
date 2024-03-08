#!/usr/bin/env python
# coding: utf-8

# # Open Baltimore Data Inventory

### This script will produce a dataframe of all data layers and other resources hosted 
### on Open Baltimore (https://data.baltimorecity.gov/). For each resource, there will 
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
import pandas as pd
import numpy as np
import json
import datetime as dt
import numpy as np
import re


### get all datasets currently on Open Baltimore
## start with getting a json of the datasets

url = 'https://data.baltimorecity.gov/data.json'
res = requests.get(url)



d = res.json()


meta = ["@type",
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


### convert to pandas dataframe

ob_inventory = pd.json_normalize(d['dataset'], meta=meta)


### extract the ESRI ID from arcgis URL for each dataset

id_url = 'https://www.arcgis.com/home/item.html?id='
ob_inventory['layer_id'] = [x.replace(id_url, '') for x in ob_inventory['identifier']]
ob_inventory['layer_id'] = [x.split('&',1)[0] for x in ob_inventory['layer_id']]



### this function will isolate ESRI open baltimore api endpoints 
### from one if the existing columns. only working ESRI API endpoints
### will be returned, and they need to include 'FeatureServer' in the url
### and end with a number

def getApiUrl(json_ob):
  try:
    info_as_lst = [d['accessURL'] for d in json_ob if d['title'] == "ArcGIS GeoService"]
    api_url = info_as_lst[0]
    ### the last character should be a digit, indicating that it's a numbered feature layer
    if (not api_url[-1].isdigit()) or re.search("FeatureServer", api_url) is None :
      raise ValueError('A very specific bad thing happened.')
    return info_as_lst[0] + '/query?outFields=*&where=1%3D1&f=geojson'
  except:
    return np.nan


## add column of working API endpoints, value will be null if one doesnt exist
## for the layer. These endpoint can be further modified with parameters 
## to filter records

ob_inventory['geo_api'] = ob_inventory['distribution'].map(lambda a: getApiUrl(a))


## change date fields from strings into datetime
ob_inventory['modified'] = pd.to_datetime(ob_inventory['modified'].str.slice(0, 10))
ob_inventory['issued'] = pd.to_datetime(ob_inventory['issued'].str.slice(0, 10))


## remove columns that arent that useful
ob_inventory = ob_inventory.drop(['@type', 'identifier', 'accessLevel', 'contactPoint.@type', 
'contactPoint.hasEmail', 'license'], axis=1)



print(ob_inventory)
ob_inventory.to_excel('ob_inventory_entire.xlsx', index=False)



#### isolate full, core datasets mantained by Baltimore City

city_datasets = ob_inventory[pd.notnull(ob_inventory["geo_api"])]
city_datasets = city_datasets[city_datasets['publisher.name'] == 'Baltimore City']


print(city_datasets)
city_datasets.to_excel('ob_inventory_city_datasets_only.xlsx', index=False)
