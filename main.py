from typing import Any
from gettext import install
import requests
import http.client
from datetime import datetime
import pandas as pd
from pprint import pprint
from sqlalchemy import create_engine, text

# Imports required for the script

response = requests.get('https://universalis.app/api/v2/extra/stats/least-recently-updated?world=louisoix')
data = response.json()
d_items = data['items']

# gets recently updated infromation from the api and stores it in d_items

cleaned = {
    'itemID': [],
    'lastUploadTime': [],
    'worldID': [],
    'worldName': []
}

# Creates dictionary for the cleaned data to be held.

for item in d_items:
    cleaned['itemID'].append(int(item['itemID']))
    cleaned['lastUploadTime'].append(
        datetime.fromtimestamp(
            round(item['lastUploadTime'] / 1000, None)).strftime("%Y-%m-%d %H:%M:%S")
        )
    cleaned['worldID'].append(item['worldID'])
    cleaned['worldName'].append(item['worldName'])

# Primary data cleaning pass, corrects lastuploadtime format

url = "https://v2.xivapi.com/api/sheet/Item/"
print(cleaned)
# Database which has item name for the corresponding ItemID

for i, item in enumerate[Any](cleaned['itemID']) : # why would i class things? later ig.
    n_url = f"{url}{item}?fields=Name"
    response = requests.get(n_url)
    xiv_data = response.json()
    cleaned['itemID'][i] = xiv_data['fields']['Name']
  
 # takes a long time to execute. i should find a way to execute all urls all at once.
 # im thinking, build the url list and then execute the list like that.
 # loops through each item in the ItemID list and replaces itemID with their proper names

df_cleaned = pd.DataFrame(cleaned)
print(df_cleaned)

# Data has been cleaned. time to load the data into PostGres

engine = create_engine("postgresql://neondb_owner:npg_3R2XoTSwUrtD@ep-billowing-boat-abutsytw-pooler.eu-west-2.aws.neon.tech/neondb?sslmode=require&channel_binding=require",echo = True)

# creates engine which can connect to the DB.

create_db = text (
    """CREATE SCHEMA IF NOT EXISTS xiv_data
;

CREATE TABLE IF NOT EXISTS xiv_data.raw_data (
    itemID VARCHAR(50) PRIMARY KEY,
    lastUploadTime VARCHAR(50),
    worldID INTEGER,
    worldName VARCHAR(50) 
);"""
)

# Creates DB Schema and table

with engine.begin() as conn:
    conn.execute(create_db)

# Creates DB in postgres if it does not exist already.

# now i need to insert the data into the datatavbkles
"""
# before i do that i need to create a sql script to put into my DB.
#  i wonder if there is a way for me to load the sql scipt from python into the DB. im sure it is possible 
"""
