from typing import Any
import requests
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text

# Imports required for the script

response = requests.get('https://universalis.app/api/v2/extra/stats/least-recently-updated?world=louisoix')
data = response.json()
d_items = data['items']

# gets recently updated infromation from the api and stores it in d_items

cleaned = {
    'itemid': [],
    'lastuploadtime': [],
    'worldid': [],
    'worldname': []
}

# Creates dictionary for the cleaned data to be held.

for item in d_items:
    cleaned['itemid'].append(int(item['itemID']))
    cleaned['lastuploadtime'].append(
        datetime.fromtimestamp(
            round(item['lastUploadTime'] / 1000, None))
        )
    cleaned['worldid'].append(item['worldID'])
    cleaned['worldname'].append(item['worldName'])

# Primary data cleaning pass, corrects lastuploadtime format

print(cleaned)

url = "https://v2.xivapi.com/api/sheet/Item/"
# print(cleaned)

# Database which has item name for the corresponding itemid

for i, item in enumerate[Any](cleaned['itemid']) : # why would i class things? later ig.
    n_url = f"{url}{item}?fields=Name"
    response = requests.get(n_url)
    xiv_data = response.json()
    cleaned['itemid'][i] = xiv_data['fields']['Name']
  
 # takes a long time to execute. i should find a way to execute all urls all at once.
 # im thinking, build the url list and then execute the list like that.
 # loops through each item in the itemid list and replaces itemid with their proper names


# commented to reduce loading time for now
# print(cleaned['worldid'])

df_cleaned = pd.DataFrame(cleaned)
print(df_cleaned)


# When turning it into a dataframe WORLDID dissapears. leave for later                     
# Data has been cleaned. time to load the data into PostGres

engine = create_engine("postgresql://neondb_owner:npg_3R2XoTSwUrtD@ep-billowing-boat-abutsytw-pooler.eu-west-2.aws.neon.tech/neondb?sslmode=require&channel_binding=require",echo = True)

# creates engine which can connect to the DB.

create_db = text (
    """CREATE SCHEMA IF NOT EXISTS xiv_data
;

CREATE TABLE IF NOT EXISTS xiv_data.raw_data (
    itemid VARCHAR(50) PRIMARY KEY,
    lastuploadtime VARCHAR(50),
    worldid INTEGER,
    worldname VARCHAR(50) 
);"""
)

# Creates DB Schema and table

with engine.begin() as conn:
    conn.execute(create_db)

# Creates DB in postgres if it does not exist already.

# now i need to insert the data into the datatables

df_cleaned.to_sql( 
    name="raw_data",
    con=engine, 
    schema="xiv_data", 
    if_exists="append", 
    index=False )

