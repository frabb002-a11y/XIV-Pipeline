from numpy import extract
import requests
import time
from datetime import datetime
import pandas as pd
import os
from password import DATABASE_URL
from sqlalchemy import create_engine, text
from concurrent.futures import ThreadPoolExecutor

# Imports required for the script

response = requests.get('https://universalis.app/api/v2/extra/stats/most-recently-updated?world=louisoix')
data = response.json()
d_items = data['items']
# gets recently updated infromation from the api and stores it in d_items

cleaned = {
    'itemid': [],
    'lastuploadtime': [],
    'worldid': [],
    'worldname' : []
}

names = {
    'itemid': [],
    'name' : [],
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

names['itemid'] = cleaned['itemid']
# Primary data cleaning pass, corrects lastuploadtime format

url = "https://v2.xivapi.com/api/sheet/Item/"

# Database which has item name for the corresponding itemid

def fetch_name(item_id):
    url = f"https://v2.xivapi.com/api/sheet/Item/{item_id}?fields=Name"
    response = requests.get(url)
    data = response.json()
    print(response.status_code)
    return data['fields']['Name']

with ThreadPoolExecutor(20) as executioner:
    start = time.time()
    names['name'] = list(executioner.map(fetch_name,cleaned['itemid']))
    end = time.time()
    execution = f"execution time: {round((end - start), 2)} seconds"
    print(execution)

# completed async

df_cleaned = pd.DataFrame(cleaned)
df_names = pd.DataFrame(names)

print(df_names)
print(df_cleaned)
# create a schema for names. you can either pregenerate all the names and their associated itemid and then merge it in the db to prevent need for calling.
# or if the name does not exist in the database in the name schema then run a call to generate that name, populate the name schema and then merge


# Create schema names


# When turning it into a dataframe WORLDID dissapears. leave for later                     
# Data has been cleaned. time to load the data into PostGres
try:
    engine = create_engine(os.getenv("DATABASE_URL"), echo=False)
except Exception as e:
    print("Cannot retrieve database url, Running Local mode:")
    engine = create_engine(DATABASE_URL(), echo=False)
# creates engine which can connect to the DB.

create_db = text ("""
   CREATE SCHEMA IF NOT EXISTS xiv_data
;

CREATE TABLE IF NOT EXISTS xiv_data.raw_data (
    itemid INT PRIMARY KEY,
    lastuploadtime VARCHAR(50),
    worldid INTEGER,
    worldname VARCHAR(50) 
);

CREATE TABLE IF NOT EXISTS xiv_data.raw_names (
    itemid INT PRIMARY KEY,
    itemname VARCHAR(50)
);
""")
# Creates DB Schema and two tabbles, raw data and name data.


rows = df_cleaned.to_dict("records")
print(rows)

insert_data = text ("""
  MERGE INTO xiv_data.raw_data t
    USING (
    VALUES (:itemid, :lastuploadtime, :worldid, :worldname)
    ) AS s (itemid, lastuploadtime, worldid, worldname)
ON t.itemid = s.itemid

WHEN MATCHED THEN
    UPDATE SET
        lastuploadtime = s.lastuploadtime,
        worldid = s.worldid,
        worldname = s.worldname

WHEN NOT MATCHED THEN
    INSERT (itemid, lastuploadtime, worldid, worldname)
    VALUES (s.itemid, s.lastuploadtime, s.worldid, s.worldname);

""")
"""
extract_name_data = text (
select *
from xiv_data.raw_names
)
name_data = extract_name_data.json()
print(name_data)
"""

with engine.begin() as conn:
    conn.execute(create_db)
    conn.execute(insert_data, rows)

# the second is more complex. it needs python to retrieve data from the db and then see if the data exists, if not then it generates. i think while the second is more complicated it would be more intereesting. 
# i completed a local running version, password is retrieved from password.py. i created a second datatable full of names. this will set us up for "filling" only if data exists in the DB.