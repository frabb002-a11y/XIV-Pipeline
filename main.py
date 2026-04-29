import requests
import time
from datetime import datetime
import pandas as pd
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

ma,es
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

url = "https://v2.xivapi.com/api/sheet/Item/"

# Database which has item name for the corresponding itemid

def fetch_name(item_id):
    url = f"https://v2.xivapi.com/api/sheet/Item/{item_id}?fields=Name"
    response = requests.get(url)
    data = response.json()
    print(response.status_code)
    return data['fields']['Name']

with ThreadPoolExecutor(len(cleaned['itemid'])) as executioner:
    start = time.time()
    names = list(executioner.map(fetch_name,cleaned['itemid']))
    end = time.time()
    execution = f"execution time: {round((end - start), 2)} seconds"
    print(execution)

# completed async

df_cleaned = pd.DataFrame(cleaned)
df_names = pd.DataFrame(names)

# create two schemas and then join

"""
# When turning it into a dataframe WORLDID dissapears. leave for later                     
# Data has been cleaned. time to load the data into PostGres

engine = create_engine("postgresql://neondb_owner:npg_3R2XoTSwUrtD@ep-billowing-boat-abutsytw-pooler.eu-west-2.aws.neon.tech/neondb?sslmode=require&channel_binding=require",echo=False)

# creates engine which can connect to the DB.

create_db = text (
   CREATE SCHEMA IF NOT EXISTS xiv_data
;

CREATE TABLE IF NOT EXISTS xiv_data.raw_data (
    itemid INT PRIMARY KEY,
    itemname VARCHAR(50),
    lastuploadtime VARCHAR(50),
    worldid INTEGER,
    worldname VARCHAR(50) 
);
)
# Creates DB Schema and table

rows = df_cleaned.to_dict("records")
# print(rows)

insert_data = text (
  MERGE INTO xiv_data.raw_data t
    USING (
    VALUES (:itemid, :itemname, :lastuploadtime, :worldid, :worldname)
    ) AS s (itemid, itemname, lastuploadtime, worldid, worldname)
ON t.itemid = s.itemid

WHEN MATCHED THEN
    UPDATE SET
        lastuploadtime = s.lastuploadtime,
        worldid = s.worldid,
        worldname = s.worldname

WHEN NOT MATCHED THEN
    INSERT (itemid, itemname, lastuploadtime, worldid, worldname)
    VALUES (s.itemid, s.itemname, s.lastuploadtime, s.worldid, s.worldname);

)

with engine.begin() as conn:
    conn.execute(create_db)
    conn.execute(insert_data, rows)
"""