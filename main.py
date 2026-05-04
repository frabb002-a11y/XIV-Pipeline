from numpy import extract
from pandas.core.internals.blocks import new_block
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
    'itemname' : [],
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
names['itemname'] = ''
# Primary data cleaning pass, corrects lastuploadtime format


# Database which has item name for the corresponding itemid

# completed async

df_cleaned = pd.DataFrame(cleaned)
df_names = pd.DataFrame(names)

# create a schema for names. you can either pregenerate all the names and their associated itemid and then merge it in the db to prevent need for calling.
# or if the name does not exist in the database in the name schema then run a call to generate that name, populate the name schema and then merge


# Create schema names


# When turning it into a dataframe WORLDID dissapears. leave for later                     
# Data has been cleaned. time to load the data into PostGres

ECHO = False
try:
    engine = create_engine(os.getenv("DATABASE_URL"), echo=ECHO)
except Exception as e:
    print("Cannot retrieve database url, Running Local mode:")
    engine = create_engine(DATABASE_URL(), echo=ECHO)
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

CREATE TABLE IF NOT EXISTS xiv_data.name_data (
    itemid INT PRIMARY KEY,
    itemname VARCHAR(100)
);
""")
# Creates DB Schema and two tabbles, raw data and name data.


rows_data = df_cleaned.to_dict("records")

insert_raw_data = text ("""
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
"""
)

rows_name = df_names.to_dict("records")

insert_name_data = text("""
    MERGE INTO xiv_data.name_data AS t
    USING (
        VALUES (:itemid)
    ) AS s (itemid)
    ON t.itemid = s.itemid

    WHEN MATCHED THEN
        UPDATE SET
            itemid = s.itemid

    WHEN NOT MATCHED THEN
        INSERT (itemid)
        VALUES (s.itemid);
""")

extract_name_data = text (
    """
select *
from xiv_data.name_data
where itemname IS NULL
""")

reset = text ( """
DROP TABLE IF EXISTS xiv_data.name_data;
DROP TABLE IF EXISTS xiv_data.raw_data;
"""
)
with engine.begin() as conn:
    conn.execute(create_db)
    conn.execute(insert_raw_data, rows_data)
    conn.execute(insert_name_data, rows_name)
    response = conn.execute(extract_name_data)
    data = response.mappings().all()



new_names = {
    'itemid': [],
    'itemname' : []
}


def fetch_name(itemid):
    url = f"https://v2.xivapi.com/api/sheet/Item/{itemid}?fields=Name"
    response = requests.get(url)
    ddata = response.json()
    return ddata['fields']['Name']


for item in data:
    new_names['itemid'].append(item['itemid'])

with ThreadPoolExecutor(20) as executioner:
    start = time.time()
    new_names['itemname'] = list(executioner.map(fetch_name,new_names['itemid']))
    end = time.time()
    execution = f"execution time: {round((end - start), 2)} seconds"
    print(execution)


df_new_names = pd.DataFrame(new_names)
rows_new_names = df_new_names.to_dict("records")


insert_new_names = text(
    """
    MERGE INTO xiv_data.name_data AS t
    USING (
        VALUES (:itemid, :itemname)
    ) AS s (itemid, itemname)
    ON t.itemid = s.itemid

    WHEN MATCHED THEN
        UPDATE SET
            itemid = s.itemid,
            itemname = s.itemname

    WHEN NOT MATCHED THEN
        INSERT (itemid,itemname)
        VALUES (s.itemid,s.itemname);
    """
)

query_filled_names = text(
    """select *
    from xiv_data.name_data
    """
)

with engine.begin() as conn:
    conn.execute(insert_new_names,rows_new_names)
    new_data = conn.execute(query_filled_names)
    data = new_data.fetchall()

"""
print(new_names)

df_ddata = pd.DataFrame(data)


rows_ddata = df_cleaned.to_dict("records")
print(rows_ddata)
insert_rows_ddata = text (
    INSERT INTO xiv_data.name_data
        (itemid, itemname)
        VALUES (:itemid, :itemname);
)

with engine.begin() as conn:
    conn.execute(insert_rows_ddata,rows_ddata)


# upload all data to database, merge, insert, update
# merge, insert or update just the nameid into namedata database
# retrieve all namedata from database which has blank itemnames
# iterate through those itemids to get all the namedata complete 
# insert all those itemids into namedata complete
# partition and clean your work

# Multiple files
# Def function

"""