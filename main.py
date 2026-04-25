from gettext import install
import requests
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text
response = requests.get('https://universalis.app/api/v2/extra/stats/least-recently-updated?world=louisoix')
data = response.json()
d_items = data['items']

cleaned = {
    'itemID': [],
    'lastUploadTime': [],
    'worldID': [],
    'worldName': []
}

for item in d_items:
    cleaned['itemID'].append(item['itemID'])
    cleaned['lastUploadTime'].append(round(item['lastUploadTime'] / 1000, None))
    cleaned['worldID'].append(item['worldID'])
    cleaned['worldName'].append(item['worldName'])


dt = [datetime.fromtimestamp(item).strftime("%Y-%m-%d %H:%M:%S")
    for item in cleaned['lastUploadTime']
]
cleaned['lastUploadTime'] = dt
print(cleaned['lastUploadTime'])


# now i can create db sql script. fdfds



"""
# now i need to create an engine
engine = create_engine("postgresql://neondb_owner:npg_3R2XoTSwUrtD@ep-billowing-boat-abutsytw-pooler.eu-west-2.aws.neon.tech/neondb?sslmode=require&channel_binding=require")

# ive made the engine make a connection, now i need to load the sql into the database.

load_d_items = text (
    "insert into "
)


# before i do that i need to create a sql script to put into my DB.
#  i wonder if there is a way for me to load the sql scipt from python into the DB. im sure it is possible 
"""
