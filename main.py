import requests
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
    cleaned['lastUploadTime'].append(item['lastUploadTime'])
    cleaned['worldID'].append(item['worldID'])
    cleaned['worldName'].append(item['worldName'])

print(d_items)

engine = create_engine("dialect+driver://username:password@host:port/database",echo=True) # creates an Engine object that manages connections., now i need to know how to execute the connection where it sends the imported and cleaned "data" to the DB

# question is, how do i go about doing this, i think i need to run the connection

with engine.connect() as connection:  # this can either execute SQL or insert data, how does it insert data
# what are with statements anyway?

insert = text(
    """

    """
)  # text function allows one to write SQL. This is the SQL you will write.
connection.execute(insert, d_items) # this should hopefully have us send the data over. we will stop here

# print(engine)
