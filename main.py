import requests
import pandas as pd
from sqlalchemy import engine
response = requests.get('https://universalis.app/api/v2/extra/stats/least-recently-updated?world=louisoix')
data = response.json()
d_items = data['items']

cleaned = {
        'itemID': [],
        'lastUploadTime': [],
        'worldID': [], 
        'worldName': []
    } 

for item in d_items :
    cleaned['itemID'].append(item['itemID'])
    cleaned['lastUploadTime'].append(item['lastUploadTime'])
    cleaned['worldID'].append(item['worldID'])
    cleaned['worldName'].append(item['worldName'])

 # so ive extracted, ive transformed now i need to load. but before that, why not turn it into a datatable?

df_cleaned = pd.DataFrame(cleaned)

print(df_cleaned)

