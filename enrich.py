from concurrent.futures import ThreadPoolExecutor
import time
import requests
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine
from load import prime_connection
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    force=True,
)
log = logging.getLogger(__name__)

def enrich_data(null_namedata: list[dict] | None = None, engine: Engine | None = None):

    if null_namedata is None:
        null_namedata = []

    if engine is None:
        engine = prime_connection()
        
    updated_namedata = {
        'itemid': [],
        'itemname' : []
    }

    for item in null_namedata:
        updated_namedata['itemid'].append(item['itemid'])

    log.info("fetching name data for unknown names, this may take a few seconds...")

    def fetch_name(itemid):
        url = f"https://v2.xivapi.com/api/sheet/Item/{itemid}?fields=Name"
        response = requests.get(url)
        ddata = response.json()
        return ddata['fields']['Name']

    with ThreadPoolExecutor(20) as executioner:
        start = time.time()
        updated_namedata['itemname'] = list(executioner.map(fetch_name,updated_namedata['itemid']))
        end = time.time()
        execution = f"execution time: {round((end - start), 2)} seconds"
        log.info(execution)

    log.info("updating namedata.")

    df_updated_namedata = pd.DataFrame(updated_namedata)
    rows_inserting_namedata = df_updated_namedata.to_dict("records")


    insert_new_names = text(
        """
        MERGE INTO xiv_data.id_names AS t
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

    log.info("namedata updated and uploaded to database.")
    with engine.begin() as conn:
        conn.execute(insert_new_names,rows_inserting_namedata)
    log.info("complete.")

if __name__ == "__main__":
    enrich_data()

