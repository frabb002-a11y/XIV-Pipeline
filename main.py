from config import dev_mode, dev_overiding
from extract import extract
from transform import transform
from load import prime_connection, execute_connection
from enrich import enrich_data
from _1extract import extract_marketable
import logging


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    force=True,
)
log = logging.getLogger(__name__)


def main():
    dev_mode = dev_overiding(True)
    extract_marketable()
    item_info = extract()
    df_cleaned, df_names, df_worlds = transform(item_info)
    rows_data = df_cleaned.to_dict("records")
    rows_name = df_names.to_dict("records")
    rows_worlds = df_worlds.to_dict("records")
    engine = prime_connection()
    null_namedata = execute_connection(rows_data, rows_name, rows_worlds, engine, dev_mode)
    enrich_data(null_namedata, engine)
    print(df_cleaned)


if __name__ == "__main__":
    main()




# upload all data to database, merge, insert, update
# merge, insert or update just the nameid into namedata database
# retrieve all namedata from database which has blank itemnames
# iterate through those itemids to get all the namedata complete 
# insert all those itemids into namedata complete
# partition and clean your work
# error handling
# scale the project to handle multiple worlds
# logging
# run tracking, retry, remove drops,
# README
# Architecture
# How to run it
# Repository structure (clean it up slightly)

# include financial infomation so

# name table -> itemname, itemid
# recently imported -> itemid, lastuploadtime, worldid, worldname, importtime, importid, gil
# historical table -> itemid, lastuploaded, worldid, importid, gil

# format github repo, readme, etc.