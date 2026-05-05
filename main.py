from extract import extract
from transform import transform
from load import prime_connection, execute_connection
from enrich import enrich_data

def main():
    item_info = extract()
    df_cleaned, df_names = transform(item_info)
    rows_data = df_cleaned.to_dict("records")
    rows_name = df_names.to_dict("records")
    engine = prime_connection()
    null_namedata = execute_connection(rows_data, rows_name, engine)
    enrich_data(null_namedata,engine)
main()

# upload all data to database, merge, insert, update
# merge, insert or update just the nameid into namedata database
# retrieve all namedata from database which has blank itemnames
# iterate through those itemids to get all the namedata complete 
# insert all those itemids into namedata complete
# partition and clean your work
# error handling
# the next thing to do is to scale the project to handle multiple worlds 

# on call it will extract data, during this process it will extract on a loop through all the worlds
# transform will remain the same 
# on load, a third datatable should be made
# one will be the name data table, one will be the most recently updated, it will contain a timestamp and a key, the third will be all imports from beginning of time to the end of time.
# the real question is, does it matter if i include a timestamp of IMPORT or not? there exists a timestamp of last updated already. something to think about.

# name table -> itemname, itemid
# recently imported -> itemid, lastuploadtime, worldid, worldname, importtime, importid
# historical table -> itemid, lastuploaded, worldid, importid,

# this way historical table can remane as lean as possible by relying on ids to join data when needed.

# include financial infomation so

# name table -> itemname, itemid
# recently imported -> itemid, lastuploadtime, worldid, worldname, importtime, importid, gil
# historical table -> itemid, lastuploaded, worldid, importid, gil

# format github repo, readme, etc.