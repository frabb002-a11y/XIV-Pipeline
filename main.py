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

def second():

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


    # Def function

    # main
        # main called Extract, then Transform, then Load, then Enrichment, then Load
    # Multiple files

    something like 

    def main():
        extract = run_extract
        transform = run_transform(extract)
        load =
        
    main()

    lets divide this project before we decide how to define main.


    """

