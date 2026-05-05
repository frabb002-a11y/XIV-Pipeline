from concurrent.futures import ThreadPoolExecutor
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from password import DATABASE_URL
import time
import os

def prime_connection(db_url = os.getenv("DATABASE_URL"), ECHO = False) :
    try:
        engine = create_engine(db_url, echo=ECHO)
        return engine
    except Exception as e:
        print(e)
        print("Cannot retrieve database url, Running Local mode")
        try:
            engine = create_engine(DATABASE_URL(), echo=ECHO)
            return engine
        except Exception as e2:
            raise ValueError("internal database url is invalid, please check password.py")


def execute_connection(rows_data: list[dict], rows_name: list[dict],engine: Engine):

    reset = text ( """
    DROP TABLE IF EXISTS xiv_data.name_data;
    DROP TABLE IF EXISTS xiv_data.raw_data;
    """
    )
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

    extract_namedata = text (
        """
    select *
    from xiv_data.name_data
    where itemname IS NULL
    """)

    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception:
        raise ValueError("DB connection failed (likely bad password)")
    
    with engine.begin() as conn:
        conn.execute(reset)
        conn.execute(create_db)
        conn.execute(insert_raw_data, rows_data)
        conn.execute(insert_name_data, rows_name)
        response = conn.execute(extract_namedata)
        return response.mappings().all()
