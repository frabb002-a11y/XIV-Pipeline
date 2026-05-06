from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import os
from config import DATABASE_URL, dev_overiding
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger(__name__)

def prime_connection(db_url: str | None = None, ECHO: bool = False, connect_timeout_s: int = 5) -> Engine:
    log.info("Creating engine to connect to database.")
    if db_url is None:
        db_url = os.getenv("DATABASE_URL")
    log.info("Fetching database url.")
    try:
        engine = create_engine(db_url, echo=ECHO, connect_args = {"connect_timeout": connect_timeout_s})
        return engine
    except Exception as e:
        print(e)
        log.info("Cannot fetch database url, Running Local mode.")
        try:
            engine = create_engine(DATABASE_URL(), echo=ECHO, connect_args = {"connect_timeout": connect_timeout_s})
            return engine
        except Exception as e2:
            log.error("internal database url is invalid, please check password.py.")
            raise ValueError()


def execute_connection(rows_data: list[dict] | None = None, rows_name: list[dict]| None = None,engine: Engine| None = None, dev_mode : ANY | None = None):
    
    if dev_mode is None:
        dev_mode = dev_overiding()
    if rows_data is None:
        rows_data = []
    if rows_name is None:
        rows_name = []
    if engine is None:
        engine = prime_connection()

    log.info("Creating database.")

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
            log.info("Connection to database successful.")
    except Exception:
        raise ValueError("DB connection failed (likely bad password.)")
    
    with engine.begin() as conn:
        if dev_mode == True:        
            log.info("devmode: resetting database.")
            conn.execute(reset)
        log.info("creating database and datatables.")
        conn.execute(create_db)
        log.info("inserting table data.")
        conn.execute(insert_raw_data, rows_data)
        conn.execute(insert_name_data, rows_name)
        log.info("Pruning data with no itemnames.")
        response = conn.execute(extract_namedata)
        return response.mappings().all()


if __name__ == "__main__":
    execute_connection()
