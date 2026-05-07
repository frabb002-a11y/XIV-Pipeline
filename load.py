from typing import Any
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import os
from config import dev_overiding
from password import DATABASE_URL
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger(__name__)

def prime_connection(db_url: str | None = None, ECHO: bool = False, connect_timeout_s: int = 5) -> Engine:
    log.info('Creating engine to connect to database.')
    if db_url is None:
        db_url = os.getenv("DATABASE_URL")
    log.info('Fetching database URL.')
    try:
        engine = create_engine(db_url, echo=ECHO, connect_args = {"connect_timeout": connect_timeout_s})
        return engine
    except Exception as e:
        log.info('Cannot fetch database URL. Running local mode.')
        try:
            engine = create_engine(DATABASE_URL(), echo=ECHO, connect_args = {"connect_timeout": connect_timeout_s})
            return engine
        except Exception as e2:
            log.error('Internal database URL is invalid. Please check password.py.')
            raise ValueError()


def execute_connection(
    rows_data: list[dict] | None = None,
    rows_name: list[dict] | None = None,
    rows_worlds: list[dict] | None = None,
    rows_finance: list[dict] | None = None,
    engine: Engine | None = None,
    dev_mode: Any | None = None,
):
    
    if dev_mode is None:
        dev_mode = dev_overiding()
    if rows_data is None:
        rows_data = []
    if rows_name is None:
        rows_name = []
    if rows_worlds is None:
        rows_worlds = []
    if rows_finance is None:
        rows_finance = []
    if engine is None:
        engine = prime_connection()

    log.info('Creating database.')

    reset = text ( """
    DROP TABLE IF EXISTS xiv_data.id_names;
    DROP TABLE IF EXISTS xiv_data.recently_updated;
    DROP TABLE IF EXISTS xiv_data.world_names;
    DROP TABLE IF EXISTS xiv_data.finance_data;
    """
    )
    create_db = text ("""
    CREATE SCHEMA IF NOT EXISTS xiv_data
    ;

    CREATE TABLE IF NOT EXISTS xiv_data.world_names (
        worldid INTEGER PRIMARY KEY,
        worldname VARCHAR(50)
    );

    CREATE TABLE IF NOT EXISTS xiv_data.recently_updated (
        worldid INTEGER,
        itemid INT,
        lastuploadtime TIMESTAMP,
        ingested_at TIMESTAMP,
        PRIMARY KEY (worldid, itemid)
    );

    CREATE TABLE IF NOT EXISTS xiv_data.id_names (
        itemid INT PRIMARY KEY,
        itemname VARCHAR(100)
    );

    CREATE TABLE IF NOT EXISTS xiv_data.finance_data (
        worldid INTEGER,
        itemid INT,
        minlisting_price REAL,
        recentpurchase_price REAL,
        average_sale_price REAL,
        daily_sale_velocity REAL,
        approx_gil_per_day REAL,
        PRIMARY KEY (worldid, itemid)
    );
    """)
    # Creates DB Schema and required tabbles, raw data and name data.

    insert_worlds = text(
        """
        MERGE INTO xiv_data.world_names t
            USING (
            VALUES (:worldid, :worldname)
            ) AS s (worldid, worldname)
        ON t.worldid = s.worldid

        WHEN MATCHED THEN
            UPDATE SET
                worldname = s.worldname

        WHEN NOT MATCHED THEN
            INSERT (worldid, worldname)
            VALUES (s.worldid, s.worldname);
        """
    )

    insert_raw_data = text ("""
    MERGE INTO xiv_data.recently_updated t
        USING (
        VALUES (:itemid, :lastuploadtime, :ingested_at, :worldid)
        ) AS s (itemid, lastuploadtime, ingested_at, worldid)
    ON t.worldid = s.worldid AND t.itemid = s.itemid

    WHEN MATCHED THEN
        UPDATE SET
            lastuploadtime = s.lastuploadtime,
            ingested_at = s.ingested_at,
            worldid = s.worldid

    WHEN NOT MATCHED THEN
        INSERT (itemid, lastuploadtime, ingested_at, worldid)
        VALUES (s.itemid, s.lastuploadtime, s.ingested_at, s.worldid);
    """
    )

    insert_name_data = text("""
        MERGE INTO xiv_data.id_names AS t
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

    insert_finance_data = text(
        """
        MERGE INTO xiv_data.finance_data t
            USING (
            VALUES (
                :worldid,
                :itemid,
                :minlisting_price,
                :recentpurchase_price,
                :average_sale_price,
                :daily_sale_velocity,
                :approx_gil_per_day
            )
            ) AS s (
                worldid,
                itemid,
                minlisting_price,
                recentpurchase_price,
                average_sale_price,
                daily_sale_velocity,
                approx_gil_per_day
            )
        ON t.worldid = s.worldid AND t.itemid = s.itemid

        WHEN MATCHED THEN
            UPDATE SET
                minlisting_price = s.minlisting_price,
                recentpurchase_price = s.recentpurchase_price,
                average_sale_price = s.average_sale_price,
                daily_sale_velocity = s.daily_sale_velocity,
                approx_gil_per_day = s.approx_gil_per_day

        WHEN NOT MATCHED THEN
            INSERT (
                worldid,
                itemid,
                minlisting_price,
                recentpurchase_price,
                average_sale_price,
                daily_sale_velocity,
                approx_gil_per_day
            )
            VALUES (
                s.worldid,
                s.itemid,
                s.minlisting_price,
                s.recentpurchase_price,
                s.average_sale_price,
                s.daily_sale_velocity,
                s.approx_gil_per_day
            );
        """
    )

    extract_namedata = text (
        """
    select *
    from xiv_data.id_names
    where itemname IS NULL
    """)

    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            log.info('Connection to database successful.')
    except Exception:
        raise ValueError("DB connection failed (likely bad password.)")
    
    with engine.begin() as conn:
        if dev_mode == True:        
            log.info('Dev mode: Resetting database.')
            conn.execute(reset)
        log.info('Creating database and data tables.')
        conn.execute(create_db)
        conn.execute(insert_worlds, rows_worlds)
        log.info('Inserting table data.')
        try:
            conn.execute(insert_raw_data, rows_data)
            conn.execute(insert_name_data, rows_name)
            conn.execute(insert_finance_data, rows_finance)
        except Exception as e:
            log.error(e)
        log.info('Pruning data with missing item names.')
        response = conn.execute(extract_namedata)
        return response.mappings().all()


if __name__ == "__main__":
    execute_connection()
