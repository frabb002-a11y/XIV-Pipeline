from datetime import datetime, timezone
import pandas as pd
from extract import extract
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    force=True,
)
log = logging.getLogger(__name__)

def transform(items_info=None) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if items_info is None:
        items_info = extract()

    ingested_at_utc = datetime.now(timezone.utc)

    cleaned = {
        'itemid': [],
        'lastuploadtime': [],
        'ingested_at': [],
        'worldid': [],
    }

    names = {
        'itemid': [],
        'itemname' : [],
    }

    worlds = {
        "worldid": [],
        "worldname": [],
    }

    log.info("Checking data quality and transforming data.")
    for item in items_info:
        for k in ("itemID", "lastUploadTime", "worldID", "worldName"):
            if k not in item:
                raise KeyError(f"Missing {k} in item: {item}.")

        try:
            item_id = int(item["itemID"])
        except (TypeError, ValueError):
            raise ValueError(f"itemID must be int-like, got {item['itemID']}.")

        try:
            world_id = int(item["worldID"])
        except (TypeError, ValueError):
            raise ValueError(f"worldID must be int-like, got {item['worldID']}.")

        try:
            last_ms = int(item["lastUploadTime"])
        except (TypeError, ValueError):
            raise ValueError(
                f"lastUploadTime must be int-like ms, got {item['lastUploadTime']}."
            )
        
        world_name = item["worldName"]
        if not isinstance(world_name, str) or not world_name.strip():
            raise ValueError(f"worldName must be a non-empty string, got {world_name}.")
        

        cleaned["itemid"].append(item_id)
        cleaned["lastuploadtime"].append(datetime.fromtimestamp(last_ms / 1000))
        cleaned["ingested_at"].append(ingested_at_utc)
        cleaned["worldid"].append(world_id)

        names["itemid"].append(item_id)
        names["itemname"].append("")

        worlds["worldid"].append(world_id)
        worlds["worldname"].append(world_name)

    return pd.DataFrame(cleaned), pd.DataFrame(names), pd.DataFrame(worlds)

def main():
    df_cleaned, df_names, df_worlds = transform()
    print(df_cleaned)
    print(df_names)
    print(df_worlds)

if __name__ == "__main__":
    main()


# when raising excepts, state the type of except you plan to raise.