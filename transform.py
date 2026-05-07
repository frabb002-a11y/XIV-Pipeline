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
        'worldname' : [],
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
        cleaned["worldname"].append(world_name)

        names["itemid"].append(item_id)
        names["itemname"].append("")

        worlds["worldid"].append(world_id)
        worlds["worldname"].append(world_name)

    df_cleaned = pd.DataFrame(cleaned)
    df_names = pd.DataFrame(names)
    df_worlds = pd.DataFrame(worlds)

    return df_cleaned, df_names, df_worlds


def transform_financial(all_worlds_data, worldname_to_id: dict[str, int]) -> pd.DataFrame:
    finance_data = {
        "itemid": [],
        "worldid": [],
        "minlisting_price": [],
        "recentpurchase_price": [],
        "average_sale_price": [],
        "daily_sale_velocity": [],
        "approx_gil_per_day": [],
    }

    for worldname, world in all_worlds_data:
        for item in world['results']:
            itemid = item.get('itemId', {})
            # log.info(f"itemid: {itemid}")

            minListing = item['nq']['minListing'].get("world") or item['nq']['minListing'].get("dc") or item['nq']['minListing'].get('region') or {} 
            minlisting_price = minListing.get('price')
            # log.info(f"min listing price: {minlisting_price}")

            recentPurchase = item["nq"]["recentPurchase"].get("world") or item["nq"]["recentPurchase"].get("dc") or item["nq"]["recentPurchase"].get('region') or {}
            recentpurchase_price = recentPurchase.get("price")
            # log.info(f"Recent purchase price: {recentpurchase_price}")

            averageSalePrice = item["nq"]["averageSalePrice"].get("world") or item["nq"]["averageSalePrice"].get("dc") or item["nq"]["averageSalePrice"].get('region') or {}
            average_sale_price = averageSalePrice.get("price")
            # log.info(f"average sale price: {average_sale_price}")

            dailySaleVelocity = item["nq"]["dailySaleVelocity"].get("world") or item["nq"]["dailySaleVelocity"].get("dc") or item["nq"]["dailySaleVelocity"].get('region') or {}
            daily_sale_velocity = dailySaleVelocity.get("quantity")
            # log.info(f"daily sale velocity: {daily_sale_velocity}")

            # log.info(item['hq'])-> ignored for now
            finance_data["itemid"].append(itemid)
            finance_data["worldid"].append(worldname_to_id.get(worldname))
            finance_data["minlisting_price"].append(minlisting_price)
            finance_data["recentpurchase_price"].append(recentpurchase_price)
            finance_data["average_sale_price"].append(average_sale_price)
            finance_data["daily_sale_velocity"].append(daily_sale_velocity)
            try:
                finance_data["approx_gil_per_day"].append(float(average_sale_price) * float(daily_sale_velocity))
            except (TypeError, ValueError):
                finance_data["approx_gil_per_day"].append(None)

    return pd.DataFrame(finance_data)

def main():
    df_cleaned, df_names, df_worlds = transform()
    print(df_cleaned)
    print(df_names)
    print(df_worlds)

if __name__ == "__main__":
    main()
