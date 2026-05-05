from datetime import datetime
import pandas as pd
from extract import extract

def transform(items_info = extract()) -> tuple[pd.DataFrame, pd.DataFrame] :
    cleaned = {
        'itemid': [],
        'lastuploadtime': [],
        'worldid': [],
        'worldname' : []
    }

    names = {
        'itemid': [],
        'itemname' : [],
    }

    for item in items_info:
        for k in ("itemID", "lastUploadTime", "worldID", "worldName"):
            if k not in item:
                raise KeyError(f"Missing {k} in item: {item!r}")

        try:
            item_id = int(item["itemID"])
        except (TypeError, ValueError):
            raise ValueError(f"itemID must be int-like, got {item['itemID']!r}")

        try:
            world_id = int(item["worldID"])
        except (TypeError, ValueError):
            raise ValueError(f"worldID must be int-like, got {item['worldID']!r}")

        try:
            last_ms = int(item["lastUploadTime"])
        except (TypeError, ValueError):
            raise ValueError(
                f"lastUploadTime must be int-like ms, got {item['lastUploadTime']!r}"
            )
        
        world_name = item["worldName"]
        if not isinstance(world_name, str) or not world_name.strip():
            raise ValueError(f"worldName must be a non-empty string, got {world_name!r}")
        

        cleaned["itemid"].append(item_id)
        cleaned["lastuploadtime"].append(datetime.fromtimestamp(last_ms / 1000))
        cleaned["worldid"].append(world_id)
        cleaned["worldname"].append(world_name)

        names["itemid"].append(item_id)
        names["itemname"].append("")


    return pd.DataFrame(cleaned), pd.DataFrame(names)

def main():
    df_cleaned, df_names = transform()
    print(df_cleaned)
    print(df_names)

if __name__ == "__main__":
    main()


# when raising excepts, state the type of except you plan to raise.