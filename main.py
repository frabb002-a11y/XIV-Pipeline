from config import dev_overiding
from extract import extract, extract_financial
from transform import transform, transform_financial
from load import prime_connection, execute_connection
from enrich import enrich_data
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    force=True,
)
log = logging.getLogger(__name__)


def main():
    dev_mode = dev_overiding(False)
    item_info = extract()
    df_cleaned, df_names, df_worlds = transform(item_info)
    grouped_itemids_by_world = df_cleaned.groupby("worldname")["itemid"].apply(list).to_dict()
    log.info("Grouped nameids by world.")
    worldname_to_id = df_worlds.set_index("worldname")["worldid"].to_dict()
    log.info("preparing finance data transformation.")
    all_worlds_data = extract_financial(grouped_itemids_by_world)
    df_finance = transform_financial(all_worlds_data, worldname_to_id)
    log.info("Transformed finanical data.")
    rows_data = df_cleaned.to_dict("records")
    rows_name = df_names.to_dict("records")
    rows_worlds = df_worlds.to_dict("records")
    rows_finance = df_finance.to_dict("records")
    engine = prime_connection()
    null_namedata = execute_connection(rows_data, rows_name, rows_worlds, rows_finance, engine, dev_mode)
    enrich_data(null_namedata, engine)

if __name__ == "__main__":
    main()
