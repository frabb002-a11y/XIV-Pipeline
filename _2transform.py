from _1extract import extract_marketable, threaded_batch_extract_aggregated
import logging
import pandas as pd


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger(__name__)


def transform_marketable(item_ids : list[int] | None = None) -> list[int]:
    if item_ids is None:
        item_ids = extract_marketable()
    non_int = 0

    for item in item_ids:
        try:
            int(item)
        except Exception:
            non_int += 1

    log.info("valid items=%d invalid items=%d", len(item_ids), non_int)


def transform_aggregated() -> tuple[list[dict], list[dict]]:
    results, failed_items = threaded_batch_extract_aggregated()
    return results, failed_items


def aggregated_to_dataframe(results) -> list[int]:
    cleaned = {
        "item_id": [],
        "nq_min_listing_price_world": [],
        "nq_avg_sale_price_4d_world": [],
        "nq_daily_sale_velocity_4d_world": [],
        "approx_gil_per_day": [],
    }

    for result in results:
        item_id = result.get("itemId", {})
        nq = result.get("nq", {})
        
        ml = nq.get("minListing",{})
        min_listing_bucket = ml.get("world") or ml.get("dc") or ml.get("region") or {}
        nq_min_listing_price = min_listing_bucket.get("price")

        nq_avg = nq.get("averageSalePrice") or {}
        min_avg_bucket = nq_avg.get("world") or nq_avg.get("dc") or nq_avg.get("region") or {}
        nq_avg_listing_price = min_avg_bucket.get("price")

        dsv = nq.get("dailySaleVelocity") or {}
        vel_bucket = dsv.get("world") or dsv.get("dc") or dsv.get("region") or {}
        nq_daily_sale_velocity = vel_bucket.get("quantity")

        cleaned["item_id"].append(item_id)
        cleaned["nq_min_listing_price_world"].append(nq_min_listing_price)
        cleaned["nq_avg_sale_price_4d_world"].append(nq_avg_listing_price)
        cleaned["nq_daily_sale_velocity_4d_world"].append(nq_daily_sale_velocity)

        try:
            approx_gil_per_day = float(nq_avg_listing_price) * float(
                nq_daily_sale_velocity
            )
        except (TypeError, ValueError):
            approx_gil_per_day = None
        cleaned["approx_gil_per_day"].append(approx_gil_per_day)

    df_cleaned = pd.DataFrame(cleaned)
    df_cleaned = df_cleaned[df_cleaned["approx_gil_per_day"] > 100]
    df_top100 = df_cleaned.nlargest(100, "approx_gil_per_day")
    whitelist_ids = df_top100["item_id"].astype(int).tolist()
    log.info(
        "aggregated data successfully cleaned and filtered, whitelist_ids=%d",
        len(whitelist_ids),
    )
    return whitelist_ids



def second():
    """
        nq_min_listing = w_min.get("price")
        nq_avg = w_avg.get("price")
        nq_vel = w_vel.get("quantity")

        if nq_avg is None or nq_vel is None:
            continue

        item_id_raw = result.get("itemId")
        if item_id_raw is None:
            continue
        item_id = int(item_id_raw)
        nq_min_listing = (
            int(nq_min_listing) if nq_min_listing is not None else None
        )
        nq_avg = float(nq_avg)
        nq_vel = float(nq_vel)
        approx_gil = nq_avg * nq_vel
    """
if __name__ == "__main__":
    results, failed_items = threaded_batch_extract_aggregated()
    aggregated_to_dataframe(results)


