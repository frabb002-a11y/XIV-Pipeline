from concurrent.futures import ThreadPoolExecutor
import logging
import time
import requests


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger(__name__)


def extract_marketable() -> list[int]:
    # Fetch the full marketable item-id universe from Universalis.
    log.info("Requesting marketable universe from Universalis.")
    url = "https://universalis.app/api/v2/marketable"
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    data = response.json()
    log.info("Received %d marketable item IDs.", len(data))
    return data


def extract_aggregated_batch(batch: int, world: str | None = None):

    # Pull aggregated pricing/stats for a single item on Louisoix (sample).
    if world is None:
        world = 'Louisoix'
    itemid = ",".join(map(str, batch)) # temp list, list contains batch of numbers
    log.info(f"Requesting aggregated data from Universalis from batch")
    url = f"https://universalis.app/api/v2/aggregated/{world}/{itemid}"
    response = requests.get(url, timeout=60)
    try:
        response.raise_for_status()
        return response.json()
    except requests.HTTPError:
        if response.status_code == 429:
            log.warning("Rate limited (429). Skipping batch.")


def threaded_batch_extract_aggregated():
    max_items = extract_marketable()
    log.info(f"max items to process: {len(max_items)}")
    batches: list[list[int]] = []
    for i in range(0, 300, 100):
        batches.append(max_items[i : i + 100])

    with ThreadPoolExecutor(10) as executor:
        log.info("initialising threading")
        start = time.time()
        execution = list(executor.map(extract_aggregated_batch, batches))
        end = time.time()
        log.info("execution time: %s seconds", round((end - start), 2))

    results: list[dict] = []
    failed_items: list[dict] = []

    for ex in execution:
        results.extend(ex.get("results", []))
        failed_items.extend(ex.get("failedItems", []))
    log.info("results=%d failedItems=%d", len(results), len(failed_items))
    return results, failed_items


def extract_history_batch(batch: list[int], world: str | None = None):

    if world is None:
        world = "Louisoix"
    item_str = ",".join(map(str, batch))
    log.info(
        "Requesting history data from Universalis from batch (%d item ids)",
        len(batch),
    )
    url = f"https://universalis.app/api/v2/history/{world}/{item_str}"
    response = requests.get(url, timeout=60)
    try:
        response.raise_for_status()
        return response.json()
    except requests.HTTPError:
        if response.status_code == 429:
            log.warning("Rate limited (429). Skipping batch.")


def threaded_whitelist_extract_history(whitelist: list[int], world: str | None = None):
    # One HTTP request per batch of item IDs (same style as aggregated batches).
    if world is None:
        world = "Louisoix"
    ids = [int(x) for x in whitelist]
    log.info("max items to process: %d", len(ids))
    if not ids:
        return [], []

    batches: list[list[int]] = []
    for i in range(0, len(ids), 100):
        batches.append(ids[i : i + 100])

    payloads: list[dict] = []
    failed_items: list[dict] = []

    start = time.time()
    for batch in batches:
        data = extract_history_batch(batch, world)
    
    end = time.time()
    log.info("execution time: %s seconds", round((end - start), 2))
    
    return data


if __name__ == "__main__":
    threaded_batch_extract_aggregated()
