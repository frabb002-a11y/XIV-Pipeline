import logging
import time

import requests
from concurrent.futures import ThreadPoolExecutor, as_completed


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    force=True,
)
log = logging.getLogger(__name__)



        
def fetch_data(world: str = 'Louisoix', *, timeout_s: int = 10) -> list[dict[str, int | str]]:
    response = requests.get(
        'https://universalis.app/api/v2/extra/stats/most-recently-updated',
        params = {"world" : world}, timeout = timeout_s
    )
    response.raise_for_status()
    data = response.json()

    if 'items' not in data:
        raise KeyError ("Universalis response missing 'items' key.")
    else:
        return data['items']

            
world_list = ['Cerberus', 'Louisoix', 'Moogle', 'Omega', 'Phantom', 'Ragnarok', 'Sagittarius', 'Spriggan']

def extract():
    success: list[dict[str, int | str]] = []
    success_count = 0

    log.info("Starting extract for %d worlds.", len(world_list))
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_data, world): world for world in world_list}
        for future in as_completed(futures):
            world = futures[future]
            try:
                items = future.result()
                success.extend(items)
                success_count += 1
                log.info("Successfully imported data from world=%s items=%d.", world, len(items))
            except Exception as e:
                log.error("%s", e)

    log.info("Successfully imported from %d out of %d worlds.", success_count, len(world_list))
    return success

if __name__ == "__main__":
    print(extract())
