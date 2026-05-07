import logging

from _1extract import extract_marketable, threaded_batch_extract_aggregated, threaded_whitelist_extract_history
from _2transform import aggregated_to_dataframe, transform_marketable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    force=True,
)
log = logging.getLogger(__name__)

def main():
    results = threaded_batch_extract_aggregated()[0]
    white_list = aggregated_to_dataframe(results)
    threaded_whitelist_extract_history(white_list)
if __name__ == "__main__":
    main()

