from _1extract import extract_marketable
import logging


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger(__name__)


def transform_marketable(item_ids : list[int] | None = None) -> list[int]:
    # Validate extracted IDs are int-coercible and log invalid count.
    if item_ids is None:
        item_ids = extract_marketable()
    non_int = 0

    for item in item_ids:
        try:
            int(item)
        except Exception:
            non_int += 1

    log.info("valid items=%d invalid items=%d", len(item_ids), non_int)
    




if __name__ == "__main__":
    transform_marketable()
