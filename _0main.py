import logging

from _1extract import extract_marketable
from _2transform import transform_marketable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    force=True,
)
log = logging.getLogger(__name__)

def main():
    transform_marketable(extract_marketable())

if __name__ == "__main__":
    main()

