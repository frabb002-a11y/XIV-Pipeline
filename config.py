import logging
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    force=True,
)
log = logging.getLogger(__name__)


def dev_mode():

    if os.getenv("USERDOMAIN") == "JARVIS":
        return True
    else:
        return False

def dev_overiding(overriding_to : bool | None = None):
    if isinstance(overriding_to, bool):
        log.info("Devmode forced to %s. running on %s", overriding_to, os.getenv("USERDOMAIN"))
        return overriding_to
    else:
        log.info("Devmode set at %s. running on %s", dev_mode(),os.getenv("USERDOMAIN"))
        return dev_mode()

def DATABASE_URL():
        db_url = "postgresql://neondb_owner:npg_3R2XoTSwUrtD@ep-billowing-boat-abutsytw-pooler.eu-west-2.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
        return db_url


