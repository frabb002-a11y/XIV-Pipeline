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
        log.info('Dev mode forced to %s. Running on %s.', overriding_to, os.getenv('USERDOMAIN'))
        return overriding_to
    else:
        log.info('Dev mode set to %s. Running on %s.', dev_mode(), os.getenv('USERDOMAIN'))
        return dev_mode()
