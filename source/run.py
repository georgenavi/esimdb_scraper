import logging
import signal
import os
import sys
import threading

from esimdb_scraper import main as scraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

shutdown_event = threading.Event()


def signal_handler(signum: int, frame) -> None:
    signal_name = signal.Signals(signum).name
    logger.warning("Received signal %s (%d), initiating graceful shutdown...", signal_name, signum)

    if not shutdown_event.is_set():
        shutdown_event.set()
        logger.info("Shutdown requested. Waiting for current operations to complete...")
    else:
        logger.error("Second signal received, exiting immediately.")
        os._exit(1)


def setup_signal_handlers() -> None:
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    logger.debug("Signal handlers registered")


def run_scraper() -> int:
    setup_signal_handlers()
    logger.info("Process started (PID: %d)", os.getpid())

    try:
        logger.info("Starting eSIMDB scraper")
        scraper(stop_event=shutdown_event)

        if shutdown_event.is_set():
            logger.warning("Scraper completed after shutdown was requested")
            return 130
        return 0

    except Exception as e:
        logger.error("Scraper failed with error: %s", e, exc_info=True)
        return 1


if __name__ == "__main__":
    code = run_scraper()
    sys.exit(code)