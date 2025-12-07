
import logging
import signal
import os

from esimdb_scraper import main as scraper

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Global flag for shutdown
shutdown_requested = False


def signal_handler(signum: int, frame) -> None:
    """
    Handle shutdown signals (SIGTERM, SIGINT).

    Args:
        signum: Signal number
        frame: Current stack frame
    """
    global shutdown_requested

    signal_name = signal.Signals(signum).name
    logger.warning("Received signal %s (%d), initiating graceful shutdown...", signal_name, signum)

    shutdown_requested = True

    # Give some time for cleanup
    logger.info("Waiting for current operations to complete...")


def setup_signal_handlers() -> None:
    """
    Register signal handlers for graceful shutdown.
    """
    # Handle SIGTERM (docker stop, kill)
    signal.signal(signal.SIGTERM, signal_handler)

    # Handle SIGINT (Ctrl+C)
    signal.signal(signal.SIGINT, signal_handler)

    logger.debug("Signal handlers registered")


def run_scraper() -> int:
    """
    Main entry point with graceful shutdown support.

    Returns:
        Exit code (0 for success, 1 for failure, 130 for interruption)
    """
    # Setup signal handlers
    setup_signal_handlers()

    logger.info("Process started (PID: %d)", os.getpid())

    try:
        logger.info("Starting eSIMDB scraper")
        # Run the main scraper function
        scraper()

        if shutdown_requested:
            logger.warning("Scraper completed but shutdown was requested")
            return 130  # Standard exit code for SIGINT
        return 0

    except KeyboardInterrupt:
        logger.warning("Scraper interrupted by user (Ctrl+C)")
        return 130

    except Exception as e:
        logger.error("Scraper failed with error: %s", e, exc_info=True)
        return 1


if __name__ == "__main__":
    run_scraper()