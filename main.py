"""
This module contains the main entry point for the program.
"""

import logging
import time

from etc.util import seconds_to_time
from prisma import Prisma
from selver import Selver

from etc.data import DB_CONNECTOR

logging.basicConfig(
    format="[%(asctime)s] [%(name)s/%(levelname)s]: %(message)s",
    datefmt="%d-%b-%y %H:%M:%S",
    level=logging.INFO,
)
logger = logging.getLogger("main")


def main():
    """Main entry point for the program."""

    if DB_CONNECTOR.is_connected():
        t1 = time.perf_counter()

        # Delete all rows from the Products table before adding new ones.
        DB_CONNECTOR.delete_rows("Products")

        # Start the tasks.
        Prisma(file_name="data/prisma/eans.txt").start()
        Selver(file_name="data/selver/skus.txt").start()

        # Match the products.
        DB_CONNECTOR.match_products()

        t2 = time.perf_counter()
        logger.info(f"Done in %s.", seconds_to_time(t2 - t1))


if __name__ == "__main__":
    main()
