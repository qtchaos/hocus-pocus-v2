"""
This module contains the main entry point for the program.
"""

import logging
import time
from argparse import ArgumentParser

from etc.data import DB_CONNECTOR
from etc.util import stot
from prisma import Prisma
from selver import Selver


argparser = ArgumentParser()
argparser.add_argument("-d", "--debug", help="Set the logging to debug", action="store_true", required=False)
argparser.add_argument("--dummy", help="Don't delete any data", action="store_true", required=False)
args = argparser.parse_args()

logging.basicConfig(
    format="[%(asctime)s] [%(name)s/%(levelname)s]: %(message)s",
    datefmt="%d-%b-%y %H:%M:%S",
    level=logging.INFO,
)
logger = logging.getLogger("main")


def main():
    """Main entry point for the program."""

    if args.debug:
        logger.info("Setting logging to debug.")
        logging.getLogger().setLevel(logging.DEBUG)
        DB_CONNECTOR.debug = args.debug

    if args.dummy:
        logger.info("Setting dummy mode.")
        DB_CONNECTOR.dummy = args.dummy

    if DB_CONNECTOR.is_connected():
        t1 = time.perf_counter()

        # Delete all rows from the Products table before adding new ones.
        DB_CONNECTOR.delete_rows("Products")

        # Start the tasks.
        Prisma(file_name="data/prisma/eans.txt", debug=args.debug).start()
        Selver(file_name="data/selver/skus.txt", debug=args.debug).start()

        # Match the products.
        DB_CONNECTOR.match_products()

        t2 = time.perf_counter()
        logger.info("Done in %s.", stot(t2 - t1))


if __name__ == "__main__":
    main()
