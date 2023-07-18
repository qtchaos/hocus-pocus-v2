"""
This module contains the main entry point for the program.
"""

import logging
import os
import sys
import time
from argparse import ArgumentParser

from etc.data import DB_CONNECTOR
from etc.util import stot
from prisma import Prisma
from selver import Selver


argparser = ArgumentParser()
argparser.add_argument("-d", "--debug", help="Set the logging to debug mode", action="store_true", required=False)
argparser.add_argument("--dummy", help="Don't delete any data from the database", action="store_true", required=False)
args = argparser.parse_args()

logging.basicConfig(
    format="[%(asctime)s] [%(name)s/%(levelname)s]: %(message)s",
    datefmt="%d-%b-%y %H:%M:%S",
    level=logging.INFO,
)
logger = logging.getLogger("main")


def main():
    """Main entry point for the program."""
    if not os.path.exists(".env"):
        logger.error("Missing '.env' file. Please copy '.env.template' into '.env' and add valid credentials.")
        sys.exit(1)
    
    if args.debug:
        logger.info("Setting logging to debug.")
        logging.getLogger().setLevel(logging.DEBUG)
        DB_CONNECTOR.debug = args.debug

    if not os.path.exists("resources/cacert.pem"):
        logger.debug("Downloading cacert.pem.")
        os.system("curl -o resources/cacert.pem https://curl.haxx.se/ca/cacert.pem")

    if args.dummy:
        logger.info("Setting dummy mode.")
        DB_CONNECTOR.dummy = args.dummy

    if DB_CONNECTOR.is_connected():
        t1 = time.perf_counter()

        # Delete all rows from the Products table before adding new ones.
        DB_CONNECTOR.delete_rows("Products")

        # Start the tasks.
        Prisma(file_name="resources/prisma/eans.txt", debug=args.debug).start()
        Selver(file_name="resources/selver/skus.txt", debug=args.debug).start()

        # Match the products.
        DB_CONNECTOR.match_products()

        t2 = time.perf_counter()
        logger.info("Done in %s.", stot(t2 - t1))


if __name__ == "__main__":
    main()
