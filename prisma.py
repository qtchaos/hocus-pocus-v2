"""
This module contains the Prisma scraping task.

Classes:
    Prisma: The class for the Prisma scraping task and other utilities it may need.
"""

import asyncio
import logging
import re
import time
from typing import Dict

import aiohttp

from etc.category import category_parser
from etc.data import DB_CONNECTOR
from etc.util import request_page, stot


class Prisma:
    """
    The class for the Prisma scraping task and other utilities it may need.

    :param file_name: The name of the file containing product ids.\n
    :param ids: The list of product ids.
    :param debug: Whether to set the logging to debug or not.

    :returns: None
    """

    def __init__(self, file_name: str = None, ids: list = None, debug = False):
        """
        Initializes the Prisma class.

        Parameters:
        file_name (str): Name of the file containing product ids.
        ids (list): List of product ids.
        """

        self.file_name: str = file_name
        self.ids: list = ids
        self.logger = logging.getLogger("prisma")
        if debug:
            self.logger.setLevel(logging.DEBUG)

    def start(self) -> None:
        """
        Starts the Prisma task.

        :returns: None
        """

        asyncio.run(self.__start_scanner())

    async def __start_scanner(self) -> None:
        """
        Scrapes the Prisma website for products and inserts them into the database.

        Parameters:
        :param file_name (str): Name of the file containing product ids.
        ids (list): List of product ids.

        Returns:
        None
        """

        self.logger.info("Starting Prisma task...")
        my_conn = aiohttp.TCPConnector(limit=20)
        tasks = []
        if self.file_name is not None:
            with open(self.file_name, encoding="utf-8") as f:
                self.ids = f.read().split(",")

        async with aiohttp.ClientSession(connector=my_conn) as session:
            session.headers.update({"X-Requested-With": "XMLHttpRequest"})
            for i in self.ids:
                url = f"https://www.prismamarket.ee/entry/{i}?main_view=1"
                tasks.append(
                    asyncio.ensure_future(request_page(session=session, url=url))
                )

            self.logger.info(f"Gathering {len(tasks)} products...")
            gathered_products = await asyncio.gather(*tasks, return_exceptions=True)

            starting_time = time.perf_counter()
            t1 = time.perf_counter()
            i = 0

            self.logger.info("Inserting products into database...")
            for product in gathered_products:
                try:
                    if product["data"] is not None:
                        item = self.__item_parser(product["data"])
                        if not DB_CONNECTOR.exists(item["ean"], item["store"]):
                            DB_CONNECTOR.insert(item)
                            i += 1
                except (TypeError, KeyError):
                    continue

                if DB_CONNECTOR.commit_transactions(t1, i):
                    t1 = time.perf_counter()

            # Avoid division by zero
            i = i + 1 if len(self.ids) > 0 else i

            # Commit the last transactions
            DB_CONNECTOR.commit_transactions()
            t2 = time.perf_counter()
            self.logger.info(
                "Done inserting %s items in %s. %s seconds per item",
                i,
                stot(t2 - t1),
                round((t2 - starting_time) / i, 4),
            )

    def __item_parser(self, product: dict) -> dict:
        try:
            return {
                "ean": product["ean"],
                "store": "Prisma",
                "name": self.__name_parser(product["name"]),
                "other_ean": [],
                "brand": self.__brand_parser(product["subname"]),
                "price": product["price"],
                "is_discount": self.__campaign_parser(product),
                "is_age_restricted": product["contains_alcohol"],
                "weight": f"{product['quantity']} {product['comp_unit']}",
                "unit_price": product["comp_price"],
                "url": f"https://prismamarket.ee/entry/{product['ean']}",
                "category": category_parser(product["aisle"]),
                "image_url": self.__image_parser(product),
                "price_difference_float": 0.0,
                "price_difference_percentage": 0,
            }
        except (KeyError, TypeError):
            return {}

    def __name_parser(self, product_name: str) -> str:
        regex: str = ",? \d{1,4}?\d? ?(g|kg|ml|l|/|tk|€|x|×|,)"
        invalid_chars: dict = {"´": "'", "`": "'", "  ": " ", "amp;": ""}
        
        for char, replacement in invalid_chars.items():
            product_name: str = product_name.replace(char, replacement)

        if re.search(regex, product_name, flags=re.IGNORECASE):
            product_name: str = re.split(regex, product_name, flags=re.IGNORECASE)[0]

        return product_name.title()

    def __brand_parser(self, product_brand: str) -> str:
        return "N/A" if product_brand == "" else product_brand

    def __image_parser(self, product: Dict[str, str]) -> str:
        try:
            image = f'https://s3-eu-west-1.amazonaws.com/balticsimages/images/320x480/{product["image_guid"]}.png'
        except KeyError:
            image = "https://www.prismamarket.ee/images/entry_no_image_170.png"
        return image

    def __campaign_parser(self, product: dict) -> bool:
        try:
            if product["entry_ad"]:
                return True
        except KeyError:
            return False
