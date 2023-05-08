"""
This module contains the Selver scraping task.

Classes:
    Selver: The class for the Selver scraping task and other utilities it may need.
"""
import asyncio
import time

import aiohttp

from etc.category import category_parser
from etc.util import request_page, seconds_to_time
from etc.data import DB_CONNECTOR


class Selver:
    """

    The class for the Selver scraping task and other utilities it may need.

    :param file_name: The name of the file containing product eans.\n
    :param eans: The list of product eans.
    """

    def __init__(self, file_name: str = None, eans: list = None):
        """
        Initializes the Selver class.

        Parameters:
        :param file_name: Name of the file containing product eans.
        :param eans: List of product eans.
        """

        self.file_name: str = file_name
        self.eans: list = eans

    def start(self) -> None:
        """
        Starts the Selver task.

        :returns: None
        """

        asyncio.run(self.__start_scanner())

    async def __start_scanner(self) -> None:
        """
        Scrapes the Selver website for products and inserts them into the database.

        Parameters:
        file_name (str): Name of the file containing product eans.
        eans (list): List of product eans.

        Returns:
        None
        """

        print("Starting Selver task...")
        my_conn = aiohttp.TCPConnector(limit=20)
        if self.file_name is not None:
            with open(self.file_name, encoding="utf-8") as f:
                eans = f.read().split(",")

        async with aiohttp.ClientSession(connector=my_conn) as session:
            session.headers.update({"X-Requested-With": "XMLHttpRequest"})
            tasks = []
            for ean in eans:
                url = (
                    "https://www.selver.ee/api/catalog/vue_storefront_catalog_et/product/_search?from=0&request={"
                    '"query":{"bool":{"filter":{"bool":{"must":[{"terms":{"sku":["%REPLACE"]}},{"terms":{"visibility":['
                    '2,3,4]}},{"terms":{"status":[1]}}]}}}}}&size=8&sort&_source_include=product_main_ean,'
                    "product_age_restricted,name,media_gallery.image,url_key,product_other_ean,*.is_discount,"
                    "unit_price,final_*,category.name,product_volume&_source_exclude=sgn,price_tax"
                )
                url = url.replace("%REPLACE", ean)
                tasks.append(
                    asyncio.ensure_future(request_page(session=session, url=url))
                )

            print("Gathering products...")
            gathered_products = await asyncio.gather(*tasks, return_exceptions=True)

            i = len(eans) > 0
            starting_time = time.perf_counter()
            t1 = starting_time + 1
            print("Inserting products into database...")
            for product in gathered_products:
                try:
                    item = self.__item_parser(product["hits"]["hits"][0]["_source"])
                    if DB_CONNECTOR.exists(item["ean"], item["store"]):
                        DB_CONNECTOR.update(item)
                        i += 1
                    else:
                        DB_CONNECTOR.insert(item)
                        i += 1

                except IndexError:
                    continue

                if DB_CONNECTOR.commit_transactions(t1, i):
                    t1 = time.perf_counter()

            # Avoid division by zero
            i = i + 1 if len(eans) > 0 else i

            # Commit the last transactions
            DB_CONNECTOR.commit_transactions()
            t2 = time.perf_counter()
            print(
                f"Done inserting {i} items in {seconds_to_time(t2 - t1)}. {(t2 - starting_time) / i:.4f} seconds per item"
            )

    def __item_parser(self, product):
        try:
            other_ean = product["product_other_ean"]
        except KeyError:
            other_ean = None

        return {
            "ean": int(product["product_main_ean"]),
            "name": product["name"].title(),
            "other_ean": self.__other_ean_parser(other_ean),
            "brand": "N/A",
            "store": "Selver",
            "price": float(f"{product['final_price_incl_tax']:.2f}"),
            "is_discount": product["prices"][0]["is_discount"],
            "is_age_restricted": product["product_age_restricted"],
            "weight": f"{product['product_volume']}",
            "unit_price": float(f"{product['unit_price']:.2f}"),
            "url": f"https://www.selver.ee/{product['url_key']}",
            "category": category_parser(product["category"][0]["name"]),
            "image_url": self.__image_parser(product),
            "price_difference_float": 0.0,
            "price_difference_percentage": 0,
        }

    def __other_ean_parser(self, other_ean):
        try:
            if other_ean is None:
                return []
            elif "," in other_ean:
                return other_ean.split(",")
            else:
                return [int(other_ean)]
        except KeyError:
            pass

    def __image_parser(self, product):
        try:
            image = f'https://www.selver.ee/img/800/800/resize{product["media_gallery"][0]["image"]}'
        except KeyError:
            image = "https://www.prismamarket.ee/images/entry_no_image_170.png"
        return image
