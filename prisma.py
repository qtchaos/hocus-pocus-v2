import asyncio
import re
import time

import aiohttp
import mysql

from etc.category import category_parser
from etc.database import item_exists, insert_item, commit_transactions
from etc.util import request_page, seconds_to_time


async def prisma_task(db_cursor: mysql.connector.connection.MySQLCursor, file_name: str):
    print("Starting Prisma task...")
    my_conn = aiohttp.TCPConnector(limit=20)
    tasks = []
    with open(file_name, encoding="utf-8") as f:
        ids = f.read().split(",")

    async with aiohttp.ClientSession(connector=my_conn) as session:
        session.headers.update({'X-Requested-With': 'XMLHttpRequest'})
        for id in ids:
            url = f"https://www.prismamarket.ee/entry/{id}?main_view=1"
            tasks.append(asyncio.ensure_future(request_page(session=session, url=url)))
        print("Gathering products...")
        gathered_products = await asyncio.gather(*tasks, return_exceptions=True)

        starting_time = time.perf_counter()
        t1 = time.perf_counter()
        i = 0
        print("Inserting products into database...")
        for product in gathered_products:
            try:
                item = item_parser(product['data'])
                if not item_exists(item, db_cursor):
                    insert_item(item, db_cursor)
                    i += 1
            except TypeError:
                continue

            if commit_transactions(db_cursor, t1, i):
                t1 = time.perf_counter()

        # Commit the last transactions
        commit_transactions(db_cursor)
        t2 = time.perf_counter()
        print(
            f"Done inserting {i} items in {seconds_to_time(t2 - t1)}. {(t2 - starting_time) / i:.4f} seconds per item")


def item_parser(product):
    try:
        return {
            "ean": product["ean"],
            "store": "Prisma",
            "name": name_parser(product["name"]),
            "other_ean": [],
            "brand": brand_parser(product["subname"]),
            "price": product["price"],
            "is_discount": campaign_parser(product),
            "is_age_restricted": product["contains_alcohol"],
            "weight": f"{product['quantity']} {product['comp_unit']}",
            "unit_price": product["comp_price"],
            "url": f"https://prismamarket.ee/entry/{product['ean']}",
            "category": category_parser(product["aisle"]),
            "image_url": image_parser(product),
            "price_difference_float": 0.0,
            "price_difference_percentage": 0,
        }
    except (KeyError, TypeError):
        return


regex = ",? \d{1,4}?\d? ?(g|kg|ml|l|/|tk|€|x|×|,)"


def name_parser(product_name):
    invalid_chars = {"´": "'", "`": "'", "  ": " ", "amp;": ""}

    for char in invalid_chars:
        product_name = product_name.replace(char, invalid_chars[char])
    if re.search(regex, product_name, flags=re.IGNORECASE):
        product_name = re.split(regex, product_name, flags=re.IGNORECASE)[0]
    return product_name.title()


def brand_parser(product_brand):
    if product_brand == "":
        return "N/A"
    return product_brand


def image_parser(product):
    try:
        image = f'https://s3-eu-west-1.amazonaws.com/balticsimages/images/320x480/{product["image_guid"]}.png'
    except KeyError:
        image = "https://www.prismamarket.ee/images/entry_no_image_170.png"
    return image


def campaign_parser(product):
    try:
        if product["entry_ad"]:
            return True
    except KeyError:
        return False
