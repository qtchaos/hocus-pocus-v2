import asyncio
import os
import random
import time
from pprint import pprint

from dotenv import load_dotenv
from mysql.connector import Error
import mysql.connector
from etc.database import delete_rows, match_products, copy_to_matches, fuzzy_search, item_exists
from etc.util import seconds_to_time
from prisma import prisma_task
from selver import selver_task
from xata import XataClient

load_dotenv()


def connect_db() -> XataClient:
    return XataClient(db_url=os.getenv("XATA_DB_URL"), api_key=os.getenv("XATA_API_KEY"))


def main(db_client: XataClient):
    t1 = time.perf_counter()
    xata_test(db_client)

    # delete_rows(db.cursor(), "Products")
    # asyncio.run(selver_task(db_client, "data/selver/skus.txt"))
    # asyncio.run(prisma_task(db.cursor(), "data/prisma/eans.txt"))

    # match_products(db.cursor())
    # copy_to_matches(db.cursor())
    t2 = time.perf_counter()
    print(f"Done in {seconds_to_time(t2 - t1)}.")


def xata_test(db_client: XataClient):
    db_client.records().insertRecordWithID("Products", "1234567890123", {"name": "test", "price": 1.99})
    pprint(item_exists({"ean": "1234567890123", "store": "test"}, db_client))
    pprint(fuzzy_search("tes", db_client))


if __name__ == "__main__":
    try:
        connection = connect_db()

        main(connection)
    except Error as e:
        print("Error while connecting to MySQL", e)
    finally:
        # if connection.is_connected():
        #     connection.close()
        print("MySQL connection is closed")
