import asyncio
import os
import time
from pprint import pprint

from dotenv import load_dotenv
from mysql.connector import Error
import mysql.connector
from etc.database import fuzzy_search, delete_rows, match_products, copy_to_matches
from etc.util import seconds_to_time
from prisma import prisma_task
from selver import selver_task

load_dotenv()


def connect_db() -> mysql.connector.pooling.MySQLConnection:
    return mysql.connector.connect(
        host=os.getenv("HOST"),
        database=os.getenv("DATABASE"),
        user=os.getenv("USER"),
        password=os.getenv("PASSWORD"),
        ssl_ca=os.getenv("SSL_CA")
    )


def main(db: mysql.connector.pooling.MySQLConnection):
    if db.is_connected():
        # asyncio.run(download_images(db.cursor()))
        t1 = time.perf_counter()

        delete_rows(db.cursor(), "Products")
        asyncio.run(selver_task(db.cursor(), "data/selver/skus.txt"))
        asyncio.run(prisma_task(db.cursor(), "data/prisma/eans.txt"))
        match_products(db.cursor())
        copy_to_matches(db.cursor())

        # pprint(fuzzy_search("coca cola", db.cursor()))
        t2 = time.perf_counter()
        print(f"Done in {seconds_to_time(t2 - t1)}.")


if __name__ == "__main__":
    try:
        connection = connect_db()
        main(connection)
    except Error as e:
        print("Error while connecting to MySQL", e)
    finally:
        if connection.is_connected():
            connection.close()
            print("MySQL connection is closed")
