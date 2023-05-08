"""
This module contains the DatabaseConnection object.

Classes:
    DatabaseConnection: DatabaseConnection object used for various actions on the database.
"""

import logging
import os
import time
import asyncio
import aiofiles
import aiohttp
from dotenv import load_dotenv
import mysql.connector
from etc.util import swap, diff

load_dotenv()


class DatabaseConnection:
    """
    DatabaseConnection object used for various actions on the database.
    """

    def __init__(self) -> None:
        self.connection: mysql.connector.connection.MySQLConnection = (
            mysql.connector.connect(
                host=os.getenv("HOST"),
                database=os.getenv("DATABASE"),
                user=os.getenv("USER"),
                password=os.getenv("PASSWORD"),
                ssl_ca=os.getenv("SSL_CA"),
            )
        )

        self.cursor: mysql.connector.connection.MySQLCursor = self.connection.cursor()
        self.logger = logging.getLogger("database")

    def commit_transactions(
        self, t1: float = None, i: int = None, i_threshold: int = None
    ) -> bool:
        """
        Commit transactions every 15 seconds or when i >= i_threshold.

        :param t1: Time when the transaction started.
        :param i: Number of products inserted.
        :param i_threshold: Threshold for i. If i >= i_threshold, commit.

        :returns: True if transaction was committed, False otherwise.
        """

        if t1 is None and (i is None or i >= i_threshold):
            self.cursor.execute("COMMIT;")
            return True

        # Commit every 15 seconds so that we don't lose performance and don't exceed transaction limit.
        t2 = time.perf_counter()
        if t2 - t1 >= 15:
            self.cursor.execute("COMMIT;")
            return True

        if i % 250 == 0:
            self.logger.info(f"Inserted {i} products into database.")

    def insert(self, product) -> None:
        """
        Inserts a product into a database.

        :param product: Product to be inserted into database.

        :return: None
        """

        self.cursor.execute(
            f"""
            INSERT INTO Products (
            ean, other_ean, name, brand, category, image_url, is_age_restricted,
            is_discount, price, store, unit_price, url, weight
            )

            VALUES (
            {product["ean"]},
            {(product["other_ean"][0] if len(product["other_ean"]) > 0 else 0)},
            '{product["name"].replace("'", '"')}',
            '{product["brand"].replace("'", '"')}',
            '{product["category"]}',
            '{product["image_url"]}',
            {product["is_age_restricted"]},
            {product["is_discount"]},
            {product["price"]},
            '{product["store"]}',
            {product["unit_price"]},
            '{product["url"]}',
            '{product["weight"].replace("'", '"')}'
            );
            """
        )


    def update(self, product) -> None:
        """
        Updates a product in a database.

        :param product: Product to be updated in database.

        :returns: None
        """

        self.cursor.execute(
            f"""
            UPDATE Products
            SET ean = {product["ean"]},
            other_ean = {(product["other_ean"][0] if len(product["other_ean"]) > 0 else 0)},
            name = '{product["name"].replace("'", '"')}',
            brand = '{product["brand"].replace("'", '"')}',
            category = '{product["category"]}',
            image_url = '{product["image_url"]}',
            is_age_restricted = {product["is_age_restricted"]},
            is_discount = {product["is_discount"]},
            price = {product["price"]},
            store = '{product["store"]}',
            unit_price = {product["unit_price"]},
            url = '{product["url"]}',
            weight = '{product["weight"].replace("'", '"')}',
            disregard = 0
            WHERE ean = {product["ean"]} AND store = '{product["store"]}';
            """
        )

    def is_connected(self) -> bool:
        """
        Checks if the database is connected.

        :returns: True if connected, False otherwise.
        """

        return self.connection.is_connected()

    def match_products(self) -> None:
        """
        Matches products with the same EAN and calculates the price difference between them.

        :returns: None
        """

        self.cursor.execute(
            """
            SELECT code, COUNT(*) AS count
            FROM (
            SELECT EAN AS code
            FROM Products
            UNION ALL
            SELECT OTHER_EAN AS code
            FROM Products
            WHERE OTHER_EAN IS NOT NULL AND OTHER_EAN != ''
            ) AS codes
            GROUP BY code
            HAVING count = 2;
            """
        )

        matched_eans = [ean[0] for ean in self.cursor.fetchall()]
        self.commit_transactions()

        i = 0
        for ean in matched_eans:
            self.cursor.execute(
                f"SELECT * FROM Products WHERE ean = {ean} OR other_ean = {ean};"
            )
            products = self.cursor.fetchall()

            if len(products) == 2:
                expensive_product, cheaper_product = swap(products[0], products[1], 8)
                percent_diff = diff(expensive_product[8], cheaper_product[8])
                float_diff = round(expensive_product[8] - cheaper_product[8], 2)
                self.cursor.execute(
                    f"""
                UPDATE Products
                SET price_difference_percentage = {percent_diff}, price_difference_float = {float_diff}
                WHERE id = {cheaper_product[0]};
                """
                )

                self.cursor.execute(
                    f"""
                    UPDATE Products
                    SET disregard = 1
                    WHERE id = {expensive_product[0]};
                """
                )

                self.logger.info(
                    f"Updated {ean} with {float_diff} and {percent_diff}% of difference."
                )
                self.commit_transactions(i, i_threshold=25)

            i += 1

        self.commit_transactions()

        self.delete_rows("Matches")
        self.cursor.execute(
            """
        INSERT INTO Matches
        SELECT ID, EAN, name, brand, category, image_url, is_age_restricted, is_discount, 
        price, store, unit_price, url, weight, other_ean, price_difference_float, price_difference_percentage
        FROM Products
        WHERE disregard = 0;
        """
        )

        self.commit_transactions()

    def search(self, query: str, count: int = 10) -> list:
        """
        Uses a fuzzy search algorithm to find products that match the search query and returns a list of products that match.

        :param query: The search query.
        :param count: The number of results to return.

        :returns: A list of products that match the search query.
        """

        self.cursor.execute(
            "SELECT * FROM Products WHERE MATCH(name, brand) AGAINST(%s) LIMIT %s",
            (
                query,
                count,
            ),
        )
        return self.cursor.fetchall()

    def exists(self, product_ean: str, product_store: str) -> bool:
        """
        Checks if the product exists in the database.

        :param product: The product to check for.

        :returns: True if the product exists in the database, False otherwise.
        """

        self.cursor.execute(
            f"SELECT * FROM Products WHERE ean = {product_ean} AND store = '{product_store}';"
        )
        return self.cursor.fetchone() is not None

    def delete_rows(self, table_name: str, product_ean: int = None) -> None:
        """
        Deletes all rows from the specified table.

        :param table_name: The name of the table to delete rows from.
        :param product_ean: The ean of the product to delete.

        :returns: None
        """

        if product_ean is not None:
            self.cursor.execute(f"DELETE FROM {table_name} WHERE ean = {product_ean};")
            self.cursor.execute("COMMIT;")
            return

        self.logger.info(f"Deleting all rows from the {table_name} table...")
        self.cursor.execute(f"TRUNCATE {table_name};")
        self.cursor.execute("COMMIT;")

    async def __download_img(self, url, item_id, session) -> None:
        """
        Downloads an image from a url.

        :param url: The url to download from.\n
        :param item_id: The id of the item.\n
        :param session: The aiohttp session.

        :returns: None
        """

        filename = f"images/{item_id}.jpg"
        async with session.get(url) as response:
            async with aiofiles.open(filename, "wb") as f:
                await f.write(await response.read())

    async def download_images(self) -> None:
        """
        Downloads all images from the database.

        :param db_cursor: The database cursor.

        :returns: None
        """

        self.cursor.execute("SELECT image_url, ID FROM Products")
        rows = self.cursor.fetchall()
        self.logger.info(f"Rows: {len(rows)}")
        async with aiohttp.ClientSession() as session:
            await asyncio.gather(
                *[self.__download_img(row[0], row[1], session) for row in rows]
            )
