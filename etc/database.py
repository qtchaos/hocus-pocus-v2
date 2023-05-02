import time

import mysql

from etc.util import swap, diff


# Blocking code
def insert_item(product, db_cursor: mysql.connector.connection.MySQLCursor):
    db_cursor.execute(
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


def update_item(product, db_cursor: mysql.connector.connection.MySQLCursor):
    db_cursor.execute(
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

def match_products(db_cursor: mysql.connector.connection.MySQLCursor):
    db_cursor.execute(
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

    matched_eans = [ean[0] for ean in db_cursor.fetchall()]
    commit_transactions(db_cursor)

    i = 0
    for ean in matched_eans:
        db_cursor.execute(f"SELECT * FROM Products WHERE ean = {ean} OR other_ean = {ean};")
        products = db_cursor.fetchall()

        if len(products) == 2:
            expensive_product, cheaper_product = swap(products[0], products[1], 8)
            percent_diff = diff(expensive_product[8], cheaper_product[8])
            float_diff = round(expensive_product[8] - cheaper_product[8], 2)
            db_cursor.execute(f"""
            UPDATE Products
            SET price_difference_percentage = {percent_diff}, price_difference_float = {float_diff}
            WHERE id = {cheaper_product[0]};
            """)

            db_cursor.execute(f"""
                UPDATE Products
                SET disregard = 1
                WHERE id = {expensive_product[0]};
            """)

            print(f"Updated {ean} with {float_diff} and {percent_diff}% of difference.")
            commit_transactions(db_cursor, i, i_threshold=25)

        i += 1

    commit_transactions(db_cursor)


def copy_to_matches(db_cursor: mysql.connector.connection.MySQLCursor):
    delete_rows(db_cursor, "Matches")
    db_cursor.execute("""
    INSERT INTO Matches
    SELECT ID, EAN, name, brand, category, image_url, is_age_restricted, is_discount, 
    price, store, unit_price, url, weight, other_ean, price_difference_float, price_difference_percentage
    FROM Products
    WHERE disregard = 0;
    """)
    commit_transactions(db_cursor)


def commit_transactions(db_cursor: mysql.connector.connection.MySQLCursor, t1: float = None, i: int = None,
                        i_threshold: int = None) -> bool:
    if t1 is None and i is None:
        db_cursor.execute("COMMIT;")
        return True

    if t1 is None and i >= i_threshold:
        db_cursor.execute("COMMIT;")
        return True

    # Commit every 15 seconds so that we don't lose performance and don't exceed transaction limit.
    t2 = time.perf_counter()
    if t2 - t1 >= 15:
        db_cursor.execute("COMMIT;")
        return True
    if i % 250 == 0:
        print(f"Inserted {i} products into database.")


def fuzzy_search(query: str, db_cursor: mysql.connector.connection.MySQLCursor, count: int = 10):
    db_cursor.execute("SELECT * FROM Products WHERE MATCH(name, brand) AGAINST(%s) LIMIT %s", (query, count,))
    return db_cursor.fetchall()


def item_exists(product, db_cursor: mysql.connector.connection.MySQLCursor):
    db_cursor.execute(f"SELECT * FROM Products WHERE ean = {product['ean']} AND store = '{product['store']}';")
    return db_cursor.fetchone() is not None


def delete_rows(db_cursor: mysql.connector.connection.MySQLCursor, table_name: str, ean: int = None):
    """WARNING - Deletes all rows from the Products table"""
    if ean is not None:
        db_cursor.execute(f"DELETE FROM {table_name} WHERE ean = {ean};")
        db_cursor.execute("COMMIT;")
        return

    print(f"Deleting all rows from the {table_name} table...")
    db_cursor.execute(f"TRUNCATE {table_name};")
    db_cursor.execute("COMMIT;")
