import asyncio

import aiofiles
import aiohttp
import mysql
import typing
from aiohttp import ClientSession


async def download_img(url, item_id, session):
    filename = f"images/{item_id}.jpg"
    async with session.get(url) as response:
        async with aiofiles.open(filename, "wb") as f:
            await f.write(await response.read())


async def download_images(db_cursor: mysql.connector.connection.MySQLCursor):
    db_cursor.execute("SELECT image_url, ID FROM Products")
    rows = db_cursor.fetchall()
    print(f"Rows: {len(rows)}")
    async with aiohttp.ClientSession() as session:
        await asyncio.gather(
            *[download_img(row[0], row[1], session) for row in rows]
        )


def seconds_to_time(seconds: float) -> str:
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    return "%d:%02d:%02d" % (h, m, s)


def swap(a, b, index) -> (typing.Tuple[int, int]):
    if a[index] > b[index]:
        return a, b
    else:
        return b, a


def diff(a, b) -> float:
    return round(((abs(a - b)) / ((a + b) / 2)) * 100, 1)


async def request_page(session: ClientSession, url: str, page_id: str = None, not_json = False):
    if page_id is not None:
        url = url.replace('%REPLACE', page_id)
    async with session.get(url) as response:
        if not_json:
            return await response.text()
        return await response.json()
