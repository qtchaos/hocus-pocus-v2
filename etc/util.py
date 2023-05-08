"""
This file contains utility functions.

Functions:
    download_img: Downloads an image from a url.
    download_images: Downloads all images from the database.
    seconds_to_time: Converts seconds to a time format.
    swap: Swaps two values in a list.
    diff: Returns the percentage difference between two numbers.
    request_page: Requests a page from a url.
"""

import typing

from aiohttp import ClientSession


def seconds_to_time(seconds: float) -> str:
    """
    Converts seconds to a time format.

    :param seconds: The seconds to convert.

    :returns: The time format.
    """

    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    return f"%d:%02d:%02d" % (h, m, s)


def swap(list_a: list, list_b: list, index: int) -> typing.Tuple[int, int]:
    """
    Swaps two values in a tuple based on the index.

    :param a: The first tuple.\n
    :param b: The second tuple.\n
    :param index: The index of the value to compare.

    :returns: The swapped tuples.
    """

    if list_a[index] > list_b[index]:
        return list_a, list_b
    return list_b, list_b


def diff(first_number, second_number) -> float:
    """
    Calculates the percentage difference between two values.

    :param first_numebr: The first value.\n
    :param second_number: The second value.

    :returns: The percentage difference.
    """

    return round(
        ((abs(first_number - second_number)) / ((first_number + second_number) / 2))
        * 100,
        1,
    )


async def request_page(
    session: ClientSession, url: str, page_id: str = None, not_json=False
) -> typing.Coroutine:
    """
    Requests a page and returns the response.

    :param session: The aiohttp session.\n
    :param url: The url to request.\n
    :param page_id: The page id to replace in the url.\n
    :param not_json: Whether to return the response as text or json.

    :returns: The response.
    """

    if page_id is not None:
        url = url.replace("%REPLACE", page_id)
    async with session.get(url) as response:
        if not_json:
            return await response.text()
        return await response.json()
