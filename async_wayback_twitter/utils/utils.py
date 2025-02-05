"""
Utility functions for handling HTTP requests and manipulating URLs.
"""

import html
import re
from datetime import datetime
from typing import Optional

import aiohttp
import asyncio

class EmptyResponseError:
    pass
class ReadTimeoutError:
    pass
class HTTPError:
    pass
class GetResponseError:
    pass

async def get_response(url: str, params: Optional[dict] = None) -> dict:
    """
    Sends an asynchronous GET request to the specified URL and returns the JSON response.

    Args:
        url (str): The URL to send the GET request to.
        params (dict, optional): The parameters to include in the GET request.

    Returns:
        The JSON-decoded response from the server.

    Raises:
        ReadTimeoutError: If a read timeout occurs.
        ConnectionError: If a connection error occurs.
        HTTPError: If an HTTP error occurs.
        EmptyResponseError: If the response is empty.
        GetResponseError: For any other request exceptions.
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/125.0.0.0 Safari/537.36"
        )
    }
    # Adjust the timeout as needed
    timeout = aiohttp.ClientTimeout(total=10)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        try:
            async with session.get(url, params=params, headers=headers) as response:
                if response.status != 200:
                    raise HTTPError(f"HTTP error {response.status} for url: {url}")
                data = await response.json()
                if not data or data == []:
                    raise EmptyResponseError("No data was saved due to an empty response.")
                return data
        except asyncio.TimeoutError:
            raise ReadTimeoutError
        except aiohttp.ClientConnectionError:
            raise ConnectionError
        except aiohttp.ClientResponseError:
            raise HTTPError
        except aiohttp.ClientError as e:
            raise GetResponseError from e


def clean_tweet_url(tweet_url: str, username: str) -> str:
    """
    Cleans a tweet URL by ensuring it is associated with the correct username.

    Args:
        tweet_url (str): The tweet URL to clean.
        username (str): The username to associate with the tweet URL.

    Returns:
        The cleaned tweet URL.
    """
    tweet_lower = tweet_url.lower()
    pattern = re.compile(r"/status/(\d+)")
    match_lower_case = pattern.search(tweet_lower)
    match_original_case = pattern.search(tweet_url)

    if match_lower_case and username in tweet_lower:
        return f"https://twitter.com/{username}/status/{match_original_case.group(1)}"
    else:
        return tweet_url


def clean_wayback_machine_url(
    wayback_machine_url: str, archived_timestamp: str, username: str
) -> str:
    """
    Cleans a Wayback Machine URL by ensuring it is associated with the correct username and timestamp.

    Args:
        wayback_machine_url (str): The Wayback Machine URL to clean.
        archived_timestamp (str): The timestamp to associate with the Wayback Machine URL.
        username (str): The username to associate with the Wayback Machine URL.

    Returns:
        The cleaned Wayback Machine URL.
    """
    wayback_machine_url = wayback_machine_url.lower()
    pattern = re.compile(r"/status/(\d+)")
    match = pattern.search(wayback_machine_url)

    if match and username in wayback_machine_url:
        return (
            f"https://web.archive.org/web/{archived_timestamp}/"
            f"https://twitter.com/{username}/status/{match.group(1)}"
        )
    else:
        return wayback_machine_url


def check_pattern_tweet(tweet_url: str) -> str:
    """
    Extracts the URL from a tweet URL with patterns such as:

    - Reply: /status//
    - Link:  /status///
    - Twimg: /status/https://pbs

    Args:
        tweet_url (str): The tweet URL to extract the URL from.

    Returns:
        Only the extracted URL from a tweet.
    """
    pattern = r'/status/((?:"(.*?)"|&quot;(.*?)(?=&|$)|&quot%3B(.*?)(?=&|$)))'
    match = re.search(pattern, tweet_url)

    if match:
        if match.group(2):
            parsed_tweet_url = match.group(2)
        elif match.group(3):
            parsed_tweet_url = match.group(3)
        elif match.group(4):
            parsed_tweet_url = match.group(4)
        else:
            parsed_tweet_url = ""

        parsed_tweet_url = html.unescape(parsed_tweet_url)
        return parsed_tweet_url

    return tweet_url


def delete_tweet_pathnames(tweet_url: str) -> str:
    """
    Removes any pathnames from a tweet URL.

    Args:
        tweet_url (str): The tweet URL to remove pathnames from.

    Returns:
        The tweet URL without any pathnames.
    """
    pattern_username = re.compile(r"https://twitter\.com/([^/]+)/status/\d+")
    match_username = pattern_username.match(tweet_url)

    pattern_id = r"https://twitter.com/\w+/status/(\d+)"
    match_id = re.search(pattern_id, tweet_url)

    if match_id and match_username:
        tweet_id = match_id.group(1)
        username = match_username.group(1)
        return f"https://twitter.com/{username}/status/{tweet_id}"
    else:
        return tweet_url


def check_double_status(wayback_machine_url: str, original_tweet_url: str) -> bool:
    """
    Checks if a Wayback Machine URL contains two occurrences of "/status/" and if the original tweet does not contain "twitter.com".

    Args:
        wayback_machine_url (str): The Wayback Machine URL to check.
        original_tweet_url (str): The original tweet URL to check.

    Returns:
        True if the conditions are met, False otherwise.
    """
    if (
        wayback_machine_url.count("/status/") == 2
        and "twitter.com" not in original_tweet_url
    ):
        return True

    return False


def semicolon_parser(string: str) -> str:
    """
    Replaces semicolons in a string with %3B.

    Args:
        string (str): The string to replace semicolons in.

    Returns:
        The string with semicolons replaced by %3B.
    """
    return "".join("%3B" if c == ";" else c for c in string)


def is_tweet_url(twitter_url: str) -> bool:
    """
    Checks if the provided URL is a Twitter status URL.

    This function checks if the provided URL contains "/status/" exactly once, which is a common pattern in Twitter status URLs.

    Args:
        twitter_url (str): The URL to check.

    Returns:
        True if the URL is a Twitter status URL, False otherwise.
    """
    return twitter_url.count("/status/") == 1


def timestamp_parser(timestamp: str) -> Optional[str]:
    """
    Parses a timestamp into a formatted string.

    Args:
        timestamp (str): The timestamp string to parse.

    Returns:
        The parsed timestamp in the format "%Y/%m/%d %H:%M:%S", or None if the
        timestamp could not be parsed.
    """
    formats = [
        "%Y",
        "%Y%m",
        "%Y%m%d",
        "%Y%m%d%H",
        "%Y%m%d%H%M",
        "%Y%m%d%H%M%S",
    ]

    for fmt in formats:
        try:
            parsed_time = datetime.strptime(timestamp, fmt)
            return parsed_time.strftime("%Y/%m/%d %H:%M:%S")
        except ValueError:
            continue

    return None


def check_url_scheme(url: str) -> str:
    """
    Corrects the URL scheme if it contains more than two slashes following the scheme.

    This function uses a regular expression to find 'http:' or 'https:' followed by two or more slashes.
    It then replaces this with the scheme followed by exactly two slashes.

    Args:
        url (str): The URL to be corrected.

    Returns:
        The corrected URL.
    """
    pattern = r"(http:|https:)(/{2,})"

    def replace_function(match):
        scheme = match.group(1)
        return f"{scheme}//"

    parsed_url = re.sub(pattern, replace_function, url)
    return parsed_url
