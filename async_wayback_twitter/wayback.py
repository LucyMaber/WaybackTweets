import json
import aiohttp
import asyncio
from typing import Any, AsyncGenerator, Dict, Optional


class WaybackTweets:
    """
    Asynchronously requests data from the Wayback Machine CDX API for a given Twitter username,
    and yields each tweet entry as a dictionary.

    Args:
        username (str): The Twitter username to search for.
        collapse (Optional[str]): Field name to collapse duplicate entries.
        timestamp_from (Optional[str]): The starting timestamp for tweets (YYYYMMDD[HH[MM[SS]]]).
        timestamp_to (Optional[str]): The ending timestamp for tweets (YYYYMMDD[HH[MM[SS]]]).
        limit (Optional[int]): Maximum number of results to return.
        offset (Optional[int]): Number of lines to skip in the results.
        matchtype (Optional[str]): Match type (e.g., prefix, host, or domain).
    """

    def __init__(
        self,
        username: str,
        collapse: Optional[str] = None,
        timestamp_from: Optional[str] = None,
        timestamp_to: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        matchtype: Optional[str] = None,
    ):
        self.username = username
        self.collapse = collapse
        self.timestamp_from = timestamp_from
        self.timestamp_to = timestamp_to
        self.limit = limit
        self.offset = offset
        self.matchtype = matchtype

    async def get_tweets(self) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Asynchronously requests data from the CDX API and yields each tweet record.

        Yields:
            Dict[str, Any]: A dictionary representing a single tweet record,
                            where keys are the header names returned by the API.
        """
        url = "https://web.archive.org/cdx/search/cdx"

        # Use a wildcard pathname unless a matchtype is specified
        wildcard_pathname = "/*" if not self.matchtype else ""
        paramss = [
            {
                "url": f"https://twitter.com/{self.username}/status{wildcard_pathname}",
                "output": "json",
            },
            {
                "url": f"https://x.com/{self.username}/status{wildcard_pathname}",
                "output": "json",
            },
        ]
        for params in paramss:
            if self.collapse:
                params["collapse"] = self.collapse
            if self.timestamp_from:
                params["from"] = self.timestamp_from
            if self.timestamp_to:
                params["to"] = self.timestamp_to
            if self.limit:
                params["limit"] = self.limit
            if self.offset:
                params["offset"] = self.offset
            if self.matchtype:
                params["matchType"] = self.matchtype

            # Optionally, you can configure a TCPConnector for concurrency or timeouts.
            async with aiohttp.ClientSession() as session:
                try:
                    async with session.get(url, params=params) as response:
                        response.raise_for_status()
                        data = await response.json()
                except aiohttp.ClientError as e:
                    # Log error or raise a custom exception if needed
                    print(f"[Error] Unable to fetch data: {e}")
                    return

            # The first row is typically a header row; subsequent rows are tweet records.
            if not data or len(data) < 2:
                # No tweet data available
                return

            headers = data[0]
            for row in data[1:]:
                # Build a dictionary for each tweet record (zipping headers and row values)
                tweet_record = dict(zip(headers, row))
                yield tweet_record
                

# Example usage of the asynchronous generator:
async def main():
    # Instantiate the class with desired parameters.
    wayback = WaybackTweets(username="jk_rowling")

    # Iterate asynchronously over the yielded tweets.
    async for tweet in wayback.get_tweets():
        print(tweet)


if __name__ == "__main__":
    # Run the async main function.
    asyncio.run(main())
