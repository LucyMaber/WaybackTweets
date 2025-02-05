from urllib.parse import unquote, urlparse
from html import unescape
import re
import traceback
import aiohttp
import asyncio
from typing import Any, AsyncGenerator, Dict, List, Optional, Tuple
from rich import print as rprint
from utils.utils import (
    check_double_status,
    check_pattern_tweet,
    check_url_scheme,
    clean_tweet_url,
    delete_tweet_pathnames,
    is_tweet_url,
    semicolon_parser,
    timestamp_parser,
)

VERBOSE = True


class JsonParser:
    """
    Asynchronously parses tweets when the mimetype is application/json.

    Note: This class is in an experimental phase.

    Args:
        archived_tweet_url (str): The URL of the archived tweet to be parsed.
    """

    def __init__(self, archived_tweet_url: str):
        self.archived_tweet_url = archived_tweet_url

    async def parse(self) -> Optional[str]:
        """
        Asynchronously fetches and parses the archived tweet in JSON format.

        Returns:
            The parsed tweet text, or None if parsing fails.
        """
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.archived_tweet_url) as response:
                    response.raise_for_status()
                    json_data = await response.json()

            # Attempt to extract the tweet text based on available keys.
            if "data" in json_data:
                data_field = json_data["data"]
                if isinstance(data_field, dict):
                    return data_field.get("text", data_field)
                return data_field

            if "retweeted_status" in json_data:
                retweeted = json_data["retweeted_status"]
                if isinstance(retweeted, dict):
                    return retweeted.get("text", retweeted)
                return retweeted

            return json_data.get("text", json_data)
        except aiohttp.ClientError as e:
            rprint(f"[red]Connection error with {self.archived_tweet_url}: {e}")
        except Exception as e:
            rprint(f"[red]An error occurred while parsing JSON: {e}")
        return None


# Example usage of the asynchronous JsonParser:
async def main():
    # Replace the URL with a valid archived tweet URL returning JSON.
    archived_url = "https://example.com/path/to/archived/tweet.json"
    parser = JsonParser(archived_url)
    tweet_text = await parser.parse()

    if tweet_text:
        print("Parsed Tweet Text:", tweet_text)
    else:
        print("Failed to parse tweet text.")


if __name__ == "__main__":
    asyncio.run(main())


class TwitterEmbed:
    """
    This class is responsible for asynchronously parsing tweets using the Twitter Publish service.

    Args:
        tweet_url (str): The URL of the tweet to be parsed.
    """

    def __init__(self, tweet_url: str):
        self.tweet_url = tweet_url

    async def embed(self) -> Optional[Tuple[List[str], List[bool], List[str]]]:
        """
        Asynchronously parses the tweet using Twitter's Publish API.

        Returns:
            A tuple containing:
              - List of tweet texts.
              - List of booleans indicating if each tweet is a retweet.
              - List of user info strings.
            Returns None if an error occurs.
        """
        url = f"https://publish.twitter.com/oembed?url={self.tweet_url}"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    response.raise_for_status()
                    json_response = await response.json()

            # Extract the embed HTML and author name.
            html_content = json_response.get("html", "")
            author_name = json_response.get("author_name", "")

            # Use regex to extract tweet content and user info.
            # The regex matches the tweet text inside a <p> tag and the user information following the em dash.
            regex = re.compile(
                r'<blockquote class="twitter-tweet"(?: [^>]+)?><p[^>]*>(.*?)<\/p>.*?&mdash;\s*(.*?)<\/a>',
                re.DOTALL,
            )
            regex_author = re.compile(r"^(.*?)\s*\(")

            matches_html = regex.findall(html_content)
            if not matches_html:
                return None

            tweet_contents: List[str] = []
            user_infos: List[str] = []
            is_retweet_flags: List[bool] = []

            for match in matches_html:
                # Remove any anchor tags from the tweet text.
                tweet_content_raw = re.sub(r"<a[^>]*>|<\/a>", "", match[0].strip())
                # Replace HTML break tags with newlines and unescape HTML entities.
                tweet_content_clean = unescape(tweet_content_raw.replace("<br>", "\n"))

                # Remove any anchor tags from the user info.
                user_info_raw = re.sub(r"<a[^>]*>|<\/a>", "", match[1].strip())
                user_info_clean = unescape(user_info_raw)

                # Extract the author from the user info (before the first parenthesis).
                match_author = regex_author.search(user_info_clean)
                author_from_tweet = match_author.group(1) if match_author else ""

                tweet_contents.append(tweet_content_clean)
                user_infos.append(user_info_clean)
                is_retweet_flags.append(author_name != author_from_tweet)

            return tweet_contents, is_retweet_flags, user_infos

        except aiohttp.ClientError as e:
            if VERBOSE:
                rprint(f"[yellow]Error parsing the tweet: {e}")
        except Exception as e:
            if VERBOSE:
                rprint(f"[red]An unexpected error occurred: {e}")

        return None


class WaybackTweetsParser:
    """
    Asynchronously parses archived tweets data from the Wayback CDX API.

    Args:
        archived_tweets_response (List[List[str]] or List[Dict[str, str]]): The raw response from the archived tweets API.
            The first row may be a header. In this example we assume that each tweet record is a dictionary.
        username (str): The Twitter username associated with the tweets.
        field_options (List[str]): The fields to be included in the parsed tweet record.
    """

    def __init__(
        self, archived_tweets_response, username: str, field_options: List[str]
    ):
        # archived_tweets_response can be a list of lists or a list of dictionaries.
        self.archived_tweets_response = archived_tweets_response
        self.username = username
        self.field_options = field_options
        self.path_urls = set()

    async def _process_response(self, response: Dict[str, str]) -> Dict[str, Any]:
        """
        Processes a single archived tweet response (as a dict) and returns a dictionary of parsed fields.

        Args:
            response (Dict[str, str]): A single tweet record from the archived response.

        Returns:
            Dict[str, Any]: A dictionary containing parsed tweet fields.
        """
        print(f"Processing response: {response}")
        if "original" not in response:
            return None
        if "statuscode" not in response:
            return None
        elif response["statuscode"] != "200":
            return None
        o = urlparse(response["original"])
        if o.path in self.path_urls:
            return None

        # Use dictionary keys rather than index positions.
        tweet_remove_char = unquote(response["original"]).replace("’", "")
        cleaned_tweet = check_pattern_tweet(tweet_remove_char).strip('"')

        # Build the original Wayback Machine URL.
        wayback_machine_url = f"https://web.archive.org/web/{response['timestamp']}/{response['original']}"
        original_tweet = delete_tweet_pathnames(
            clean_tweet_url(cleaned_tweet, self.username)
        )

        # Check for potential duplicate status segments.
        double_status = check_double_status(wayback_machine_url, original_tweet)
        if double_status:
            original_tweet = delete_tweet_pathnames(
                f"https://twitter.com{original_tweet}"
            )
        elif "://" not in original_tweet:
            original_tweet = delete_tweet_pathnames(f"https://{original_tweet}")

        # Create a parsed version of the Wayback Machine URL.
        parsed_wayback_machine_url = (
            f"https://web.archive.org/web/{response['timestamp']}/{original_tweet}"
        )
        encoded_archived_tweet = check_url_scheme(semicolon_parser(wayback_machine_url))
        encoded_parsed_archived_tweet = check_url_scheme(
            semicolon_parser(parsed_wayback_machine_url)
        )
        encoded_tweet = check_url_scheme(semicolon_parser(response["original"]))
        encoded_parsed_tweet = check_url_scheme(semicolon_parser(original_tweet))
        o = urlparse(encoded_parsed_tweet)
        self.path_urls.add(o.path)

        # Initialize fields for available tweet information.
        available_tweet_text: Optional[str] = None
        available_tweet_is_RT: Optional[bool] = None
        available_tweet_info: Optional[str] = None

        # If the tweet URL looks like a valid Twitter status URL, attempt to fetch its embed data.
        if is_tweet_url(encoded_tweet):
            embed_parser = TwitterEmbed(encoded_tweet)
            content = await embed_parser.embed()
            if content:
                # Assume content is a tuple of lists, where we take the first element of each.
                available_tweet_text = semicolon_parser(content[0][0])
                available_tweet_is_RT = content[1][0]
                available_tweet_info = semicolon_parser(content[2][0])

        # Build the tweet record as a dictionary.
        tweet_record = {
            "available_tweet_text": available_tweet_text,
            "available_tweet_is_RT": available_tweet_is_RT,
            "available_tweet_info": available_tweet_info,
            "archived_urlkey": response["urlkey"],
            "archived_timestamp": response["timestamp"],
            "parsed_archived_timestamp": timestamp_parser(response["timestamp"]),
            "archived_tweet_url": encoded_archived_tweet,
            "parsed_archived_tweet_url": encoded_parsed_archived_tweet,
            "original_tweet_url": encoded_tweet,
            "parsed_tweet_url": encoded_parsed_tweet,
            "archived_mimetype": response["mimetype"],
            "archived_statuscode": response["statuscode"],
            "archived_digest": response["digest"],
            "archived_length": response["length"],
        }
        return tweet_record

    async def parse(self) -> AsyncGenerator[Dict[str, Any], None]:
        responses = self.archived_tweets_response

        # Check if responses is an async generator by looking for the __aiter__ attribute.
        async for response in responses:
            try:
                tweet_record = await self._process_response(response)
                if tweet_record is None:
                    continue
                yield tweet_record
            except Exception as e:
                traceback.print_exc()
                print(f"[Error] Processing tweet record failed: {e}")


class CommonCrawlTweetsParser:
    """
    Asynchronously parses tweet records from Common Crawl data.

    Args:
        common_crawl_response (List[Dict[str, str]] or AsyncGenerator[Dict[str, str], None]):
            The raw response records from Common Crawl. Each record is expected to be a dictionary.
        username (str): The Twitter username associated with the tweets.
        field_options (List[str]): The fields to be included in the parsed tweet record.
    """

    def __init__(self, common_crawl_response, username: str, field_options: List[str]):
        self.common_crawl_response = common_crawl_response
        self.username = username
        self.field_options = field_options

    async def _process_response(self, response: Dict[str, str]) -> Dict[str, Any]:
        """
        Process a single Common Crawl tweet record and return a dictionary of parsed fields.
        """
        # For Common Crawl, we assume the captured URL is stored under 'url'
        tweet_remove_char = unquote(response["url"]).replace("’", "")
        cleaned_tweet = check_pattern_tweet(tweet_remove_char).strip('"')

        # Build a representative Common Crawl URL (this can be adjusted as needed)
        common_crawl_url = (
            f"https://commoncrawl.org/{response['timestamp']}/{response['url']}"
        )
        original_tweet = delete_tweet_pathnames(
            clean_tweet_url(cleaned_tweet, self.username)
        )

        # Check for potential duplicate status segments.
        double_status = check_double_status(common_crawl_url, original_tweet)
        if double_status:
            original_tweet = delete_tweet_pathnames(
                f"https://twitter.com{original_tweet}"
            )
        elif "://" not in original_tweet:
            original_tweet = delete_tweet_pathnames(f"https://{original_tweet}")

        # Create parsed URLs.
        parsed_common_crawl_url = (
            f"https://commoncrawl.org/{response['timestamp']}/{original_tweet}"
        )
        encoded_common_crawl_url = check_url_scheme(semicolon_parser(common_crawl_url))
        encoded_parsed_common_crawl_url = check_url_scheme(
            semicolon_parser(parsed_common_crawl_url)
        )
        encoded_tweet = check_url_scheme(semicolon_parser(response["url"]))
        encoded_parsed_tweet = check_url_scheme(semicolon_parser(original_tweet))

        # Initialize additional tweet fields.
        available_tweet_text: Optional[str] = None
        available_tweet_is_RT: Optional[bool] = None
        available_tweet_info: Optional[str] = None

        # Attempt to fetch embed data if the URL looks like a valid Twitter status URL.
        if is_tweet_url(encoded_tweet):
            embed_parser = TwitterEmbed(encoded_tweet)
            content = await embed_parser.embed()
            if content:
                available_tweet_text = semicolon_parser(content[0][0])
                available_tweet_is_RT = content[1][0]
                available_tweet_info = semicolon_parser(content[2][0])

        tweet_record = {
            "available_tweet_text": available_tweet_text,
            "available_tweet_is_RT": available_tweet_is_RT,
            "available_tweet_info": available_tweet_info,
            "common_crawl_url": response["url"],
            "common_crawl_timestamp": response["timestamp"],
            "parsed_common_crawl_timestamp": timestamp_parser(response["timestamp"]),
            "common_crawl_tweet_url": encoded_common_crawl_url,
            "parsed_common_crawl_tweet_url": encoded_parsed_common_crawl_url,
            "original_tweet_url": encoded_tweet,
            "parsed_tweet_url": encoded_parsed_tweet,
            "common_crawl_mimetype": response.get("mimetype", ""),
            "common_crawl_statuscode": response.get("statuscode", ""),
            "common_crawl_digest": response.get("digest", ""),
            "common_crawl_length": response.get("length", ""),
        }
        return tweet_record

    async def parse(self) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Asynchronously parse the Common Crawl tweet records and yield a parsed record dictionary.
        """
        responses = self.common_crawl_response

        # Check if responses is an async iterable.
        if hasattr(responses, "__aiter__"):
            async for response in responses:
                try:
                    tweet_record = await self._process_response(response)
                    yield tweet_record
                except Exception as e:
                    traceback.print_exc()
                    rprint(f"[Error] Processing tweet record failed: {e}")
        else:
            # Assume responses is a synchronous iterable.
            for response in responses:
                try:
                    tweet_record = await self._process_response(response)
                    yield tweet_record
                except Exception as e:
                    traceback.print_exc()
                    rprint(f"[Error] Processing tweet record failed: {e}")


# Example usage of the asynchronous TweetsParser.
async def main():
    # Example archived tweets response.
    # The first row is the header; subsequent rows represent tweet records.
    archived_tweets_response = [
        [
            "archived_urlkey",
            "archived_timestamp",
            "tweet_url",
            "archived_mimetype",
            "archived_statuscode",
            "archived_digest",
            "archived_length",
        ],
        [
            "example_key",
            "20230101120000",
            "https://twitter.com/example/status/1234567890",
            "text/html",
            "200",
            "digest",
            "1234",
        ],
        # You can add more tweet records here.
    ]
    username = "example"
    field_options = [
        "available_tweet_text",
        "available_tweet_is_RT",
        "available_tweet_info",
        "archived_urlkey",
        "archived_timestamp",
        "parsed_archived_timestamp",
        "archived_tweet_url",
        "parsed_archived_tweet_url",
        "original_tweet_url",
        "parsed_tweet_url",
        "archived_mimetype",
        "archived_statuscode",
        "archived_digest",
        "archived_length",
    ]

    parser = WaybackTweetsParser(archived_tweets_response, username, field_options)
    # async for tweet_record in parser.parse(print_progress=True):
    #     print(tweet_record)


if __name__ == "__main__":
    asyncio.run(main())
