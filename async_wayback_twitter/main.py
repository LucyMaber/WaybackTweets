import asyncio

from parse import WaybackTweetsParser
from request import WaybackTweets

USERNAME = "jk_rowling"

async def main():
    # Instantiate the API with the given username.
    api = WaybackTweets(USERNAME)
    
    # Asynchronously fetch the archived tweets from the Wayback Machine.
    # (This assumes that the get() method is implemented as an async function.)
    archived_tweets =  api.get_tweets()
    
    if archived_tweets:
        field_options = [
            "archived_timestamp",
            "original_tweet_url",
            "archived_tweet_url",
            "archived_statuscode",
        ]
        
        # Instantiate the TweetsParser with the archived tweets data.
        parser = WaybackTweetsParser(archived_tweets, USERNAME, field_options)
        
        # Asynchronously iterate over each parsed tweet record.
        async for tweet_record in parser.parse():
            print(tweet_record)
    else:
        print("No archived tweets were found.")


if __name__ == "__main__":
    asyncio.run(main())
