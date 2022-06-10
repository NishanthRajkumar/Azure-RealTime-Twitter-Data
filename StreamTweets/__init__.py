import asyncio
import os
import tweepy
import pandas as pd
import azure.functions as func
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient
from azure.storage.blob import BlobClient

# Twitter Environment Variables
api_key = os.environ['TWITTER_API_KEY']
api_key_secret = os.environ['TWITTER_API_KEY_SECRET']
access_token = os.environ['TWITTER_ACCESS_TOKEN']
access_token_secret = os.environ['TWITTER_ACCESS_TOKEN_SECRET']

# Azure Environment Variables
EVENTHUB_CONN = os.environ['EVENT_HUB_CONN_STR_SEND']
EVENTHUB_NAME = os.environ['EVENT_HUB_NAME']
TWEET_ID_BLOB_URL = os.environ['TWEET_ID_CHKPOINT_BLOB_URL']
azure_storage_conn = os.environ['AZURE_STORAGE_CONN']
storage_container_name = os.environ['AZURE_CONTAINER_NAME']

async def send_event_data_batch(producer: EventHubProducerClient, message: str) -> None:
    """
        Description:
            Without specifying partition_id or partition_key, 
            the events will be distributed to available partitions via round-robin.

        Parameter:
            producer: Eventhub producer client to send event data
        
        Return:
            None
    """
    event_data_batch = await producer.create_batch()
    event_data_batch.add(EventData(message))
    await producer.send_batch(event_data_batch)

async def send_to_eventhub(message: str) -> None:
    """
        Description:
            Creates Eventhub producer client and sends the message as string to EventHub.

        Parameter:
            message: The message to be sent as event data to event hub.
        
        Return:
            None
    """
    producer = EventHubProducerClient.from_connection_string(
        conn_str=EVENTHUB_CONN,
        eventhub_name=EVENTHUB_NAME
    )
    async with producer:
        await send_event_data_batch(producer, message)

def update_tweet_checkpoint_blob(df: pd.DataFrame):
    """
        Description:
            Updates the tweet_id checkpoint file

        Parameter:
            df: the datframe to store as csv on azure storage
        
        Return:
            None
    """
    output = df.to_csv(encoding = "utf-8", index=False)

    blob = BlobClient.from_connection_string(
        conn_str=azure_storage_conn,
        container_name=storage_container_name,
        blob_name="latest_tweet_id.csv"
    )

    if blob.exists():
        blob.delete_blob(delete_snapshots="include")

    blob.upload_blob(output)

def get_live_tweets(name: str) -> str:
    """
        Description:
            Get live tweets from twitter user timeline

        Parameter:
            None
        
        Return:
            A string message stating whether new tweets were streamed or not.
    """
    # authentication
    auth = tweepy.OAuthHandler(api_key, api_key_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)

    # Check Last streamed tweet id
    df_tweet_id = pd.read_csv(TWEET_ID_BLOB_URL, dtype={'screen_name': str, 'tweet_id': str})
    latest_streamed_tweet_id = ''
    if name in df_tweet_id['screen_name'].values:
        latest_streamed_tweet_id = str(df_tweet_id.loc[df_tweet_id['screen_name']==name]['tweet_id'].values[0])
    if latest_streamed_tweet_id!='' and latest_streamed_tweet_id!='nan':
        public_tweets = api.user_timeline(screen_name=name, count=60, since_id=latest_streamed_tweet_id, tweet_mode="extended")
    else:
        public_tweets = api.user_timeline(screen_name=name, count=60, tweet_mode="extended")

    if len(public_tweets) != 0:
        columns = ['id_str', 'User', 'Tweet']
        data = []
        for tweet in public_tweets:
            data.append([tweet.id_str, tweet.user.screen_name, tweet.full_text])
        pd.set_option('display.max_colwidth',300)
        df = pd.DataFrame(data, columns=columns, dtype=str)

        # Updating Latest streamed tweet id in tweet checkpoint blob
        latest_streamed_tweet_id = df["id_str"][0]
        if name not in df_tweet_id['screen_name'].values:
            new_screen_name = {'screen_name': [name], 'tweet_id': [latest_streamed_tweet_id]}
            new_df_tweet_id = pd.DataFrame(new_screen_name)
            df_tweet_id = pd.concat([df_tweet_id, new_df_tweet_id], ignore_index=True)
        else:
            df_tweet_id.loc[df_tweet_id['screen_name']==name, 'tweet_id'] = latest_streamed_tweet_id
        update_tweet_checkpoint_blob(df_tweet_id)

        output = df.to_csv(index=False)
        asyncio.run(send_to_eventhub(output))

        return "New tweets streamed"
    else:
        return "No new tweets streamed"

def main(req: func.HttpRequest) -> func.HttpResponse:
    name = req.params.get('name')

    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        result = get_live_tweets(name)
        return func.HttpResponse(f"StreamTweets function executed successfully. {result}")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body to get tweets of user with that name",
             status_code=200
        )