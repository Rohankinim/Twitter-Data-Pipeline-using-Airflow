import tweepy
import pandas as pd
import time
import boto3
import os
import logging
from airflow.models import Variable
from datetime import timedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_twitter_etl():
    def check_rate_limits(client):
        try:
            status = client.get_rate_limit_status()
            user_tweets_endpoint = status['resources']['users']['/users/:id/tweets']
            logger.info(f"User tweets endpoint: {user_tweets_endpoint['remaining']} requests remaining, resets at {user_tweets_endpoint['reset']}")
            return user_tweets_endpoint['remaining']
        except Exception as e:
            logger.error(f"Error checking rate limits: {str(e)}")
            return 0

    def upload_to_s3(local_file, bucket_name, s3_file):
        s3 = boto3.client('s3')
        try:
            if not os.path.exists(local_file):
                raise FileNotFoundError(f"Local file {local_file} does not exist.")
            s3.upload_file(local_file, bucket_name, s3_file)
            logger.info(f"Uploaded {local_file} to s3://{bucket_name}/{s3_file}")
        except boto3.exceptions.S3UploadFailedError as e:
            logger.error(f"S3 upload failed: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Failed to upload to S3: {str(e)}")
            raise

    def get_user_tweets(client, username, count=5, max_retries=3, initial_wait=60):
        try:
            user = client.get_user(username=username)
            if not user.data:
                logger.warning(f"User {username} not found.")
                return []

            tweets = []
            next_token = None
            remaining_count = count
            retries = 0

            while remaining_count > 0 and retries < max_retries:
                try:
                    batch_size = min(remaining_count, 100)
                    response = client.get_users_tweets(
                        id=user.data.id,
                        max_results=batch_size,
                        tweet_fields=['created_at', 'public_metrics'],
                        exclude=['replies', 'retweets'],
                        pagination_token=next_token
                    )

                    if not response.data:
                        logger.info("No tweets found.")
                        break

                    tweets.extend([{
                        'id': tweet.id,
                        'created_at': tweet.created_at,
                        'text': tweet.text,
                        'likes': tweet.public_metrics['like_count'],
                        'retweets': tweet.public_metrics['retweet_count']
                    } for tweet in response.data])

                    remaining_count -= len(response.data)
                    next_token = response.meta.get('next_token')
                    if not next_token or len(tweets) >= count:
                        break

                except tweepy.TooManyRequests:
                    retries += 1
                    wait_time = initial_wait * (2 ** (retries - 1))
                    logger.warning(f"Rate limit hit. Retry {retries}/{max_retries}. Waiting {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                except tweepy.TweepyException as e:
                    logger.error(f"Twitter API error: {str(e)}")
                    return []

            if retries >= max_retries:
                logger.error(f"Max retries ({max_retries}) reached. No more attempts.")
                return []

            return tweets[:count]

        except Exception as e:
            logger.error(f"Error fetching tweets: {str(e)}")
            return []

    def transform_tweets(tweets):
        df = pd.DataFrame(tweets, columns=['id', 'created_at', 'text', 'likes', 'retweets'])
        if not df.empty:
            df['id'] = df['id'].astype(str)
            df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
            df['likes'] = df['likes'].fillna(0).astype(int)
            df['retweets'] = df['retweets'].fillna(0).astype(int)
            df['text'] = df['text'].str.replace('\n', ' ').str.strip()
            df['tweet_length'] = df['text'].str.len()
            df['created_date'] = df['created_at'].dt.date
            df['created_hour'] = df['created_at'].dt.hour
            df = df.sort_values(by='created_at', ascending=False)
            df = df.reset_index(drop=True)
        return df

    # Load credentials from Airflow Variables
    try:
        BEARER_TOKEN = Variable.get("twitter_bearer_token")
        API_KEY = Variable.get("twitter_api_key")
        API_KEY_SECRET = Variable.get("twitter_api_key_secret")
        ACCESS_TOKEN = Variable.get("twitter_access_token")
        ACCESS_TOKEN_SECRET = Variable.get("twitter_access_token_secret")
    except Exception as e:
        logger.error(f"Failed to load Twitter credentials from Airflow Variables: {str(e)}")
        raise

    client = tweepy.Client(
        bearer_token=BEARER_TOKEN,
        consumer_key=API_KEY,
        consumer_secret=API_KEY_SECRET,
        access_token=ACCESS_TOKEN,
        access_token_secret=ACCESS_TOKEN_SECRET
    )

    logger.info("Checking rate limits before fetching tweets...")
    remaining_requests = check_rate_limits(client)
    if remaining_requests < 1:
        logger.error("No requests remaining in the current rate limit window. Failing task.")
        raise ValueError("Rate limit exhausted.")

    logger.info("Fetching up to 5 tweets for @NASA...")
    tweets = get_user_tweets(client, "NASA", count=5)

    if tweets:
        df = transform_tweets(tweets)
        logger.info(f"Fetched {len(tweets)} tweets for @NASA")
        
        local_path = '/tmp/tweets.csv'
        try:
            df.to_csv(local_path, index=False)
            logger.info(f"Local file {local_path} created successfully with {len(df)} rows and columns: {list(df.columns)}")
        except Exception as e:
            logger.error(f"Failed to save CSV locally: {str(e)}")
            raise

        try:
            upload_to_s3(local_path, 'airfloe-kini', 'twitter_data/tweets.csv')
            logger.info("✅ Tweets transformed, saved locally, and uploaded to S3.")
        except Exception as e:
            logger.error(f"⚠️ Failed to complete S3 upload: {str(e)}")
            raise
    else:
        logger.error("⚠️ No tweets fetched.")
        raise ValueError("No tweets fetched, failing task.")
            # password: GGx49BzVgthEf5KW