import tweepy
import pandas as pd
import time
import boto3
import os

def run_twitter_etl():
    def upload_to_s3(local_file, bucket_name, s3_file):
        # Rely on EC2 instance's IAM role for authentication
        s3 = boto3.client('s3')
        try:
            if not os.path.exists(local_file):
                raise FileNotFoundError(f"Local file {local_file} does not exist.")
            s3.upload_file(local_file, bucket_name, s3_file)
            print(f"Uploaded {local_file} to s3://{bucket_name}/{s3_file}")
        except Exception as e:
            print(f"Failed to upload to S3: {str(e)}")
            raise

    def get_user_tweets(client, username, count=5, max_retries=3, wait_time=900):
        try:
            user = client.get_user(username=username)
            if not user.data:
                print(f"User {username} not found.")
                return []

            tweets = []
            next_token = None
            remaining_count = count

            while remaining_count > 0:
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
                        print("No tweets found.")
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
                    print(f"Rate limit hit. Waiting {wait_time // 60} minutes...")
                    time.sleep(wait_time)
                    continue

            return tweets[:count]

        except Exception as e:
            print(f"Error fetching tweets: {str(e)}")
            return []

    def transform_tweets(tweets):
        # Convert list of tweets to DataFrame with explicit columns
        df = pd.DataFrame(tweets, columns=['id', 'created_at', 'text', 'likes', 'retweets'])
        
        # Data cleaning and transformation
        if not df.empty:
            # Ensure data types
            df['id'] = df['id'].astype(str)  # Convert ID to string to avoid precision issues
            df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')  # Standardize datetime
            df['likes'] = df['likes'].fillna(0).astype(int)  # Fill missing likes with 0
            df['retweets'] = df['retweets'].fillna(0).astype(int)  # Fill missing retweets with 0
            df['text'] = df['text'].str.replace('\n', ' ').str.strip()  # Clean text: remove newlines, strip whitespace
            
            # Add derived columns
            df['tweet_length'] = df['text'].str.len()  # Length of tweet text
            df['created_date'] = df['created_at'].dt.date  # Extract date
            df['created_hour'] = df['created_at'].dt.hour  # Extract hour for time-based analysis
            
            # Sort by created_at (newest first)
            df = df.sort_values(by='created_at', ascending=False)
            
            # Reset index to ensure clean row numbering
            df = df.reset_index(drop=True)
        
        return df

    # Hardcoded Twitter credentials for temporary testing
    BEARER_TOKEN = "AAAAAAAAAAAAAAAAAAAAAIA32QEAAAAA1cjreFhS%2FOJHA4BxgFkFpCfVf9s%3DgNMYrd73w5vOQts66bQxCWgrDkNhCVN43H4ZofWTMj3QjyIADA"
    API_KEY = "tLqmk7LuFP00uqfxw1fyaVLtn"
    API_KEY_SECRET = "3GUHSrzTtZhRE6yO5Nhmo12a70Bcs04LcEwnlFTmTt2LsZmTsW"
    ACCESS_TOKEN = "739883307790307328-ddS9Qe1mWlLTP2VyHNDzXs6soROWMSo"
    ACCESS_TOKEN_SECRET = "JeWghm5uJyXfTGquRRSxlsNMx2MGZWv9dqW8JXe94PIUT"

    client = tweepy.Client(
        bearer_token=BEARER_TOKEN,
        consumer_key=API_KEY,
        consumer_secret=API_KEY_SECRET,
        access_token=ACCESS_TOKEN,
        access_token_secret=ACCESS_TOKEN_SECRET
    )

    print("Fetching up to 5 tweets for @NASA...")
    tweets = get_user_tweets(client, "NASA", count=5)

    if tweets:
        # Transform tweets into a structured dataset
        df = transform_tweets(tweets)
        
        local_path = '/tmp/tweets.csv'
        try:
            df.to_csv(local_path, index=False)
            print(f"Local file {local_path} created successfully with {len(df)} rows and columns: {list(df.columns)}")
        except Exception as e:
            print(f"Failed to save CSV locally: {str(e)}")
            raise  # Raise exception to fail the DAG task

        # Upload to your S3 bucket
        try:
            upload_to_s3(local_path, 'airfloe-kini', 'twitter_data/tweets.csv')
            print("✅ Tweets transformed, saved locally, and uploaded to S3.")
        except Exception as e:
            print(f"⚠️ Failed to complete S3 upload: {str(e)}")
            raise  # Raise exception to fail the DAG task
    else:
        print("⚠️ No tweets fetched.")
        raise ValueError("No tweets fetched, failing task.")  # Raise exception to fail the DAG task