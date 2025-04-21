from kafka import KafkaConsumer
import json
import threading
import time
import psycopg2
from psycopg2 import extras

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'
TWEETS_TOPIC = 'tweets_topic'
HASHTAGS_TOPIC = 'hashtags_topic'
USER_ACTIVITY_TOPIC = 'user_activity_topic'

# PostgreSQL connection parameters
pg_params = {
    'database': 'twitter_data',
    'user': 'postgres',
    'password': 'postgres',  # Change if your password is different
    'host': 'localhost',
    'port': '5432'
}

class TwitterConsumer:
    def __init__(self):
        self.should_stop = False
        self.threads = []
        
        # For tracking consumer progress
        self.tweets_count = 0
        self.hashtags_count = 0
        self.user_activities_count = 0
    
    def connect_to_db(self):
        """Create connection to PostgreSQL"""
        try:
            conn = psycopg2.connect(**pg_params)
            return conn
        except Exception as e:
            print(f"Error connecting to PostgreSQL: {e}")
            return None
    
    def consume_tweets(self):
        """Consume messages from tweets_topic"""
        consumer = KafkaConsumer(
            TWEETS_TOPIC,
            bootstrap_servers=[KAFKA_BROKER],
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='tweet_consumer_group'
        )
        
        conn = self.connect_to_db()
        if not conn:
            print("Cannot consume tweets: no database connection")
            return
        
        cursor = conn.cursor()
        
        print(f"Starting consumer for {TWEETS_TOPIC}")
        while not self.should_stop:
            # Poll for messages with timeout
            messages = consumer.poll(timeout_ms=1000)
            
            if not messages:
                continue
                
            batch_data = []
            for tp, records in messages.items():
                for record in records:
                    try:
                        tweet = record.value
                        # Extract data
                        tweet_id = tweet['tweet_id']
                        username = tweet['username']
                        text = tweet['text'].replace("'", "''")  # Escape single quotes
                        retweets = tweet['retweets']
                        likes = tweet['likes']
                        timestamp = tweet['timestamp']
                        
                        # Add to batch
                        batch_data.append((tweet_id, username, text, retweets, likes, timestamp))
                        self.tweets_count += 1
                    except Exception as e:
                        print(f"Error processing tweet: {e}")
            
            # Insert batch into database
            if batch_data:
                try:
                    extras.execute_batch(cursor, """
                        INSERT INTO tweets (tweet_id, username, tweet_text, retweets, likes, timestamp)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (tweet_id) DO NOTHING
                    """, batch_data)
                    conn.commit()
                except Exception as e:
                    print(f"Error inserting tweets: {e}")
                    conn.rollback()
        
        # Clean up
        cursor.close()
        conn.close()
        consumer.close()
        print(f"Tweets consumer stopped. Processed {self.tweets_count} tweets.")
    
    def consume_hashtags(self):
        """Consume messages from hashtags_topic"""
        consumer = KafkaConsumer(
            HASHTAGS_TOPIC,
            bootstrap_servers=[KAFKA_BROKER],
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='hashtag_consumer_group'
        )
        
        conn = self.connect_to_db()
        if not conn:
            print("Cannot consume hashtags: no database connection")
            return
        
        cursor = conn.cursor()
        
        print(f"Starting consumer for {HASHTAGS_TOPIC}")
        while not self.should_stop:
            # Poll for messages with timeout
            messages = consumer.poll(timeout_ms=1000)
            
            if not messages:
                continue
                
            batch_data = []
            for tp, records in messages.items():
                for record in records:
                    try:
                        hashtag_data = record.value
                        # Extract data
                        hashtag = hashtag_data['hashtag'].replace("'", "''")  # Escape single quotes
                        timestamp = hashtag_data['timestamp']
                        
                        # Add to batch with count=1 for each occurrence
                        batch_data.append((hashtag, 1, timestamp, timestamp))
                        self.hashtags_count += 1
                    except Exception as e:
                        print(f"Error processing hashtag: {e}")
            
            # Insert batch into database
            if batch_data:
                try:
                    extras.execute_batch(cursor, """
                        INSERT INTO hashtag_counts (hashtag, count, window_end, last_updated)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (hashtag, window_end)
                        DO UPDATE SET 
                            count = hashtag_counts.count + 1,
                            last_updated = EXCLUDED.last_updated
                    """, batch_data)
                    conn.commit()
                except Exception as e:
                    print(f"Error inserting hashtags: {e}")
                    conn.rollback()
        
        # Clean up
        cursor.close()
        conn.close()
        consumer.close()
        print(f"Hashtags consumer stopped. Processed {self.hashtags_count} hashtags.")
    
    def consume_user_activity(self):
        """Consume messages from user_activity_topic"""
        consumer = KafkaConsumer(
            USER_ACTIVITY_TOPIC,
            bootstrap_servers=[KAFKA_BROKER],
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='user_activity_consumer_group'
        )
        
        conn = self.connect_to_db()
        if not conn:
            print("Cannot consume user activity: no database connection")
            return
        
        cursor = conn.cursor()
        
        print(f"Starting consumer for {USER_ACTIVITY_TOPIC}")
        while not self.should_stop:
            # Poll for messages with timeout
            messages = consumer.poll(timeout_ms=1000)
            
            if not messages:
                continue
                
            batch_data = []
            for tp, records in messages.items():
                for record in records:
                    try:
                        activity = record.value
                        # Extract data
                        username = activity['username'].replace("'", "''")  # Escape single quotes
                        tweet_count = activity['tweet_count']
                        retweets = activity['retweets']
                        likes = activity['likes']
                        timestamp = activity['timestamp']
                        
                        # We'll use current time as the window end for simplicity
                        window_end = timestamp
                        
                        # Add to batch
                        batch_data.append((username, tweet_count, likes, retweets, window_end, timestamp))
                        self.user_activities_count += 1
                    except Exception as e:
                        print(f"Error processing user activity: {e}")
            
            # Insert batch into database
            if batch_data:
                try:
                    extras.execute_batch(cursor, """
                        INSERT INTO user_activity (username, tweet_count, total_likes, total_retweets, window_end, last_updated)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (username, window_end)
                        DO UPDATE SET 
                            tweet_count = user_activity.tweet_count + EXCLUDED.tweet_count,
                            total_likes = user_activity.total_likes + EXCLUDED.total_likes,
                            total_retweets = user_activity.total_retweets + EXCLUDED.total_retweets,
                            last_updated = EXCLUDED.last_updated
                    """, batch_data)
                    conn.commit()
                except Exception as e:
                    print(f"Error inserting user activity: {e}")
                    conn.rollback()
        
        # Clean up
        cursor.close()
        conn.close()
        consumer.close()
        print(f"User activity consumer stopped. Processed {self.user_activities_count} activities.")
    
    def start_consumers(self):
        """Start all consumer threads"""
        # Create and start threads
        tweet_thread = threading.Thread(target=self.consume_tweets)
        hashtag_thread = threading.Thread(target=self.consume_hashtags)
        user_thread = threading.Thread(target=self.consume_user_activity)
        
        self.threads = [tweet_thread, hashtag_thread, user_thread]
        
        for thread in self.threads:
            thread.start()
        
        print("All consumers started. Press Ctrl+C to stop.")
        
        try:
            # Print progress periodically
            while any(thread.is_alive() for thread in self.threads):
                print(f"Progress: {self.tweets_count} tweets, {self.hashtags_count} hashtags, {self.user_activities_count} user activities")
                time.sleep(5)
        except KeyboardInterrupt:
            print("Stopping consumers...")
            self.should_stop = True
            
            # Wait for threads to finish
            for thread in self.threads:
                thread.join()
            
            print("All consumers stopped")

if __name__ == "__main__":
    try:
        print("Starting Kafka Consumers for Twitter data")
        consumer = TwitterConsumer()
        consumer.start_consumers()
    except Exception as e:
        print(f"Error in Kafka consumer: {e}")
