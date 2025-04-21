from kafka import KafkaProducer
import csv
import json
import time
from datetime import datetime
import re

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'
TWEETS_TOPIC = 'tweets_topic'
HASHTAGS_TOPIC = 'hashtags_topic'
USER_ACTIVITY_TOPIC = 'user_activity_topic'
DATA_FILE = "../data/twitter_dataset_small.csv"
SAMPLE_SIZE = 200  # Limit for testing; set to None for full dataset

def extract_hashtags(text):
    """Extract hashtags from tweet text"""
    if not text:
        return []
    
    # Extract hashtags using regex
    return re.findall(r'#(\w+)', text)

def calculate_user_activity(username, tweet_text, retweets, likes):
    """Calculate user activity metrics for a single tweet"""
    return {
        'username': username,
        'tweet_count': 1,
        'hashtag_count': len(extract_hashtags(tweet_text)),
        'retweets': retweets,
        'likes': likes,
        'timestamp': datetime.now().isoformat()
    }

def produce_to_kafka():
    """Read data from CSV and send to Kafka topics"""
    # Initialize Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    print(f"Initialized Kafka producer connecting to {KAFKA_BROKER}")
    print(f"Will publish to topics: {TWEETS_TOPIC}, {HASHTAGS_TOPIC}, {USER_ACTIVITY_TOPIC}")
    
    # Open and read the CSV file
    with open(DATA_FILE, 'r', encoding='utf-8') as file:
        csv_reader = csv.DictReader(file)
        
        # Process each row and send to Kafka
        count = 0
        for row in csv_reader:
            # Limit dataset size if specified
            if SAMPLE_SIZE is not None and count >= SAMPLE_SIZE:
                break
                
            # Extract data
            tweet_id = int(row['Tweet_ID'])
            username = row['Username']
            tweet_text = row['Text']
            retweets = int(row['Retweets'])
            likes = int(row['Likes'])
            timestamp = row['Timestamp']
            
            # 1. Send to tweets_topic (raw data)
            tweet_data = {
                'tweet_id': tweet_id,
                'username': username,
                'text': tweet_text,
                'retweets': retweets,
                'likes': likes,
                'timestamp': timestamp
            }
            producer.send(TWEETS_TOPIC, tweet_data)
            
            # 2. Extract hashtags and send to hashtags_topic
            hashtags = extract_hashtags(tweet_text)
            for hashtag in hashtags:
                hashtag_data = {
                    'tweet_id': tweet_id,
                    'hashtag': hashtag,
                    'username': username,
                    'timestamp': timestamp
                }
                producer.send(HASHTAGS_TOPIC, hashtag_data)
            
            # 3. Calculate user activity and send to user_activity_topic
            user_activity = calculate_user_activity(username, tweet_text, retweets, likes)
            producer.send(USER_ACTIVITY_TOPIC, user_activity)
            
            count += 1
            if count % 100 == 0:
                print(f"Sent {count} records to Kafka")
                producer.flush()
            
            # Add a small delay to prevent overwhelming the system
            time.sleep(0.01)
    
    # Make sure all messages are sent
    producer.flush()
    print(f"Total of {count} records sent to Kafka across 3 topics")

if __name__ == "__main__":
    try:
        print(f"Starting Kafka Producer for Twitter data")
        produce_to_kafka()
        print("Data transfer completed")
    except Exception as e:
        print(f"Error in Kafka producer: {e}")
