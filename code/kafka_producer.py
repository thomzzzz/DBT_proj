from kafka import KafkaProducer
import csv
import json
import time
from datetime import datetime
from kafka.admin import KafkaAdminClient, NewTopic

def create_kafka_topics():
    """Helper function to create all required Kafka topics"""
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=['localhost:9092']
        )
        
        # Define the topics to create
        topic_list = [
            NewTopic(name="tweets_topic", num_partitions=1, replication_factor=1),
            NewTopic(name="hashtag_topic", num_partitions=1, replication_factor=1),
            NewTopic(name="user_activity_topic", num_partitions=1, replication_factor=1)
        ]
        
        # Create topics
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        print("Created Kafka topics: tweets_topic, hashtag_topic, user_activity_topic")
    except Exception as e:
        print(f"Error creating Kafka topics: {e}")
        print("If topics already exist, this error can be ignored.")

def csv_to_kafka(csv_file, topic_name):
    """Read data from CSV and send to Kafka topic"""
    # Initialize Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    # Open and read the CSV file
    with open(csv_file, 'r', encoding='utf-8') as file:
        csv_reader = csv.DictReader(file)
        
        # Process each row and send to Kafka
        count = 0
        for row in csv_reader:
            # Clean and convert data
            tweet_data = {
                'tweet_id': int(row['Tweet_ID']),
                'username': row['Username'],
                'text': row['Text'],
                'retweets': int(row['Retweets']),
                'likes': int(row['Likes']),
                'timestamp': row['Timestamp']
            }
            
            # Send to Kafka
            producer.send(topic_name, tweet_data)
            
            count += 1
            if count % 1000 == 0:
                print(f"Sent {count} records to Kafka")
                producer.flush()
            
            # Add a small delay to prevent overwhelming the system
            time.sleep(0.001)  # Reduced for faster processing
    
    # Make sure all messages are sent
    producer.flush()
    print(f"Total of {count} records sent to Kafka")

if __name__ == "__main__":
    # First create the topics
    create_kafka_topics()
    
    # Then send data to the main topic
    csv_file = "../data/twitter_dataset.csv"
    topic_name = "tweets_topic"
    
    print(f"Starting to send data from {csv_file} to Kafka topic {topic_name}")
    csv_to_kafka(csv_file, topic_name)
    print("Data transfer completed")
