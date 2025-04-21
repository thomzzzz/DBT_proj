from kafka import KafkaProducer
import csv
import json
import time

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
            if count >= 50:  # Just send 50 records for quick testing
                break
                
            if count % 10 == 0:
                print(f"Sent {count} records to Kafka")
            
            # No delay to speed up processing
    
    # Make sure all messages are sent
    producer.flush()
    print(f"Total of {count} records sent to Kafka")

if __name__ == "__main__":
    try:
        # Send data to the main topic
        csv_file = "../data/twitter_dataset.csv"
        topic_name = "tweets_topic"
        
        print(f"Starting to send data from {csv_file} to Kafka topic {topic_name}")
        csv_to_kafka(csv_file, topic_name)
        print("Data transfer completed")
    except Exception as e:
        print(f"Error: {e}")
