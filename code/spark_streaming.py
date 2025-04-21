from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re
import json
import psycopg2
from psycopg2 import extras
from kafka import KafkaProducer

# PostgreSQL connection parameters
pg_params = {
    'database': 'twitter_data',
    'user': 'postgres',
    'password': 'postgres',  # Change if your password is different
    'host': 'localhost',
    'port': '5432'
}

# Kafka producer for sending processed data to other topics
producer = None

def get_hashtags(text):
    """Extract hashtags from tweet text"""
    if text is None:
        return []
    # Find all hashtags using regex
    hashtags = re.findall(r'#(\w+)', text)
    return hashtags

# Create UDF for hashtag extraction
extract_hashtags_udf = udf(get_hashtags, ArrayType(StringType()))

def save_to_postgres(df, epoch_id):
    """Save DataFrame to PostgreSQL"""
    # Get the dataframe as a list of rows
    rows = df.collect()
    
    if not rows:
        return
    
    # Connect to PostgreSQL
    conn = psycopg2.connect(**pg_params)
    cursor = conn.cursor()
    
    try:
        # Process hashtag counts
        if "hashtag" in df.columns:
            hashtag_data = [(row.hashtag, row.count, row.window_end, row.timestamp) for row in rows]
            
            # Use batch insert with ON CONFLICT
            extras.execute_batch(cursor, """
                INSERT INTO hashtag_counts (hashtag, count, window_end, last_updated)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (hashtag, window_end)
                DO UPDATE SET 
                    count = EXCLUDED.count,
                    last_updated = EXCLUDED.last_updated
            """, hashtag_data)
            
            # Also send to hashtag_topic
            global producer
            if producer is not None:
                for row in rows:
                    hashtag_msg = {
                        "hashtag": row.hashtag,
                        "count": row.count,
                        "window_end": row.window_end.isoformat(),
                        "timestamp": row.timestamp.isoformat()
                    }
                    producer.send("hashtag_topic", hashtag_msg)
        
        # Process user activity
        elif "username" in df.columns and "tweet_count" in df.columns:
            user_data = [(row.username, row.tweet_count, row.total_likes, 
                        row.total_retweets, row.window_end, row.timestamp) for row in rows]
            
            extras.execute_batch(cursor, """
                INSERT INTO user_activity (username, tweet_count, total_likes, total_retweets, window_end, last_updated)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (username, window_end)
                DO UPDATE SET 
                    tweet_count = EXCLUDED.tweet_count,
                    total_likes = EXCLUDED.total_likes,
                    total_retweets = EXCLUDED.total_retweets,
                    last_updated = EXCLUDED.last_updated
            """, user_data)
            
            # Also send to user_activity_topic
            global producer
            if producer is not None:
                for row in rows:
                    user_msg = {
                        "username": row.username,
                        "tweet_count": row.tweet_count,
                        "total_likes": row.total_likes,
                        "total_retweets": row.total_retweets,
                        "window_end": row.window_end.isoformat(),
                        "timestamp": row.timestamp.isoformat()
                    }
                    producer.send("user_activity_topic", user_msg)
        
        # Process raw tweets
        elif "tweet_id" in df.columns:
            tweet_data = [(row.tweet_id, row.username, row.text, 
                        row.retweets, row.likes, row.timestamp) for row in rows]
            
            extras.execute_batch(cursor, """
                INSERT INTO tweets (tweet_id, username, tweet_text, retweets, likes, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (tweet_id)
                DO NOTHING
            """, tweet_data)
        
        # Commit and close
        conn.commit()
    except Exception as e:
        print(f"Error saving to PostgreSQL: {e}")
    finally:
        cursor.close()
        conn.close()

def process_stream():
    """Set up and process Spark Streaming from Kafka"""
    # Create Spark Session
    spark = SparkSession \
        .builder \
        .appName("TwitterStreamProcessor") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
        .getOrCreate()
    
    # Set log level
    spark.sparkContext.setLogLevel("WARN")
    
    # Initialize Kafka producer for sending to other topics
    global producer
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    # Define schema for tweets
    schema = StructType([
        StructField("tweet_id", IntegerType()),
        StructField("username", StringType()),
        StructField("text", StringType()),
        StructField("retweets", IntegerType()),
        StructField("likes", IntegerType()),
        StructField("timestamp", StringType())
    ])
    
    # Read from Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "tweets_topic") \
        .option("startingOffsets", "earliest") \
        .load()
    
    # Parse JSON data
    tweets_df = df \
        .select(from_json(col("value").cast("string"), schema).alias("tweet")) \
        .select("tweet.*") \
        .withColumn("timestamp", to_timestamp("timestamp"))
    
    # Store raw tweets
    query1 = tweets_df \
        .writeStream \
        .foreachBatch(save_to_postgres) \
        .outputMode("append") \
        .start()
        
    # Define window duration - 15 minutes as per project requirement
    window_duration = "15 minutes"
    slide_duration = "5 minutes"  # For sliding window
    
    # Extract hashtags and count within window
    hashtags_df = tweets_df \
        .withColumn("hashtags", extract_hashtags_udf(col("text"))) \
        .withColumn("hashtag", explode(col("hashtags"))) \
        .withWatermark("timestamp", "20 minutes") \
        .groupBy(
            window(col("timestamp"), window_duration, slide_duration),
            col("hashtag")
        ) \
        .agg(
            count("*").alias("count"),
            max("timestamp").alias("timestamp")
        ) \
        .select(
            col("hashtag"),
            col("count"),
            col("window.end").alias("window_end"),
            col("timestamp")
        )
    
    query2 = hashtags_df \
        .writeStream \
        .trigger(processingTime='30 seconds') \
        .foreachBatch(save_to_postgres) \
        .outputMode("complete") \
        .start()
    
    # Aggregate user activity within window
    user_activity_df = tweets_df \
        .withWatermark("timestamp", "20 minutes") \
        .groupBy(
            window(col("timestamp"), window_duration, slide_duration),
            col("username")
        ) \
        .agg(
            count("*").alias("tweet_count"),
            sum("likes").alias("total_likes"),
            sum("retweets").alias("total_retweets"),
            max("timestamp").alias("timestamp")
        ) \
        .select(
            col("username"),
            col("tweet_count"),
            col("total_likes"),
            col("total_retweets"),
            col("window.end").alias("window_end"),
            col("timestamp")
        )
    
    query3 = user_activity_df \
        .writeStream \
        .trigger(processingTime='30 seconds') \
        .foreachBatch(save_to_postgres) \
        .outputMode("complete") \
        .start()
    
    # Wait for termination
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    process_stream()
