from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re
import json
import psycopg2
from psycopg2 import extras
import time
from datetime import datetime

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

def save_to_postgres(table_name):
    """Returns a function that saves a DataFrame to PostgreSQL table"""
    def _save_to_postgres(df, epoch_id):
        """Save DataFrame to PostgreSQL"""
        # Get the dataframe as a list of rows
        rows = df.collect()
        
        if not rows:
            return
        
        try:
            # Connect to PostgreSQL
            conn = psycopg2.connect(**pg_params)
            cursor = conn.cursor()
            
            # Save metrics about this batch
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            execution_time = time.time() * 1000  # Simulated execution time in ms
            
            # Log performance metrics
            cursor.execute("""
                INSERT INTO performance_metrics 
                (query_id, execution_mode, execution_time, window_size, records_processed, execution_timestamp)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (f"spark_streaming_{table_name}", "streaming", execution_time, "15 minutes", len(rows), now))
            
            if table_name == "hashtag_counts":
                hashtag_data = [(row.hashtag, row.count, row.window_end, row.timestamp) for row in rows]
                
                extras.execute_batch(cursor, """
                    INSERT INTO hashtag_counts (hashtag, count, window_end, last_updated)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (hashtag, window_end)
                    DO UPDATE SET 
                        count = EXCLUDED.count,
                        last_updated = EXCLUDED.last_updated
                """, hashtag_data)
                
            elif table_name == "user_activity":
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
                
            elif table_name == "tweets":
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
            cursor.close()
            conn.close()
            
            print(f"Batch {epoch_id}: Saved {len(rows)} records to {table_name}")
            
        except Exception as e:
            print(f"Error saving to PostgreSQL: {e}")
    
    return _save_to_postgres

def get_hashtags(text):
    """Extract hashtags from tweet text"""
    if text is None:
        return []
    # Find all hashtags using regex
    hashtags = re.findall(r'#(\w+)', text)
    return hashtags

def create_spark_session():
    """Create and configure Spark session"""
    return SparkSession \
        .builder \
        .appName("TwitterStreamProcessor") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
        .getOrCreate()

def process_tweets_stream(spark):
    """Process tweets stream from Kafka"""
    # Define schema for tweets
    tweet_schema = StructType([
        StructField("tweet_id", IntegerType()),
        StructField("username", StringType()),
        StructField("text", StringType()),
        StructField("retweets", IntegerType()),
        StructField("likes", IntegerType()),
        StructField("timestamp", StringType())
    ])
    
    # Read from Kafka
    tweets_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", TWEETS_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load() \
        .select(from_json(col("value").cast("string"), tweet_schema).alias("tweet")) \
        .select("tweet.*") \
        .withColumn("timestamp", to_timestamp("timestamp"))
    
    # Store raw tweets
    query1 = tweets_df \
        .writeStream \
        .foreachBatch(save_to_postgres("tweets")) \
        .outputMode("append") \
        .start()
    
    return tweets_df, query1

def process_hashtags_stream(tweets_df):
    """Process hashtags from tweets stream"""
    # Create UDF for hashtag extraction
    extract_hashtags_udf = udf(get_hashtags, ArrayType(StringType()))
    
    # Define window duration
    window_duration = "15 minutes"
    slide_duration = "5 minutes"
    
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
        .foreachBatch(save_to_postgres("hashtag_counts")) \
        .outputMode("complete") \
        .start()
    
    return hashtags_df, query2

def process_user_activity_stream(tweets_df):
    """Process user activity from tweets stream"""
    # Define window duration
    window_duration = "15 minutes"
    slide_duration = "5 minutes"
    
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
        .foreachBatch(save_to_postgres("user_activity")) \
        .outputMode("complete") \
        .start()
    
    return user_activity_df, query3

def process_direct_hashtags_stream(spark):
    """Process hashtags directly from the hashtags topic"""
    # Define schema for hashtag messages
    hashtag_schema = StructType([
        StructField("tweet_id", IntegerType()),
        StructField("hashtag", StringType()),
        StructField("username", StringType()),
        StructField("timestamp", StringType())
    ])
    
    # Read from Kafka hashtags topic
    hashtags_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", HASHTAGS_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load() \
        .select(from_json(col("value").cast("string"), hashtag_schema).alias("hashtag_data")) \
        .select("hashtag_data.*") \
        .withColumn("timestamp", to_timestamp("timestamp"))
    
    # Define window duration
    window_duration = "15 minutes"
    slide_duration = "5 minutes"
    
    # Count hashtags within window
    hashtags_count_df = hashtags_df \
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
    
    query4 = hashtags_count_df \
        .writeStream \
        .trigger(processingTime='30 seconds') \
        .foreachBatch(save_to_postgres("hashtag_counts")) \
        .outputMode("complete") \
        .start()
    
    return hashtags_count_df, query4

def process_direct_user_activity_stream(spark):
    """Process user activity directly from the user activity topic"""
    # Define schema for user activity messages
    user_schema = StructType([
        StructField("username", StringType()),
        StructField("tweet_count", IntegerType()),
        StructField("hashtag_count", IntegerType()),
        StructField("retweets", IntegerType()),
        StructField("likes", IntegerType()),
        StructField("timestamp", StringType())
    ])
    
    # Read from Kafka user activity topic
    user_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", USER_ACTIVITY_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load() \
        .select(from_json(col("value").cast("string"), user_schema).alias("user_data")) \
        .select("user_data.*") \
        .withColumn("timestamp", to_timestamp("timestamp"))
    
    # Define window duration
    window_duration = "15 minutes"
    slide_duration = "5 minutes"
    
    # Aggregate user activity within window
    user_activity_df = user_df \
        .withWatermark("timestamp", "20 minutes") \
        .groupBy(
            window(col("timestamp"), window_duration, slide_duration),
            col("username")
        ) \
        .agg(
            sum("tweet_count").alias("tweet_count"),
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
    
    query5 = user_activity_df \
        .writeStream \
        .trigger(processingTime='30 seconds') \
        .foreachBatch(save_to_postgres("user_activity")) \
        .outputMode("complete") \
        .start()
    
    return user_activity_df, query5

def main():
    """Main entry point for the streaming application"""
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print("Starting Spark Streaming application...")
    
    # Process streams
    tweets_df, query1 = process_tweets_stream(spark)
    hashtags_df, query2 = process_hashtags_stream(tweets_df)
    user_activity_df, query3 = process_user_activity_stream(tweets_df)
    
    # Process direct topic streams
    direct_hashtags_df, query4 = process_direct_hashtags_stream(spark)
    direct_user_df, query5 = process_direct_user_activity_stream(spark)
    
    print("All streaming queries started. Waiting for termination...")
    
    try:
        # Wait for termination
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("Stopping streams...")
        query1.stop()
        query2.stop()
        query3.stop()
        query4.stop()
        query5.stop()
        print("All streams stopped.")

if __name__ == "__main__":
    main()
