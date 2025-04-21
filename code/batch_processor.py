import time
import psycopg2
from datetime import datetime

# PostgreSQL connection parameters
pg_params = {
    'database': 'twitter_data',
    'user': 'postgres',
    'password': 'postgres',  # Change if your password is different
    'host': 'localhost',
    'port': '5432'
}

def execute_query(query, query_id=None):
    """Execute SQL query and return results with timing"""
    conn = psycopg2.connect(**pg_params)
    cursor = conn.cursor()
    
    start_time = time.time()
    cursor.execute(query)
    end_time = time.time()
    
    results = cursor.fetchall()
    results = cursor.fetchall()
    execution_time = (end_time - start_time) * 1000  # Convert to milliseconds
    
    # Log performance metrics if query_id is provided
    if query_id:
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute("""
            INSERT INTO performance_metrics 
            (query_id, execution_mode, execution_time, window_size, records_processed, execution_timestamp)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (query_id, "batch", execution_time, "all", len(results), now))
        conn.commit()
    
    cursor.close()
    conn.close()
    
    return results, execution_time

def batch_analysis():
    """Perform batch analysis on stored data"""
    print("\n===== Twitter Data Batch Analysis =====")
    
    # Analysis 1: Top hashtags
    print("\n--- Analysis 1: Top hashtags ---")
    hashtag_query = """
        SELECT hashtag, SUM(count) as total_count 
        FROM hashtag_counts 
        GROUP BY hashtag
        ORDER BY total_count DESC 
        LIMIT 10
    """
    
    hashtag_results, hashtag_time = execute_query(hashtag_query, "batch_top_hashtags")
    
    print(f"Batch execution time: {hashtag_time:.2f} ms")
    print("Top Hashtags:")
    for hashtag, count in hashtag_results:
        print(f"#{hashtag}: {count}")
    
    # Analysis 2: Users by tweet count
    print("\n--- Analysis 2: Users by tweet count ---")
    user_query = """
        SELECT username, SUM(tweet_count) as total_tweets 
        FROM user_activity 
        GROUP BY username
        ORDER BY total_tweets DESC 
        LIMIT 10
    """
    
    user_results, user_time = execute_query(user_query, "batch_top_users")
    
    print(f"Batch execution time: {user_time:.2f} ms")
    print("Top Users by Tweet Count:")
    for username, count in user_results:
        print(f"{username}: {count} tweets")
    
    # Analysis 3: Average likes per user
    print("\n--- Analysis 3: Average likes per user ---")
    avg_likes_query = """
        SELECT 
            username, 
            SUM(total_likes) as sum_likes,
            SUM(tweet_count) as sum_tweets,
            CASE 
                WHEN SUM(tweet_count) > 0 THEN SUM(total_likes)::float / SUM(tweet_count) 
                ELSE 0 
            END as avg_likes_per_tweet
        FROM user_activity
        GROUP BY username
        ORDER BY avg_likes_per_tweet DESC
        LIMIT 10
    """
    
    avg_likes_results, avg_likes_time = execute_query(avg_likes_query, "batch_avg_likes")
    
    print(f"Batch execution time: {avg_likes_time:.2f} ms")
    print("Users by Average Likes per Tweet:")
    for row in avg_likes_results:
        username, total_likes, tweet_count, avg_likes = row
        print(f"{username}: {avg_likes:.2f} avg likes ({total_likes} total likes over {tweet_count} tweets)")

def compare_performance():
    """Compare performance between streaming and batch modes"""
    print("\n===== Performance Comparison: Streaming vs Batch =====")
    
    comparison_query = """
        SELECT 
            query_id,
            execution_mode,
            AVG(execution_time) as avg_time,
            COUNT(*) as executions
        FROM performance_metrics
        GROUP BY query_id, execution_mode
        ORDER BY query_id, execution_mode
    """
    
    results, _ = execute_query(comparison_query)
    
    # Organize results by query type
    streaming_metrics = {}
    batch_metrics = {}
    
    for row in results:
        query_id, mode, avg_time, executions = row
        if mode.startswith('streaming'):
            streaming_metrics[query_id] = (avg_time, executions)
        else:
            batch_metrics[query_id] = (avg_time, executions)
    
    # Print comparison table
    print(f"\n{'Query Type':<25} {'Streaming Avg (ms)':<20} {'Batch Avg (ms)':<20} {'Difference':<15} {'Speedup Factor':<15}")
    print("-" * 95)
    
    # Match similar queries for comparison
    comparisons = []
    for batch_id, batch_data in batch_metrics.items():
        batch_type = batch_id.replace('batch_', '')
        for stream_id, stream_data in streaming_metrics.items():
            if batch_type in stream_id:
                comparisons.append((batch_type, stream_data, batch_data))
    
    for query_type, stream_data, batch_data in comparisons:
        stream_avg, stream_count = stream_data
        batch_avg, batch_count = batch_data
        
        diff = abs(stream_avg - batch_avg)
        if batch_avg > 0 and stream_avg > 0:  # Avoid division by zero
            factor = stream_avg / batch_avg if stream_avg > batch_avg else batch_avg / stream_avg
            faster = "Streaming" if stream_avg < batch_avg else "Batch"
            print(f"{query_type:<25} {stream_avg:<20.2f} {batch_avg:<20.2f} {diff:<15.2f} {faster} is {factor:.2f}x faster")
    
    print("\nNote: The performance difference is affected by various factors including:")
    print("- Data volume")
    print("- Query complexity")
    print("- System resources")
    print("- Streaming involves overhead for maintaining state across windows")

if __name__ == "__main__":
    try:
        print("Starting batch analysis and performance comparison...")
        batch_analysis()
        compare_performance()
        print("\nAnalysis complete.")
    except Exception as e:
        print(f"Error in batch processing: {e}")
