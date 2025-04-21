-- Connect to default database first
\c postgres;

-- Create twitter_data database
DROP DATABASE IF EXISTS twitter_data;
CREATE DATABASE twitter_data;

-- Connect to the newly created database
\c twitter_data;

-- Create tweets table to store raw tweet data
CREATE TABLE tweets (
    tweet_id INTEGER PRIMARY KEY,
    username VARCHAR(255),
    tweet_text TEXT,
    retweets INTEGER,
    likes INTEGER,
    timestamp TIMESTAMP
);

-- Create hashtag_counts table to store hashtag analysis with window support
CREATE TABLE hashtag_counts (
    hashtag VARCHAR(255),
    count INTEGER,
    window_end TIMESTAMP,
    last_updated TIMESTAMP,
    PRIMARY KEY (hashtag, window_end)
);

-- Create user_activity table to store user statistics with window support
CREATE TABLE user_activity (
    username VARCHAR(255),
    tweet_count INTEGER,
    total_likes INTEGER,
    total_retweets INTEGER,
    window_end TIMESTAMP,
    last_updated TIMESTAMP,
    PRIMARY KEY (username, window_end)
);

-- Create tables for comparison
CREATE TABLE stream_hashtags (
    hashtag VARCHAR(255),
    count INTEGER,
    processed_at TIMESTAMP
);

CREATE TABLE stream_users (
    username VARCHAR(255),
    tweet_count INTEGER,
    total_likes INTEGER,
    total_retweets INTEGER,
    processed_at TIMESTAMP
);

CREATE TABLE batch_hashtags (
    hashtag VARCHAR(255),
    count INTEGER,
    processed_at TIMESTAMP
);

CREATE TABLE batch_users (
    username VARCHAR(255),
    tweet_count INTEGER,
    total_likes INTEGER,
    total_retweets INTEGER,
    processed_at TIMESTAMP
);

CREATE TABLE raw_tweets (
    tweet_id INTEGER PRIMARY KEY,
    username VARCHAR(255),
    tweet_text TEXT,
    retweets INTEGER,
    likes INTEGER,
    timestamp TIMESTAMP
);

-- Create indexes for better performance
CREATE INDEX tweets_username_idx ON tweets(username);
CREATE INDEX tweets_timestamp_idx ON tweets(timestamp);
CREATE INDEX hashtag_counts_window_idx ON hashtag_counts(window_end);
CREATE INDEX user_activity_window_idx ON user_activity(window_end);

-- Create a table for storing performance metrics
CREATE TABLE performance_metrics (
    query_id VARCHAR(255),
    execution_mode VARCHAR(50),
    execution_time NUMERIC,
    window_size VARCHAR(50),
    records_processed INTEGER,
    execution_timestamp TIMESTAMP,
    PRIMARY KEY (query_id, execution_mode, execution_timestamp)
);
