-- Connect to twitter_data database
\c twitter_data;

-- Insert sample tweets
INSERT INTO tweets (tweet_id, username, tweet_text, retweets, likes, timestamp)
VALUES 
(1, 'user1', 'This is a #test tweet about #technology', 5, 10, NOW()),
(2, 'user2', 'Another #test tweet about #politics', 8, 15, NOW()),
(3, 'user1', 'A third tweet about #sports', 12, 20, NOW()),
(4, 'user3', 'Fourth tweet about #technology and #business', 7, 25, NOW()),
(5, 'user2', 'Fifth tweet about #news', 9, 18, NOW());

-- Insert sample hashtag counts
INSERT INTO hashtag_counts (hashtag, count, window_end, last_updated)
VALUES 
('technology', 25, NOW(), NOW()),
('politics', 18, NOW(), NOW()),
('sports', 15, NOW(), NOW()),
('business', 12, NOW(), NOW()),
('news', 10, NOW(), NOW()),
('test', 8, NOW(), NOW());

-- Insert sample user activity
INSERT INTO user_activity (username, tweet_count, total_likes, total_retweets, window_end, last_updated)
VALUES 
('user1', 10, 150, 80, NOW(), NOW()),
('user2', 8, 120, 60, NOW(), NOW()),
('user3', 5, 90, 40, NOW(), NOW());
