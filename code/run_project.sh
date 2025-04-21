#!/bin/bash

# Color codes for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== Twitter Data Processing Project (Complete Implementation) ===${NC}"

# Activate virtual environment (create if it doesn't exist)
if [ ! -d ~/twitter-venv ]; then
    echo -e "${YELLOW}Creating virtual environment...${NC}"
    python3 -m venv ~/twitter-venv
    source ~/twitter-venv/bin/activate
    pip install kafka-python psycopg2-binary pyspark
else
    source ~/twitter-venv/bin/activate
fi

# Create logs directory if it doesn't exist
mkdir -p ../logs

# Check PostgreSQL connection
echo -e "${YELLOW}Checking PostgreSQL connection...${NC}"
sudo -u postgres psql -c "\l" > /dev/null
if [ $? -ne 0 ]; then
    echo -e "${RED}Failed to connect to PostgreSQL. Please make sure it's running.${NC}"
    exit 1
fi
echo -e "${GREEN}PostgreSQL connection successful.${NC}"

# Check Kafka connection
echo -e "${YELLOW}Checking Kafka connection...${NC}"
nc -z localhost 9092 >/dev/null 2>&1
KAFKA_RUNNING=$?
if [ $KAFKA_RUNNING -ne 0 ]; then
    echo -e "${RED}Kafka is not running on port 9092. Please start Kafka before running this script.${NC}"
    echo "You can start Kafka using:"
    echo "  1. Start ZooKeeper: /usr/local/kafka/bin/zookeeper-server-start.sh /usr/local/kafka/config/zookeeper.properties"
    echo "  2. Start Kafka: /usr/local/kafka/bin/kafka-server-start.sh /usr/local/kafka/config/server.properties"
    exit 1
fi
echo -e "${GREEN}Kafka is running.${NC}"

# Setup PostgreSQL database
echo -e "${YELLOW}Setting up PostgreSQL database...${NC}"
sudo -u postgres psql -f postgres-setup.sql
if [ $? -ne 0 ]; then
    echo -e "${RED}Failed to setup PostgreSQL database. Please check the error message.${NC}"
    exit 1
fi
echo -e "${GREEN}PostgreSQL database setup completed.${NC}"

# Create a smaller dataset for faster testing
echo -e "${YELLOW}Creating a smaller dataset for testing...${NC}"
head -n 201 ../data/twitter_dataset.csv > ../data/twitter_dataset_small.csv
echo -e "${GREEN}Smaller dataset created at ../data/twitter_dataset_small.csv${NC}"

# Modify kafka producer to use smaller dataset
sed -i 's|DATA_FILE = "../data/twitter_dataset.csv"|DATA_FILE = "../data/twitter_dataset_small.csv"|' kafka_producer_full.py

# Create Kafka topics
echo -e "${YELLOW}Creating Kafka topics...${NC}"
/usr/local/kafka/bin/kafka-topics.sh --create --topic tweets_topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --if-not-exists
/usr/local/kafka/bin/kafka-topics.sh --create --topic hashtags_topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --if-not-exists
/usr/local/kafka/bin/kafka-topics.sh --create --topic user_activity_topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --if-not-exists
echo -e "${GREEN}Kafka topics created.${NC}"

# Start Kafka producer in background
echo -e "${YELLOW}Starting Kafka producer...${NC}"
python3 kafka_producer_full.py > ../logs/kafka_producer.log 2>&1 &
PRODUCER_PID=$!
echo -e "${GREEN}Kafka producer started with PID: $PRODUCER_PID${NC}"

# Wait a moment for producer to start sending messages
sleep 5

# Start Kafka consumer in background
echo -e "${YELLOW}Starting Kafka consumer...${NC}"
python3 kafka_consumer_full.py > ../logs/kafka_consumer.log 2>&1 &
CONSUMER_PID=$!
echo -e "${GREEN}Kafka consumer started with PID: $CONSUMER_PID${NC}"

# Start Spark streaming in background
echo -e "${YELLOW}Starting Spark streaming application...${NC}"
python3 spark_streaming_full.py > ../logs/spark_streaming.log 2>&1 &
SPARK_PID=$!
echo -e "${GREEN}Spark streaming started with PID: $SPARK_PID${NC}"

# Wait for processing to complete
echo -e "${YELLOW}Waiting for data processing to complete...${NC}"
echo "Press Enter when you're ready to proceed to batch analysis."
read

# Kill Kafka producer (it should be done by now)
echo -e "${YELLOW}Stopping Kafka producer...${NC}"
kill $PRODUCER_PID 2>/dev/null || true
echo -e "${GREEN}Kafka producer stopped.${NC}"

# Wait a bit longer for Spark to process all data
echo -e "${YELLOW}Waiting 30 seconds for Spark to process all data...${NC}"
sleep 30

# Stop Spark streaming and Kafka consumer
echo -e "${YELLOW}Stopping Spark streaming and Kafka consumer...${NC}"
kill $SPARK_PID 2>/dev/null || true
kill $CONSUMER_PID 2>/dev/null || true
echo -e "${GREEN}Spark streaming and Kafka consumer stopped.${NC}"

# Run batch analysis
echo -e "${YELLOW}Running batch analysis...${NC}"
python3 batch_processor.py
echo -e "${GREEN}Batch analysis completed.${NC}"

echo -e "${GREEN}=== Project execution completed ===${NC}"
echo "You can now examine the data in the PostgreSQL database:"
echo "  sudo -u postgres psql -d twitter_data"
echo "  SELECT * FROM hashtag_counts ORDER BY count DESC LIMIT 10;"
echo "  SELECT * FROM user_activity ORDER BY tweet_count DESC LIMIT 10;"
echo "  SELECT * FROM performance_metrics;"
