#!/bin/bash

# Color codes for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== Twitter Data Processing Project (Demo) ===${NC}"

# Activate virtual environment
source ~/twitter-venv/bin/activate

# Setup PostgreSQL database
echo -e "${YELLOW}Setting up PostgreSQL database...${NC}"
sudo -u postgres psql -f postgres-setup.sql
if [ $? -ne 0 ]; then
    echo -e "${RED}Failed to setup PostgreSQL database. Please check the error message.${NC}"
    exit 1
fi
echo -e "${GREEN}PostgreSQL database setup completed.${NC}"

# Populate database with sample data
echo -e "${YELLOW}Populating database with sample data...${NC}"
sudo -u postgres psql -f populate_db.sql
if [ $? -ne 0 ]; then
    echo -e "${RED}Failed to populate database. Please check the error message.${NC}"
    exit 1
fi
echo -e "${GREEN}Database populated with sample Twitter data.${NC}"

# Run the simplified batch analysis
echo -e "${YELLOW}Running batch analysis...${NC}"
python3 simple_batch.py
if [ $? -ne 0 ]; then
    echo -e "${RED}Batch analysis failed.${NC}"
else
    echo -e "${GREEN}Batch analysis completed successfully.${NC}"
fi

echo -e "${GREEN}=== Project demonstration completed ===${NC}"
echo "You can now examine the data in the PostgreSQL database:"
echo "  sudo -u postgres psql -d twitter_data"
echo "  SELECT * FROM hashtag_counts ORDER BY count DESC LIMIT 10;"
