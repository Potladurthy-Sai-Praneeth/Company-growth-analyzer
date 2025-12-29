# Message Processing & Event Extraction Workflow

A production-ready event-driven pipeline that processes company's communications (Slack messages and emails), extracts significant business events using LLM, and generates investor outreach emails.

--- 

### Key Features

- **Real-time Message Processing**: Kafka-based queue system for handling high-throughput message streams
- **Intelligent Event Extraction**: Google Gemini LLM identifies significant business milestones from communication channels
- **Efficient Caching**: Redis cache with threshold-based batch processing
- **Scalable Storage**: Cassandra for messages, PostgreSQL for events
- **Automated Email Generation**: AI-generated investor outreach emails based on extracted events


### Components

1. **Kafka**: Message queue with async consumers for real-time processing
2. **Redis**: Temporary cache with threshold-based batch triggering
3. **Cassandra**: Persistent storage for raw messages (chat and email)
4. **PostgreSQL**: Structured storage for extracted significant events
5. **Google Gemini LLM**: Event extraction and email generation

---

## Setup & Installation

### 1. Install Required Services

#### Install Java (required for Kafka and Cassandra)

```bash
sudo apt update
sudo apt install -y openjdk-11-jdk
java -version
```

#### Install Apache Kafka

[Kafka Installation Guide](https://www.cherryservers.com/blog/install-apache-kafka-ubuntu)


#### Install Redis

```bash
# Install Redis
sudo apt update
sudo apt install -y redis-server

# Start Redis service
sudo systemctl start redis-server
sudo systemctl enable redis-server

# Verify Redis is running
redis-cli ping
```

#### Install Apache Cassandra

[Cassandra Installation Guide](https://www.hostinger.com/tutorials/set-up-and-install-cassandra-ubuntu)

#### Install PostgreSQL

```bash
# Install PostgreSQL
sudo apt update
sudo apt install -y postgresql postgresql-contrib

# Start PostgreSQL service
sudo systemctl start postgresql
sudo systemctl enable postgresql

# Create database and user
sudo -u postgres psql -c "CREATE DATABASE events_db;"
sudo -u postgres psql -c "ALTER USER postgres PASSWORD 'postgres';"

# Verify PostgreSQL is running
sudo systemctl status postgresql
```

**Note:** After installation, all services will be running in the background. To stop them:

```bash
# Stop Kafka 
/opt/kafka/bin/kafka-server-stop.sh
sudo systemctl stop kafka

# Stop Redis
sudo systemctl stop redis-server

# Stop Cassandra
sudo systemctl stop cassandra

# Stop PostgreSQL
sudo systemctl stop postgresql
```

### 2. Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 3. Set Up Environment Variables

Create a `.env` file in the project root:

```bash
# Google Gemini API Key
GOOGLE_API_KEY=your_google_api_key_here

# Database Credentials (optional, uses defaults if not set)
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=events_db
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres

```

### 4. Generate Sample Data (Optional)

If you want to generate new sample data:

```bash
python generate_data.py
```

This creates `data/full_data.csv` with 1000 simulated messages.

---

## Running the Pipeline

### Method 1: Complete Pipeline Run 

```bash
python main.py
```

**What happens:**
1. Initializes all services (Kafka, Redis, Cassandra, PostgreSQL, LLM)
2. Starts async Kafka consumers that listen to chat and email topics
3. Reads and produces messages from `data/full_data.csv` to Kafka
4. Consumers automatically process each message as it arrives:
   - Store in cache (Redis) and database (Cassandra) simultaneously
   - Check if cache threshold (200 messages) is reached
   - If threshold met, trigger async LLM processing
5. Waits for all consumers to finish processing messages
6. Processes any remaining cached messages below threshold
7. Waits for all async LLM tasks to complete
8. Generates investor email from all extracted events
9. Outputs results to `output/pipeline_results.md`

**Key Features:**
- **Async Processing**: Consumers process messages in real-time as they arrive
- **Parallel Execution**: Multiple LLM tasks can run concurrently
- **Automatic Batching**: Cache automatically triggers LLM when threshold is met
- **Non-blocking**: CSV reading doesn't wait for message processing

### Method 2: Flush Databases Before Running

To start with clean databases (recommended for fresh runs):

```bash
python flush_databases.py
python main.py
```

This ensures no partial data from previous runs interferes with the current execution.

---

## Configuration

### Key Configuration Parameters

**Application Settings:**
- `cache_trigger_threshold`: Number of messages to trigger LLM processing (default: 200)
- `data_file_path`: Path to input CSV file (default: `data/full_data.csv`)
- `company_id`: Company identifier for message tracking (default: `scaleflow`)

**Redis Cache:**
- `message_limit`: Max messages per source before trigger (default: 100)
- `ttl`: Cache expiration time in seconds (default: 86400 = 24 hours)
- Connection pooling for concurrent access from multiple consumers

**Kafka Configuration:**
- **Topics**: 
  - `slack-messages`: Chat message topic
  - `gmail-messages`: Email message topic
- **Consumer Groups**: Async consumers with automatic offset management
- **Auto-commit**: Enabled for automatic offset tracking


## System Architecture

![Architecture Diagram](system_architecture.png)

### Architecture Overview

The pipeline uses an **async event-driven architecture** with Kafka consumers that automatically process messages as they arrive:

1. **CSV Processing**: Reads data and produces messages to Kafka topics
2. **Kafka Topics**: Separate topics for chat (`slack-messages`) and email (`gmail-messages`)
3. **Async Consumers**: Automatically consume and process messages in real-time
4. **Parallel Storage**: Messages are simultaneously cached in Redis and stored in Cassandra
5. **Threshold-Based LLM**: When cache reaches 200 messages, triggers batch LLM processing
6. **Event Storage**: Extracted events stored in PostgreSQL for structured querying
7. **Email Generation**: Final step generates investor outreach email from all events

### Key Architectural Patterns

**1. Producer-Consumer Pattern**
- CSV data is read and produced to Kafka topics
- Async consumers automatically process messages as they arrive
- Decouples data ingestion from processing for scalability

**2. Async Event-Driven Processing**
- Kafka consumers run in background async tasks
- Message handlers are called asynchronously for each message
- Non-blocking architecture allows high throughput

**3. Batch Processing with Thresholds**
- Messages accumulate in Redis cache
- When threshold (200) is reached, triggers LLM processing
- Async tasks handle LLM calls without blocking consumers
- Lock mechanism prevents duplicate processing

**4. Parallel LLM Execution**
- Multiple LLM tasks can run concurrently
- Each batch is processed in separate async task
- Results are aggregated at the end
