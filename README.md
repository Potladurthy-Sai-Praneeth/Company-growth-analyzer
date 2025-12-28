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

## File Documentation

#### **main.py**
Main orchestration script that runs the entire pipeline.

**Key Components:**
- `MessagePipeline`: Main pipeline class that coordinates all services
- `initialize_services()`: Sets up Kafka, Redis, Cassandra, PostgreSQL, and LLM services
- `start_consumers()`: Starts async Kafka consumers for automatic message processing
- `_handle_chat_message()`: Consumer callback for processing chat messages from Kafka
- `_handle_email_message()`: Consumer callback for processing email messages from Kafka
- `_check_and_trigger_llm()`: Checks cache threshold and triggers LLM processing
- `_process_popped_messages_with_llm()`: Async LLM event extraction task
- `process_csv_data()`: Reads CSV file and produces messages to Kafka
- `wait_for_consumers()`: Waits for consumers to finish processing messages
- `process_remaining_messages()`: Processes cached messages below threshold
- `wait_for_llm_tasks()`: Waits for all async LLM tasks to complete
- `generate_investor_email()`: Creates investor outreach email from extracted events
- `run()`: Executes the complete pipeline workflow

**Consumer Architecture:**
The pipeline uses **async Kafka consumers** that automatically process messages as they arrive:
1. Consumers register handler functions for chat and email topics
2. When a message arrives, the appropriate handler is called asynchronously
3. Handlers store messages in both cache (Redis) and database (Cassandra) simultaneously
4. After each message, checks if cache threshold (200) is reached
5. If threshold met, spawns async task to process messages with LLM
6. Uses lock mechanism to prevent duplicate LLM triggers from concurrent handlers

---

### Service Files

#### **kafka_service.py**
Kafka Queue Service for message production and asynchronous consumption.

**Features:**
- Creates and manages Kafka topics (slack-messages, gmail-messages)
- Producer: Sends messages to topics with retry logic and validation
- Async Consumer: Automatically processes messages from topics in real-time
- Handler registration system for message callbacks
- Dead letter queue for failed messages
- Health monitoring and metrics tracking

**Key Methods:**
- `initialize()`: Sets up Kafka admin client, creates topics, and initializes producer
- `produce_message()`: Sends a message to specified topic with validation
- `start_consuming()`: Starts async consumers for processing messages from topics
- `register_message_handler()`: Registers callback functions for message processing
- `_consume_topic()`: Internal async loop that consumes messages and calls handlers
- `close()`: Gracefully shuts down producer and consumers

**Consumer Pattern:**
- Consumers run in async background tasks
- Each topic (chat/email) has dedicated consumer
- Messages automatically trigger registered handler functions
- Non-blocking architecture allows parallel message processing

---

#### **cache_service.py**
Redis Cache Service for temporary message storage and threshold-based triggering.

**Features:**
- Caches messages by company_id and source (slack/gmail)
- Automatic TTL expiration (24 hours default)
- Threshold-based processing triggers
- Thread-safe operations with connection pooling
- Processing locks to prevent duplicate processing during async operations

**Key Methods:**
- `add_message()`: Adds message to cache
- `get_message_count()`: Returns count of cached messages for a source
- `get_messages()`: Retrieves messages from cache
- `pop_messages()`: Atomically removes and returns messages from cache
- `release_processing_lock()`: Releases lock after LLM processing
- `get_cache_stats()`: Returns cache statistics
- `close()`: Closes Redis connection

**Cache Keys:**
- `messages:{company_id}:{source}:queue` - Message storage (list)
- `messages:{company_id}:{source}:metadata` - Cache metadata
- `messages:{company_id}:{source}:processing` - Processing lock flag

**Async Pattern:**
- Supports concurrent access from multiple consumer handlers
- Messages are popped atomically to prevent duplicate processing
- Lock mechanism ensures only one LLM task processes a batch

---

#### **database_service.py**
Cassandra Database Service for persistent message storage.

**Features:**
- Async operations using ThreadPoolExecutor
- Separate tables for chat and email messages
- Time-series optimized schema with clustering
- Connection pooling and retry logic
- Prepared statements for performance

**Schema:**
- `chat_messages`: Slack message storage (partitioned by company_id, ordered by timestamp)
- `email_messages`: Gmail message storage (partitioned by company_id, ordered by timestamp)
- `message_metadata`: Quick lookup table for message status and processing info

**Key Methods:**
- `initialize()`: Creates keyspace, tables, and prepared statements
- `store_message()`: Stores a message in appropriate table based on type
- `get_messages()`: Retrieves messages with optional time range filtering
- `update_message_status()`: Updates processing status of a message

---

#### **events_database.py**
PostgreSQL Database Service for storing extracted significant events.

**Features:**
- Async operations using asyncpg connection pool
- Pydantic model validation
- JSONB storage for flexible metrics and stakeholders
- Comprehensive indexing for fast queries
- Batch operations for efficiency

**Schema:**
- `significant_events` table with fields:
  - `id`, `event_type`, `title`, `description`, `impact`
  - `metrics` (JSONB), `timeline`, `source_messages` (JSONB)
  - `severity`, `stakeholders` (JSONB), `company_id`
  - Indexes on: company_id, event_type, created_at, severity, is_active

**Key Methods:**
- `store_event()`: Stores a single SignificantEvent with upsert logic
- `store_events_batch()`: Stores multiple events in a transaction
- `get_all_events()`: Retrieves events with filtering options
- `get_event_statistics()`: Returns aggregated statistics on events

---

#### **llm_service.py**
LLM Service using Google Gemini for intelligent event extraction and email generation.

**Features:**
- Uses Gemini 3 Pro Preview model via LangChain
- Structured prompts for consistent output
- JSON output parsing with fallback handling
- Handles both event extraction and email generation

**Key Methods:**
- `extract_events()`: Analyzes messages and extracts significant events
  - Input: List of message dictionaries (Slack + Gmail)
  - Output: EventExtractionResult with list of SignificantEvent models
  - Uses detailed system prompt to identify 14 event types
  
- `generate_investor_email()`: Creates personalized investor outreach
  - Input: List of SignificantEvent models
  - Output: InvestorEmail with subject, body, highlights, metrics
  - Prioritizes high-impact events and formats data appealingly

**Event Types Detected:**
- User growth, revenue milestones, funding, partnerships
- Product launches, hiring, enterprise deals
- Technical achievements, market expansion, compliance
- Media coverage, investor interest, customer success

---

### Data Processing Files

#### **process_message.py**
Message processors that convert CSV data into API-formatted messages.

**Classes:**

1. **SlackProcessor**: Converts chat messages to Slack API format
   - Creates message blocks with rich text formatting
   - Generates unique client_msg_id and timestamps
   - Formats: user, text, ts (timestamp), channel, team

2. **GmailProcessor**: Converts emails to Gmail API format
   - Extracts subject from message content
   - Base64 encodes message body
   - Formats: id, threadId, snippet, payload with headers
   - Creates labeled messages (INBOX, UNREAD)

---

#### **models.py**
Pydantic models for data validation and structure.

**Models:**

1. **EventType** (Enum): 14 types of significant events
2. **EventSeverity** (Enum): high, medium, low
3. **SignificantEvent**: Complete event model with validation
   - Validates title length (5-200 chars)
   - Ensures meaningful descriptions
   - Supports optional metrics as key-value dict
4. **EventExtractionResult**: LLM extraction output wrapper
5. **InvestorEmail**: Generated email structure
6. **MessageBatch**: Batch tracking for LLM processing
7. **ProcessingStatus**: Pipeline status tracking

---

### Utility Files

#### **utils.py**
Shared utility functions used across the project.

**Functions:**

- `setup_logging()`: Configures production logging
  - Console output with timestamps
  - Rotating file logs (10MB, 5 backups)
  - Module-specific log files
  
- `load_config()`: Loads YAML configuration
  - Searches multiple possible paths
  - Returns configuration dictionary
  
- `validate_message_format()`: Validates message structure
  - Checks required fields for chat/email types
  
- `extract_message_metadata()`: Extracts metadata from messages
  - Returns topic, source, company_id, timestamp

---

### Auxiliary Files

#### **flush_databases.py**
Utility script to clean all databases before a fresh run.

**Operations:**
- Flushes all Redis data (`FLUSHALL`)
- Drops Cassandra keyspace `message_queue_db`
- Truncates PostgreSQL `significant_events` table

**Usage:**
```bash
python flush_databases.py
```

---

#### **generate_data.py**
Generates synthetic startup communication data for testing.

**Features:**
- Creates 1000 rows of simulated messages (85% chat, 15% email)
- Uses templates and dynamic generation for variety
- Simulates realistic startup scenarios:
  - Team discussions about features, bugs, scaling
  - Investor updates with metrics (MRR, NRR, DAU)
  - Enterprise sales conversations
  - Security and compliance communications

**Templates Include:**
- Technical roadmap discussions
- Vulnerability disclosures
- Monthly investor updates
- Customer feedback
- Infrastructure scaling issues

**Output:** `data/full_data.csv` with columns:
- id, is_chat, chat_sender, email_sender, timestamp, content

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

