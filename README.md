# Sago Workflow - Message Processing & Event Extraction Pipeline

A production-ready event-driven pipeline that processes company's communications (Slack messages and emails), extracts significant business events using LLM, and generates investor outreach emails.
--- 

### Key Features

- **Real-time Message Processing**: Kafka-based queue system for handling high-throughput message streams
- **Intelligent Event Extraction**: Google Gemini LLM identifies significant business milestones from communications
- **Efficient Caching**: Redis cache with threshold-based batch processing
- **Scalable Storage**: Cassandra for messages, PostgreSQL for events
- **Automated Email Generation**: AI-generated investor outreach emails based on extracted events

---

## System Architecture

```
CSV Data → Kafka Topics → Cache (Redis) → Database (Cassandra)
                             ↓
                     Threshold Reached (200 messages)
                             ↓
                        LLM Processing (Gemini)
                             ↓
                    Events Database (PostgreSQL)
                             ↓
                   Investor Email Generation
```

### Components

1. **Kafka**: Message queue for decoupling data ingestion from processing
2. **Redis**: Temporary cache for batching messages before LLM processing
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

This will:
1. Initialize all services (Kafka, Redis, Cassandra, PostgreSQL)
2. Process messages from `data/full_data.csv`
3. Extract events using LLM when cache reaches 200 messages
4. Generate an investor email from all extracted events
5. Output results to `output/pipeline_results.md`

### Method 2: Flush Databases Before Running (Delete partial runs from previous attempts if any)

To start with clean databases:

```bash
python flush_databases.py
python main.py
```
---

## File Documentation

#### **main.py**
Main orchestration script that runs the entire pipeline.

**Key Components:**
- `MessagePipeline`: Main pipeline class that coordinates all services
- `initialize_services()`: Sets up Kafka, Redis, Cassandra, PostgreSQL, and LLM services
- `process_csv_data()`: Reads CSV file and sends messages to Kafka
- `_store_message_directly()`: Stores messages in cache and database
- `_process_messages_with_llm()`: Triggers LLM event extraction when cache reaches threshold
- `generate_investor_email()`: Creates investor outreach email from extracted events
- `run()`: Executes the complete pipeline workflow

**Flow:**
1. Load configuration from `config.yaml`
2. Initialize all services
3. Read CSV data row by row
4. Process each row as Slack or Gmail message
5. Store in cache and database
6. When cache reaches 200 messages, extract events using LLM
7. After all data processed, generate investor email
8. Write results to markdown file

---

### Service Files

#### **kafka_service.py**
Kafka Queue Service for message production and consumption.

**Features:**
- Creates and manages Kafka topics (slack-messages, gmail-messages)
- Producer: Sends messages to topics with retry logic
- Consumer: Asynchronously processes messages from topics
- Dead letter queue for failed messages
- Health monitoring and metrics tracking

**Key Methods:**
- `initialize()`: Sets up Kafka admin client, creates topics, and initializes producer
- `produce_message()`: Sends a message to specified topic with validation
- `start_consuming()`: Starts async consumers for processing messages
- `register_message_handler()`: Registers callback functions for message processing

---

#### **cache_service.py**
Redis Cache Service for temporary message storage and batch triggering.

**Features:**
- Caches messages by company_id and source (slack/gmail)
- Automatic TTL expiration (24 hours default)
- Threshold-based processing triggers
- Thread-safe operations with connection pooling
- Processing locks to prevent duplicate processing

**Key Methods:**
- `add_message()`: Adds message to cache, returns if threshold reached
- `get_messages()`: Retrieves messages from cache
- `pop_messages()`: Removes and returns messages from cache
- `poll_for_processing()`: Checks all queues for threshold triggers
- `get_cache_stats()`: Returns cache statistics

**Cache Keys:**
- `messages:{company_id}:{source}:queue` - Message storage
- `messages:{company_id}:{source}:metadata` - Cache metadata
- `messages:{company_id}:{source}:processing` - Processing lock flag

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
- `data_file_path`: Path to input CSV file
- `company_id`: Company identifier for message tracking

**Redis Cache:**
- `message_limit`: Messages per source before trigger (default: 100)
- `ttl`: Cache expiration time in seconds (default: 86400 = 24 hours)
- `poll_interval`: Polling frequency in seconds

**Kafka Topics:**
- `slack-messages`: Chat message topic
- `gmail-messages`: Email message topic
- Auto-created dead letter queues (DLQ) for failed messages
