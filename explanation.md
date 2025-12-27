# Sago Workflow

## Use Case
An investor meets a founder but decides the startup is too early. They want to follow up in the future but do not know when. Ideally, Sago would notify them when there are meaningful signals to re-engage and generate a personalized outreach message.

## Workflow Consideration
The current approach perfectly adheres to the Sago design principles
1. **Seamless Integration**: The system integrates with existing communication tools (Slack, Email) without requiring users to change their workflows.
2. **Hyper-Personalization**: By analyzing company's communication patterns and significant events/milestones, the system provides highly personalized notifications and outreach messages. 
3. **True Agency**: The system autonomously monitors communications, identifies significant events, and generates outreach messages, reducing the manual effort.

### Intuition
To analyse the company's whereabouts, the data considered is the Slack group chats and the company's corresponding emails. The group chats simulate the team members' interactions ranging from milestones, achievements, challenges, and day-to-day operations. The emails provide a more formal communication channel that often includes important updates about the product launch, announcements, strategic discussions, and external communications with partners or clients. By combining insights from both Slack messages and Emails, the system can gain a comprehensive understanding of the company's activities and significant events.

### Data Consideration
To simulate a real-time high growth startup environment, I have simulated data (using LLM) for a fictional startup that mimics the interactions across group members and the company's email inbox with the above-mentioned context for a breif period of 1 month. (The data represents a holistic view of the company ranging from product development, team dynamics, market strategies, customer interactions, and overall growth trajectory.) 


## System Architecture
![Architecture Diagram](system_architecture.png)

This system is an **Event-Driven Data Pipeline** designed to analyse raw communication channels, filter them for importance using AI, and trigger alerts based on significant milestones.

### Data Ingestion: 
The system ingests data from Slack and Gmail using their respective APIs. Slack messages from group chats and emails from the company's inbox are fetched in real-time.

1. **Kafka**: As the data obtained is a continuous stream of data, we need to have a robust queue system that is capable of handling extremely large streams of data. Apache Kafka is a distributed event streaming platform capable of handling high-throughput, real-time data feeds. Kafka is the ideal choice for this workflow due to the following reasons:
    - Separates multiple data sources gracefully through **topics** such that each service can be processed independently decoupling the data ingestion from processing.
    - Real time processing capabilities to handle streaming data, as we can have huge influx of messages and emails within short span of time, message queues like Kafka can handle this influx efficiently without data loss.
    - Asynchronous processing: The chats and emails are captured and a separate consumer service processes them independently, ensuring non-blocking ingestion.

2. **Cassandra**: This acts as the persistent storage layer for the ingested raw messages. Even though Sago workflow connects directly to the company's API, we need a form of persistent storage for historical analysis and auditing (this may be redundant as it incurs additonal storage cost but provides reliability and fault tolerance). Since the incoming data is time-series in nature (messages with timestamps), Apache Cassandra is a suitable choice due to its high write throughput. Since the chats & emails are append only write operations with occasional reads (for auditing or re-processing), Cassandra's architecture is optimized for such workloads.

**Note**: A PostgreSQL or MySQL database can also be used here instead of Cassandra for simplicity, but with ever changing dynamics of the data from the APIs, Cassandra provides more flexibility and scalability.

3. **Redis Cache**: Sago workflow is asynchronus meaning the analysis is made periodically (through a cron job) to determine any significant events that are worthy of consolidating, we need an efficient caching layer to temporarily store incoming messages before processing. This use of cache eleiminates the expensive database read operations and speeds up the processing. (Even though we use PostGresql (read optimized) to store data, reading incoming data periodically form a large databse of mesages adds a signifacnt ovehead and is not efficient). 
Redis has the following advantages that make it suitable for this workflow:
   - **In-Memory Storage**: Redis stores data in memory, allowing for extremely fast read and write operations. This is crucial for real-time processing of incoming messages.

One might notice the dis-advantage of incuring an additonal cost to maintain this service, since we flush the cache periodically after processing, we don't store large amounts of data in Redis, keeping the memory footprint low and cost manageable.

```
To simulate a cron job that run periodically, I have implemented a mechanism where once the number of cached messages reaches a predefined threshold (200 messages), the system triggers the LLM to process the batch of messages. This batching strategy optimizes LLM usage by reducing the number of API calls, leading to cost savings and improved throughput.
```

4. **Events Database**: A separate databse (Postgres/ MySQL) is used to store the extracted significant events from the LLM processing. This allows for structured querying and reporting of key milestones. This database seems optional but it is a better choice to have a separate database to store the extracted events rather than storing it in the raw messages database. This allows for better organization and retrieval of significant events without cluttering the raw data.

5 . **LLM Service**: The core of the Sago workflow is the LLM service that performs two main functions:
   - **Event Extraction**: The LLM analyses the cached messages to identify significant events such as product launches, funding rounds, user growth milestones, and technical achievements. It uses a carefully crafted prompt to extract structured information about these events.
   - **Notification Generation**: A cron job triggers the LLM to generate a personalized outreach email to investors, highlighting the company's recent achievements and growth trajectory. 

```
In this particular case, I have not used a cron job rather simulated the Notification generation step immediately after entire data is processed and stored. In real time scenario, this step can be scheduled to run periodically (eg: once in a month) to generate the outreach email based on the latest significant events polling the events database.
```

## Output
The following is the generated  outreach email by the Sago workflow analysing the company's Slack and Email data.

### Subject
**ScaleFlow: $5.4M ARR, 143% NRR, and Series A Term Sheet**

### Body
Hi [Investor Name],

I’m reaching out because ScaleFlow’s growth velocity has accelerated significantly over the last quarter, and we have just secured a **Series A term sheet ($15M @ $80M valuation)**.

We are currently finalizing the round and are looking to bring on a select group of strategic partners who understand the enterprise infrastructure space.

**Key Traction Highlights:**

*   **Revenue Growth:** We recently hit a peak of **$450k MRR** (~$5.4M ARR), driven by our new Causal Engine and Real-time Collaboration modules.
*   **Best-in-Class Retention:** Our Net Revenue Retention (NRR) reached a record **143%** in October, demonstrating massive expansion revenue within our existing cohorts.
*   **Enterprise Validation:** We have secured commitments from **Delta Airlines, FedEx, and Walmart**, with active late-stage pilots at Goldman Sachs, Uber, and Airbnb.
*   **Technical Moat:** Our proprietary 'Causal Consistency' engine is now handling over **25,000 concurrent WebSockets** at scale. To support this, we are onboarding senior engineering talent, including alumni from the Google Spanner team.

We have proven product-market fit with Fortune 500 clients and the unit economics to scale efficiently. 

I’d love to find 15 minutes this week to walk you through the deck and discuss the remaining allocation in the round.

Best,

**Alex**
CEO, ScaleFlow