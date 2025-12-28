# 🚀 Sago Workflow

## 🎯 Use Case
An investor meets a founder but decides the startup is too early. They want to follow up in the future but do not know when. Ideally, Sago would notify them when there are meaningful signals to re-engage and generate a personalized outreach message.

## 🏗️ Workflow Principles
The current approach perfectly adheres to the Sago design principles
1. **🔗 Seamless Integration**: The system integrates with existing communication tools (Slack, Email) without requiring users to change their workflows.
2. **🎯 Hyper-Personalization**: By analyzing company's communication patterns and significant events/milestones, the system provides highly personalized notifications and outreach messages. 
3. **🤖 True Agency**: The system autonomously monitors communications, identifies significant events, and generates outreach messages, reducing manual effort.

### 📊 Data Strategy

**Intuition**

Slack group chats and the company's corresponding emails provide insights on company's whereabouts. The group chats simulate the team members' interactions ranging from milestones, achievements, challenges, and day-to-day operations. The emails provide a more formal communication channel that often includes important updates about the product launch, announcements, strategic discussions, and external communications with partners or clients. By combining insights from both Slack messages and Emails, the system can gain a comprehensive understanding of the company's activities and significant events.

**Data Simulation**

An LLM is used to simulate a real-time high growth fictional startup environment, that mimics the interactions across group members and the company's email inbox with the above-mentioned context for a breif period of 1 month. (The data represents a holistic view of the company ranging from product development, team discussions, market strategies, customer interactions, and overall growth trajectory.) 


**Sample Input Data**
| id | is_chat | chat_sender | email_sender | timestamp | content |
|---|---|---|---|---|---|
| 1 | True | Sarah (CTO) | | 2025-11-01 10:35:00 | AWS alert just hit my inbox. DB is redlining. James, hold off on the migration for a second. We might need to scale the instance vertically before we run the index fix. Maya, check if there's a leaked connection pool in the 'AnalyticsWorker' service. |
| 2 | True | Maya (Dev) | | 2025-11-01 10:42:15 | Found it. The AnalyticsWorker is opening a new connection for every batch instead of using the pool. It’s a regression from the v2.4 library update. I’m rolling back that specific service to v2.3 now. |
| 3 | True | Leo (Dev) | | 2025-11-01 11:00:10 | Service rolled back. Freeable memory is climbing back up. We're at 2.4GB free now. Crisis averted for the moment. James, you're clear to run the migration on staging. |
| 4 | False | | Stripe Analytics - Growth Report | 2025-11-01 12:00:00 | Subject: Weekly Revenue Analysis - Oct 25 to Nov 1. Total MRR: $192,400 (+12.5% WoW). New Subscriptions: 84. Top Plan: Enterprise (60% of new revenue). Churn Rate: 1.2% (Flat). Analysis: Your expansion revenue from existing customers grew by $8k this week, largely due to seat additions in the 'FinTech' segment. Warning: Your 'Starter' plan conversion rate has dropped by 5%, likely due to the recent changes in the onboarding UI. Action Recommended: Review the A/B test results for the 'One-Click Setup' feature. |
| 5 | False | | AWS Infrastructure Support | 2025-11-01 10:30:00 | Subject: [URGENT] High Memory Utilization Warning - RDS Instance. This is an automated alert from AWS CloudWatch. Your primary database instance (db-prod-01) has sustained memory utilization above 90% for the last 60 minutes. Current Freeable Memory: 420MB. This may lead to an Out of Memory (OOM) error and an automatic reboot of the instance. Suggested Action: Review active connections or consider scaling to an r6g.2xlarge instance to accommodate current workloads. |

## ⚙️ System Architecture
![Architecture Diagram](system_architecture.png)

The workflow is an **Event-Driven Data Pipeline** designed to analyse raw communication channels, filter them for importance using AI, and trigger alerts based on significant milestones. The system ingests data from Slack and Gmail using their respective APIs. Slack messages from group chats and emails from the company's inbox are fetched in real-time.

1. **🌊 Apache Kafka (Stream Processing)**: As the data obtained is a continuous stream, we need to have a robust queue system that is capable of handling extremely large streams of data. Apache Kafka is a distributed event streaming platform capable of handling high-throughput, real-time data feeds. Kafka is the ideal choice for this workflow due to the following reasons:
    - **Separation of concerns**: Separates multiple data sources gracefully through **topics** such that each service can be processed independently decoupling the data ingestion from processing.

    - **Data Consistency**: Kafka can handle the huge influx of messages and emails within short span of time efficiently without data loss.

    - **Asynchronous processing**: The chats and emails are captured and a separate consumer service processes them independently, ensuring non-blocking ingestion.

2. **🗄️ Apache Cassandra**: Primary database that stores raw messages. Even though Sago workflow connects directly to the company's API, we need a form of persistent storage for historical analysis and auditing. This database may seem redundant as the chats are anyway stored by the company database. However, it is advantageous for maintaining reliability and fault tolerance. Cassandra is considered for the following reasons: 
   - **High write throughput**: The chats & emails are time-series data that requires append only write operations with occasional reads (for auditing or re-processing). Cassandra's architecture is optimized for such workloads.

   - **Flexible Schema**: Cassandra's schema-less design allows for easy adaptation to changing data structures (not possible with Postgres), which is common in communication data.

3. ⚡**Redis Cache**: Sago workflow is asynchronous, meaning, the analysis is made periodically (through a cron job) to determine any significant events that are worthy of consolidating. Hence, we need an efficient caching layer to temporarily store incoming messages before processing. This use of cache eliminates the expensive database read operations and speeds up the processing. 
Redis has the following advantages that make it suitable for this workflow:
   - **In-Memory Storage**: Redis stores data in memory, allowing for extremely fast read and write operations. This is crucial for real-time processing of incoming messages.

   The disadvantage of incurring an additional cost to maintain this service is outweighed by the fact that the cache is periodically flushed keeping the memory footprint low and cost manageable.

   ```
   The workflow simulates a cron job by implementing a mechanism based on the number of cached messages. Once the number of cached messages reaches a predefined threshold (200 messages), the system triggers the LLM to process the batch of messages. This batching strategy optimizes LLM usage by reducing the number of API calls, leading to cost savings and improved throughput.
   ```

4. **🗃️ Events Database (PostgreSQL)**: A separate database (Postgres/ MySQL) is used to store the extracted significant events from the LLM processing. This allows for structured querying and reporting of key milestones. This database seems optional but it is a better choice to have a separate database to store the extracted events rather than storing it in the raw messages database. This allows for better organization and retrieval of significant events without cluttering the raw data.

5. **🧠 LLM Service**: The core of the Sago workflow is the LLM service that performs two main functions:
   - **Event Extraction**: The LLM analyses the cached messages to identify significant events such as product launches, funding rounds, user growth milestones, and technical achievements. It uses a carefully crafted prompt to extract structured information about these events persisted in the Events Database.

   - **Notification Generation**: A cron job triggers the LLM to generate a personalized outreach notification (email in this case) reading all the significant events from the Events Database highlighting the company's recent achievements and growth trajectory. 

      ```
      The workflow simulates a notification generation cron job with the completion of data processing. Real time use cases include cron jobs scheduled to run periodically (eg: once in a month) to generate the outreach notification based on the latest significant events polled from the events database.
      ```

## Output
The following is the generated  outreach email by the Sago workflow analysing the company's Slack and Email data.


### Subject
**ScaleFlow: $5.4M ARR, 141% NRR, and Major Enterprise Wins**

### Body
Hi [Investor Name],

I’m writing to share some recent milestones at ScaleFlow. We have just crossed **$450k MRR ($5.4M ARR)**, driven by a breakout month where we saw Net Revenue Retention (NRR) hit **141%**.

Following our #1 launch on ProductHunt and a successful technical deep dive with Sequoia, we have secured a Series A term sheet. We are now moving quickly to close the round and wanted to connect regarding the remaining allocation.

**Recent Commercial Traction:**
*   **Enterprise Wins:** We have confirmed contracts moving forward with **Delta Airlines** and **Goldman Sachs** (for our API v2), alongside wins with FedEx and Shopify.
*   **Pipeline Explosion:** Our enterprise pipeline has grown 400%, with active late-stage discussions with JPMC, Airbnb, and Uber.
*   **Retention:** Our churn remains exceptionally low (~1.0 - 1.7%), validating strong product-market fit in the enterprise segment.

**Technical Validation:**
*   **Scale:** Our infrastructure is now successfully handling peaks of **~25,000 concurrent WebSockets** with real-time latency reduced to 85ms.
*   **Moat:** Our proprietary Causal Consistency engine has been technically audited and validated as a key differentiator against incumbents.

We are capitalizing on this momentum to expand our engineering team and close the integration gaps (Snowflake/Databricks) requested by our Fortune 500 prospects.

Are you available for a brief call this Tuesday or Thursday to discuss the round?

Best,

Alex
CEO, ScaleFlow

### Key Highlights
1. $450k MRR / $5.4M ARR milestone
2. 141% Net Revenue Retention (NRR)
3. Major contracts: Delta Airlines, Goldman Sachs, FedEx
4. Sequoia Term Sheet secured
5. Technical scale: 25k concurrent WebSockets

### Metrics Summary
- **Peak MRR**: $450,000
- **Implied ARR**: $5.4M
- **Peak NRR**: 141%
- **Peak DAU**: 80,000
- **Concurrent WebSockets**: 25,000+