"""Main Pipeline Script for Message Ingestion and Event Processing.

Architecture:
    CSV Data → Kafka Producer → Kafka Topics
    Kafka Consumers (async) → Cache + Database
    Cache Threshold → LLM Event Extraction
    Events → PostgreSQL → Investor Email Generation
"""

import os
import asyncio
import pandas as pd
from typing import Dict, Any, List, Optional
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

from kafka_service import KafkaQueueService
from cache_service import MessageCacheService
from database_service import MessageDatabaseService
from events_database import EventsDatabaseService
from llm_service import LLMService
from process_message import SlackProcessor, GmailProcessor
from models import ProcessingStatus, SignificantEvent, MessageBatch
from utils import load_config, setup_logging

logger = setup_logging(__name__, "main")

OUTPUT_FILE = "output/pipeline_results.md"


class MessagePipeline:
    """
    Main pipeline for message processing and event extraction.
    
    Uses async Kafka consumers to automatically process messages
    as they arrive in the queue.
    """
    
    def __init__(self, config_path: str = "config.yaml"):
        """Initialize the message pipeline."""
        self.config_path = config_path
        self.config = load_config(config_path)
        
        app_config = self.config.get('application', {})
        self.cache_trigger_threshold = app_config.get('cache_trigger_threshold', 50)
        self.data_file_path = app_config.get('data_file_path', 'data/full_data.csv')
        self.company_id = app_config.get('company_id', 'scaleflow')
        
        # Message processors
        self.slack_processor = SlackProcessor()
        self.gmail_processor = GmailProcessor()
        
        # Services (initialized asynchronously)
        self.kafka_service: Optional[KafkaQueueService] = None
        self.cache_service: Optional[MessageCacheService] = None
        self.db_service: Optional[MessageDatabaseService] = None
        self.events_db: Optional[EventsDatabaseService] = None
        self.llm_service: Optional[LLMService] = None
        
        # Processing state
        self.status = ProcessingStatus()
        self.all_extracted_events: List[SignificantEvent] = []
        self.llm_tasks: List[asyncio.Task] = []
        self._running = False
        self._consumer_ready = asyncio.Event()
        
        # Lock to prevent multiple concurrent LLM triggers
        self._llm_trigger_lock = asyncio.Lock()
    
    async def initialize_services(self):
        """Initialize all required services."""
        try:
            self.kafka_service = KafkaQueueService(self.config_path)
            await self.kafka_service.initialize()
            
            self.cache_service = MessageCacheService(self.config_path)
            
            self.db_service = MessageDatabaseService(self.config_path)
            await self.db_service.initialize()
            
            self.events_db = EventsDatabaseService(self.config_path)
            await self.events_db.initialize()
            
            self.llm_service = LLMService()
            
            # Register handlers for Kafka consumers
            self.kafka_service.register_message_handler('chat', self._handle_chat_message)
            self.kafka_service.register_message_handler('email', self._handle_email_message)
            
            logger.info("All services initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize services: {e}")
            raise
    
    async def _handle_chat_message(self, message: Dict[str, Any]):
        """Handle incoming chat message from Kafka consumer."""
        try:
            company_id = message.get('metadata', {}).get('company_id', self.company_id)
            
            self.cache_service.add_message(message, company_id, 'slack')
            asyncio.create_task(self.db_service.store_message(message))
            self.status.total_messages_ingested += 1
            
            await self._check_and_trigger_llm(company_id)
            
        except Exception as e:
            logger.error(f"Error handling chat message: {e}", exc_info=True)
            self.status.errors.append(str(e))
    
    async def _handle_email_message(self, message: Dict[str, Any]):
        """Handle incoming email message from Kafka consumer."""
        try:
            company_id = message.get('metadata', {}).get('company_id', self.company_id)
            
            self.cache_service.add_message(message, company_id, 'gmail')
            asyncio.create_task(self.db_service.store_message(message))
            self.status.total_messages_ingested += 1
            
            await self._check_and_trigger_llm(company_id)
            
        except Exception as e:
            logger.error(f"Error handling email message: {e}", exc_info=True)
            self.status.errors.append(str(e))
    
    async def _check_and_trigger_llm(self, company_id: str):
        """Check if cache has reached threshold and trigger LLM processing."""
        try:
            # Use lock to prevent race condition where multiple handlers
            # see cache >= threshold and all trigger LLM simultaneously
            async with self._llm_trigger_lock:
                chat_count = self.cache_service.get_message_count(company_id, 'slack')
                email_count = self.cache_service.get_message_count(company_id, 'gmail')
                total_count = chat_count + email_count
                
                self.status.current_cache_size = total_count
                
                if total_count >= self.cache_trigger_threshold:
                    self.status.llm_triggers += 1
                    
                    # Pop messages immediately while holding the lock
                    # This ensures the next check sees a reduced count
                    chat_messages = self.cache_service.pop_messages(company_id, 'slack', chat_count)
                    email_messages = self.cache_service.pop_messages(company_id, 'gmail', email_count)
                    
                    # Create task for async LLM processing (outside lock release)
                    task = asyncio.create_task(
                        self._process_popped_messages_with_llm(
                            company_id, chat_messages, email_messages
                        )
                    )
                    self.llm_tasks.append(task)
                
        except Exception as e:
            logger.error(f"Error checking/triggering LLM: {e}", exc_info=True)
    
    async def _process_popped_messages_with_llm(
        self,
        company_id: str,
        chat_messages: list,
        email_messages: list
    ):
        """Process already-popped messages with LLM to extract significant events."""
        try:
            all_messages = chat_messages + email_messages
            
            if not all_messages:
                return
            
            batch = MessageBatch(
                messages=all_messages,
                company_id=company_id,
                chat_count=len(chat_messages),
                email_count=len(email_messages)
            )
            
            extraction_result = await self.llm_service.extract_events(all_messages, company_id)
            
            if extraction_result.events:
                await self.events_db.store_events_batch(
                    extraction_result.events,
                    company_id,
                    batch.batch_id
                )
                
                self.status.total_events_extracted += len(extraction_result.events)
                self.all_extracted_events.extend(extraction_result.events)
                
                print(f"      → Extracted {len(extraction_result.events)} events from {len(all_messages)} messages")
            
            self.cache_service.release_processing_lock(company_id, 'slack')
            self.cache_service.release_processing_lock(company_id, 'gmail')
            
        except Exception as e:
            logger.error(f"Error in LLM processing: {e}", exc_info=True)
            self.status.errors.append(str(e))
    
    async def start_consumers(self):
        """Start Kafka consumers to automatically process incoming messages."""
        await self.kafka_service.start_consuming(['chat', 'email'])
        self._consumer_ready.set()
        logger.info("Kafka consumers started")
    
    async def process_csv_data(self, file_path: str = None):
        """Read CSV data and pump messages to Kafka queue."""
        if file_path is None:
            file_path = self.data_file_path
        
        try:
            await self._consumer_ready.wait()
            
            df = pd.read_csv(file_path)
            total_rows = len(df)
            print(f"      Loaded {total_rows} rows from CSV")
            
            for index, row in df.iterrows():
                try:
                    row_dict = row.to_dict()
                    is_chat = str(row_dict.get('is_chat', 'true')).lower() == 'true'
                    
                    if is_chat:
                        message = self.slack_processor.process(row_dict)
                        topic_type = 'chat'
                    else:
                        message = self.gmail_processor.process(row_dict)
                        topic_type = 'email'
                    
                    message['metadata']['company_id'] = self.company_id
                    
                    success = await self.kafka_service.produce_message(
                        message,
                        topic_type,
                        key=self.company_id
                    )
                    
                    if success:
                        self.status.total_rows_processed += 1
                    
                    if (index + 1) % 200 == 0:
                        print(
                            f"      Progress: {index + 1}/{total_rows} rows | "
                            f"{self.status.total_events_extracted} events extracted"
                        )
                    
                    await asyncio.sleep(0.01)
                    
                except Exception as e:
                    logger.error(f"Error processing row {index}: {e}", exc_info=True)
                    self.status.errors.append(f"Row {index}: {str(e)}")
            
            print(f"      ✓ Finished producing {total_rows} rows to Kafka")
            
        except Exception as e:
            logger.error(f"Error reading CSV file: {e}", exc_info=True)
            raise
    
    async def wait_for_consumers(self, timeout: float = 30.0):
        """Wait for consumers to finish processing all messages."""
        print("      Waiting for consumers to process remaining messages...")
        
        await asyncio.sleep(2)
        
        last_count = -1
        stable_iterations = 0
        
        while stable_iterations < 3 and timeout > 0:
            current_count = self.status.total_messages_ingested
            if current_count == last_count:
                stable_iterations += 1
            else:
                stable_iterations = 0
            last_count = current_count
            await asyncio.sleep(1)
            timeout -= 1
        
        print(f"      ✓ Consumers processed {self.status.total_messages_ingested} messages")
    
    async def process_remaining_messages(self):
        """Process any remaining messages in the cache that didn't reach threshold."""
        try:
            chat_count = self.cache_service.get_message_count(self.company_id, 'slack')
            email_count = self.cache_service.get_message_count(self.company_id, 'gmail')
            total_remaining = chat_count + email_count
            
            if total_remaining > 0:
                print(f"      Processing {total_remaining} remaining messages...")
                self.status.llm_triggers += 1
                chat_messages = self.cache_service.pop_messages(self.company_id, 'slack', chat_count)
                email_messages = self.cache_service.pop_messages(self.company_id, 'gmail', email_count)
                await self._process_popped_messages_with_llm(self.company_id, chat_messages, email_messages)
            else:
                print("      No remaining messages to process")
                
        except Exception as e:
            logger.error(f"Error processing remaining messages: {e}", exc_info=True)
    
    async def wait_for_llm_tasks(self):
        """Wait for all pending LLM tasks to complete."""
        if self.llm_tasks:
            await asyncio.gather(*self.llm_tasks, return_exceptions=True)
            self.llm_tasks.clear()
    
    async def generate_investor_email(self):
        """Generate investor email from all extracted events."""
        try:
            events = await self.events_db.get_active_events_for_email(self.company_id)
            
            if not events:
                return "No events to generate email from.", None
            
            print(f"      Generating email from {len(events)} events...")
            
            email = await self.llm_service.generate_investor_email(
                events=events,
                startup_name="ScaleFlow",
                sender_name="Alex",
                sender_title="CEO"
            )
            
            output = f"""
{'='*80}
GENERATED INVESTOR EMAIL
{'='*80}

📧 SUBJECT: {email.subject}

{'─'*80}
EMAIL BODY:
{'─'*80}

{email.body}

{'─'*80}
📊 KEY HIGHLIGHTS:
{'─'*80}
"""
            for i, highlight in enumerate(email.highlights, 1):
                output += f"  {i}. {highlight}\n"
            
            output += f"""
{'─'*80}
📈 METRICS SUMMARY:
{'─'*80}
"""
            for metric, value in email.metrics_summary.items():
                output += f"  • {metric}: {value}\n"
            
            output += f"""
{'='*80}
Generated at: {email.generated_at}
Total events analyzed: {len(events)}
{'='*80}
"""
            
            return output, email
            
        except Exception as e:
            logger.error(f"Error generating investor email: {e}", exc_info=True)
            raise
    
    def _write_output_file(self, stats: Dict, email_obj) -> str:
        """Write results to markdown output file."""
        os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
        
        duration = (self.status.completed_at - self.status.started_at).total_seconds() if self.status.completed_at else 0
        
        content = f"""# Pipeline Results
**Generated at:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

---

## 📊 Pipeline Statistics

| Metric | Value |
|--------|-------|
| Total Rows Processed | {self.status.total_rows_processed} |
| Total Messages Consumed | {self.status.total_messages_ingested} |
| Total Events Extracted | {self.status.total_events_extracted} |
| LLM Triggers | {self.status.llm_triggers} |
| Processing Duration | {duration:.2f} seconds |
| Errors | {len(self.status.errors)} |

### Events by Severity
| Severity | Count |
|----------|-------|
| High | {stats.get('severity_distribution', {}).get('high', 0)} |
| Medium | {stats.get('severity_distribution', {}).get('medium', 0)} |
| Low | {stats.get('severity_distribution', {}).get('low', 0)} |

### Events by Type
"""
        if stats.get('type_distribution'):
            for event_type, count in stats['type_distribution'].items():
                content += f"- **{event_type}**: {count}\n"
        
        content += f"""
---

## 📧 Generated Investor Email

### Subject
**{email_obj.subject if email_obj else 'N/A'}**

### Body
{email_obj.body if email_obj else 'No email generated'}

### Key Highlights
"""
        if email_obj and email_obj.highlights:
            for i, highlight in enumerate(email_obj.highlights, 1):
                content += f"{i}. {highlight}\n"
        
        content += """
### Metrics Summary
"""
        if email_obj and email_obj.metrics_summary:
            for metric, value in email_obj.metrics_summary.items():
                content += f"- **{metric}**: {value}\n"
        
        content += f"""
---

*Pipeline completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*
"""
        
        with open(OUTPUT_FILE, 'w') as f:
            f.write(content)
        
        return OUTPUT_FILE
    
    async def run(self):
        """Run the complete pipeline with async consumer pattern."""
        self._running = True
        self.status.started_at = datetime.now()
        self.status.status = "running"
        
        print("=" * 60)
        print("     STARTING MESSAGE PROCESSING PIPELINE")
        print("=" * 60)
        
        try:
            print("\n[1/6] Initializing services...")
            await self.initialize_services()
            print("      ✓ All services initialized")
            
            print("\n[2/6] Starting Kafka consumers...")
            asyncio.create_task(self.start_consumers())
            await asyncio.sleep(1)
            print("      ✓ Consumers started and listening")
            
            print("\n[3/6] Processing CSV data...")
            await self.process_csv_data()
            
            print("\n[4/6] Waiting for consumers to complete...")
            await self.wait_for_consumers()
            
            print("\n[5/6] Processing remaining messages...")
            await self.process_remaining_messages()
            await self.wait_for_llm_tasks()
            
            print("\n[6/6] Generating investor email...")
            
            try:
                stats = await self.events_db.get_event_statistics(self.company_id)
            except Exception:
                stats = {}
            
            email_output, email_obj = await self.generate_investor_email()
            
            self.status.completed_at = datetime.now()
            self.status.status = "completed"
            
            output_path = self._write_output_file(stats, email_obj)
            
            duration = (self.status.completed_at - self.status.started_at).total_seconds()
            print("\n" + "=" * 60)
            print("     PIPELINE COMPLETED SUCCESSFULLY")
            print("=" * 60)
            print(f"""
📊 Summary:
   • Rows Processed: {self.status.total_rows_processed}
   • Messages Consumed: {self.status.total_messages_ingested}
   • Events Extracted: {self.status.total_events_extracted}
   • LLM Triggers: {self.status.llm_triggers}
   • Duration: {duration:.2f} seconds
   
📄 Output file: {output_path}
""")
            
        except Exception as e:
            self.status.status = "failed"
            self.status.errors.append(str(e))
            logger.error(f"Pipeline failed: {e}")
            raise
        finally:
            await self.cleanup()
    
    async def cleanup(self):
        """Cleanup resources."""
        try:
            if self.kafka_service:
                await self.kafka_service.close()
            if self.cache_service:
                self.cache_service.close()
            if self.db_service:
                await self.db_service.close()
            if self.events_db:
                await self.events_db.close()
        except Exception as e:
            logger.error(f"Error during cleanup: {e}", exc_info=True)


async def main():
    """Main entry point."""
    pipeline = MessagePipeline()
    await pipeline.run()


if __name__ == "__main__":
    asyncio.run(main())
