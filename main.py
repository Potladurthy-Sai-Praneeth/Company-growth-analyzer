"""Main Pipeline Script for Message Ingestion and Event Processing."""

import os
import sys
import asyncio
import pandas as pd
import logging
import logging.handlers
from typing import Dict, Any, List, Optional
from datetime import datetime
from uuid import uuid4
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables
load_dotenv()

# Import services
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
    
    Handles:
    - CSV data reading
    - Kafka message production
    - Cache and database storage
    - LLM-based event extraction
    - Investor email generation
    """
    
    def __init__(self, config_path: str = "config.yaml"):
        """
        Initialize the message pipeline.
        
        Args:
            config_path: Path to configuration file.
        """
        self.config_path = config_path
        self.config = load_config(config_path)
        
        # Load application configuration
        app_config = self.config.get('application', {})
        self.cache_trigger_threshold = app_config.get('cache_trigger_threshold', 50)
        self.data_file_path = app_config.get('data_file_path', 'data/full_data.csv')
        self.company_id = app_config.get('company_id', 'scaleflow')
        
        # Initialize processors
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
        self.message_buffer: List[Dict[str, Any]] = []
        self.all_extracted_events: List[SignificantEvent] = []
        
        # Async processing
        self.llm_tasks: List[asyncio.Task] = []
        self._running = False
    
    async def initialize_services(self):
        """Initialize all required services."""
        try:
            # Initialize Kafka service
            self.kafka_service = KafkaQueueService(self.config_path)
            await self.kafka_service.initialize()
            
            # Initialize Cache service
            self.cache_service = MessageCacheService(self.config_path)
            
            # Initialize Message Database service
            self.db_service = MessageDatabaseService(self.config_path)
            await self.db_service.initialize()
            
            # Initialize Events Database service
            self.events_db = EventsDatabaseService(self.config_path)
            await self.events_db.initialize()
            
            # Initialize LLM service
            self.llm_service = LLMService()
            
            # Register message handlers for Kafka consumers
            self.kafka_service.register_message_handler('chat', self._handle_chat_message)
            self.kafka_service.register_message_handler('email', self._handle_email_message)
            
        except Exception as e:
            logger.error(f"Failed to initialize services: {e}")
            raise
    
    async def _handle_chat_message(self, message: Dict[str, Any]):
        """
        Handle incoming chat message from Kafka.
        Stores in cache and database asynchronously.
        """
        try:
            company_id = message.get('metadata', {}).get('company_id', self.company_id)
            
            # Add to cache (non-blocking)
            should_process, count = self.cache_service.add_message(
                message, company_id, 'chat'
            )
            
            # Store in database asynchronously
            asyncio.create_task(self.db_service.store_message(message))
            
            # Add to message buffer for LLM processing
            self.message_buffer.append(message)
            
            # Check if we should trigger LLM processing
            await self._check_and_trigger_llm(company_id)
            
        except Exception as e:
            logger.error(f"Error handling chat message: {e}", exc_info=True)
            self.status.errors.append(str(e))
    
    async def _handle_email_message(self, message: Dict[str, Any]):
        """
        Handle incoming email message from Kafka.
        Stores in cache and database asynchronously.
        """
        try:
            company_id = message.get('metadata', {}).get('company_id', self.company_id)
            
            # Add to cache (non-blocking)
            should_process, count = self.cache_service.add_message(
                message, company_id, 'email'
            )
            
            # Store in database asynchronously
            asyncio.create_task(self.db_service.store_message(message))
            
            # Add to message buffer for LLM processing
            self.message_buffer.append(message)
            
            # Check if we should trigger LLM processing
            await self._check_and_trigger_llm(company_id)
            
        except Exception as e:
            logger.error(f"Error handling email message: {e}", exc_info=True)
            self.status.errors.append(str(e))
    
    async def _check_and_trigger_llm(self, company_id: str):
        """
        Check if cache has reached threshold and trigger LLM processing.
        
        Args:
            company_id: Company identifier.
        """
        try:
            # Get total message count from both sources
            chat_count = self.cache_service.get_message_count(company_id, 'chat')
            email_count = self.cache_service.get_message_count(company_id, 'email')
            total_count = chat_count + email_count
            
            self.status.current_cache_size = total_count
            
            # Trigger LLM if threshold reached
            if total_count >= self.cache_trigger_threshold:
                # Create async task for LLM processing (non-blocking)
                task = asyncio.create_task(
                    self._process_messages_with_llm(company_id, chat_count, email_count)
                )
                self.llm_tasks.append(task)
                self.status.llm_triggers += 1
                
        except Exception as e:
            logger.error(f"Error checking/triggering LLM: {e}", exc_info=True)
    
    async def _process_messages_with_llm(
        self,
        company_id: str,
        chat_count: int,
        email_count: int
    ):
        """
        Process messages with LLM to extract significant events.
        
        Args:
            company_id: Company identifier.
            chat_count: Number of chat messages in cache.
            email_count: Number of email messages in cache.
        """
        try:
            # Get messages from cache (using 'slack' and 'gmail' as stored)
            chat_messages = self.cache_service.pop_messages(company_id, 'slack', chat_count)
            email_messages = self.cache_service.pop_messages(company_id, 'gmail', email_count)
            
            # Combine messages
            all_messages = chat_messages + email_messages
            
            if not all_messages:
                return
            
            # Create batch for tracking
            batch = MessageBatch(
                messages=all_messages,
                company_id=company_id,
                chat_count=len(chat_messages),
                email_count=len(email_messages)
            )
            
            # Extract events using LLM
            extraction_result = await self.llm_service.extract_events(
                all_messages,
                company_id
            )
            
            # Store events in PostgreSQL
            if extraction_result.events:
                stored_ids = await self.events_db.store_events_batch(
                    extraction_result.events,
                    company_id,
                    batch.batch_id
                )
                
                self.status.total_events_extracted += len(extraction_result.events)
                self.all_extracted_events.extend(extraction_result.events)
                
                # Milestone log: events extracted
                print(f"      → Extracted {len(extraction_result.events)} events from {len(all_messages)} messages")
            
            # Release processing locks
            self.cache_service.release_processing_lock(company_id, 'slack')
            self.cache_service.release_processing_lock(company_id, 'gmail')
            
        except Exception as e:
            logger.error(f"Error in LLM processing: {e}", exc_info=True)
            self.status.errors.append(str(e))
    
    async def process_csv_data(self, file_path: str = None):
        """
        Read CSV data and pump messages to Kafka queue.
        
        Args:
            file_path: Path to CSV file. If None, uses config value.
        """
        if file_path is None:
            file_path = self.data_file_path
        
        try:
            # Read CSV file
            df = pd.read_csv(file_path)
            total_rows = len(df)
            print(f"      Loaded {total_rows} rows from CSV")
            
            # Process each row
            for index, row in df.iterrows():
                try:
                    # Convert row to dict
                    row_dict = row.to_dict()
                    
                    # Determine if chat or email based on is_chat column
                    is_chat = str(row_dict.get('is_chat', 'true')).lower() == 'true'
                    
                    if is_chat:
                        # Process as Slack message
                        message = self.slack_processor.process(row_dict)
                        topic_type = 'chat'
                    else:
                        # Process as Gmail message
                        message = self.gmail_processor.process(row_dict)
                        topic_type = 'email'
                    
                    # Add company_id to metadata
                    message['metadata']['company_id'] = self.company_id
                    
                    # Send to Kafka
                    success = await self.kafka_service.produce_message(
                        message,
                        topic_type,
                        key=self.company_id
                    )
                    
                    if success:
                        self.status.total_rows_processed += 1
                        self.status.total_messages_ingested += 1
                        
                        # Store directly in cache and database (since we're not running consumers)
                        await self._store_message_directly(message, topic_type)
                    
                    # Log progress every 200 rows
                    if (index + 1) % 200 == 0:
                        print(
                            f"      Progress: {index + 1}/{total_rows} rows | "
                            f"{self.status.total_events_extracted} events extracted"
                        )
                    
                    # Small delay to avoid overwhelming the system
                    await asyncio.sleep(0.01)
                    
                except Exception as e:
                    logger.error(f"Error processing row {index}: {e}", exc_info=True)
                    self.status.errors.append(f"Row {index}: {str(e)}")
            
            print(f"      ✓ Finished processing {total_rows} rows")
            
        except Exception as e:
            logger.error(f"Error reading CSV file: {e}", exc_info=True)
            raise
    
    async def _store_message_directly(self, message: Dict[str, Any], topic_type: str):
        """
        Store message directly in cache and database.
        This is used when not running Kafka consumers.
        
        Args:
            message: Message dictionary.
            topic_type: 'chat' or 'email'.
        """
        try:
            company_id = message.get('metadata', {}).get('company_id', self.company_id)
            
            # Add to cache
            should_process, count = self.cache_service.add_message(
                message, company_id, 'slack' if topic_type == 'chat' else 'gmail'
            )
            
            # Store in database
            await self.db_service.store_message(message)
            
            # Add to message buffer
            self.message_buffer.append(message)
            
            # Get total count combining both sources
            chat_count = self.cache_service.get_message_count(company_id, 'slack')
            email_count = self.cache_service.get_message_count(company_id, 'gmail')
            total_count = chat_count + email_count
            
            self.status.current_cache_size = total_count
            
            # Check if we should trigger LLM processing 
            # This is analogous to a cron job checking the cache periodically
            if total_count >= self.cache_trigger_threshold:
                # Process synchronously to ensure completion before continuing
                await self._process_messages_with_llm(company_id, chat_count, email_count)
                
        except Exception as e:
            logger.error(f"Error storing message directly: {e}", exc_info=True)
    
    async def process_remaining_messages(self):
        """Process any remaining messages in the cache that didn't reach threshold."""
        try:
            # Get remaining messages from cache
            chat_count = self.cache_service.get_message_count(self.company_id, 'slack')
            email_count = self.cache_service.get_message_count(self.company_id, 'gmail')
            total_remaining = chat_count + email_count
            
            if total_remaining > 0:
                print(f"      Processing {total_remaining} remaining messages...")
                await self._process_messages_with_llm(self.company_id, chat_count, email_count)
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
        """
        Generate investor email from all extracted events.
        
        Returns:
            Tuple of (formatted output string, email object).
        """
        try:
            # Get all active events from database
            events = await self.events_db.get_active_events_for_email(self.company_id)
            
            if not events:
                return "No events to generate email from.", None
            
            print(f"      Generating email from {len(events)} events...")
            
            # Generate email using LLM
            email = await self.llm_service.generate_investor_email(
                events=events,
                startup_name="ScaleFlow",
                sender_name="Alex",
                sender_title="CEO"
            )
            
            # Format the output
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
    
    def _write_output_file(self, stats: Dict, email_output: str, email_obj) -> str:
        """Write results to markdown output file."""
        # Ensure output directory exists
        os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
        
        duration = (self.status.completed_at - self.status.started_at).total_seconds() if self.status.completed_at else 0
        
        content = f"""# Pipeline Results
**Generated at:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

---

## 📊 Pipeline Statistics

| Metric | Value |
|--------|-------|
| Total Rows Processed | {self.status.total_rows_processed} |
| Total Messages Ingested | {self.status.total_messages_ingested} |
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
        """Run the complete pipeline."""
        self._running = True
        self.status.started_at = datetime.now()
        self.status.status = "running"
        
        print("=" * 60)
        print("     STARTING MESSAGE PROCESSING PIPELINE")
        print("=" * 60)
        
        try:
            # Step 1: Initialize all services
            print("\n[1/5] Initializing services...")
            await self.initialize_services()
            print("      ✓ All services initialized")
            
            # Step 2: Process CSV data
            print("\n[2/5] Processing CSV data...")
            await self.process_csv_data()
            
            # Step 3: Process any remaining messages
            print("\n[3/5] Processing remaining messages...")
            await self.process_remaining_messages()
            
            # Step 4: Wait for all LLM tasks to complete
            await self.wait_for_llm_tasks()
            
            # Step 5: Generate statistics and email
            print("\n[4/5] Generating investor email...")
            
            # Get event statistics from database
            try:
                stats = await self.events_db.get_event_statistics(self.company_id)
            except Exception:
                stats = {}
            
            email_output, email_obj = await self.generate_investor_email()
            
            # Mark completion
            self.status.completed_at = datetime.now()
            self.status.status = "completed"
            
            # Write output file
            print("\n[5/5] Writing output file...")
            output_path = self._write_output_file(stats, email_output, email_obj)
            
            # Print summary to console
            duration = (self.status.completed_at - self.status.started_at).total_seconds()
            print("\n" + "=" * 60)
            print("     PIPELINE COMPLETED SUCCESSFULLY")
            print("=" * 60)
            print(f"""
📊 Summary:
   • Rows Processed: {self.status.total_rows_processed}
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
    
    async def _get_statistics(self):
        """Get pipeline statistics from database."""
        try:
            stats = await self.events_db.get_event_statistics(self.company_id)
        except Exception as stats_error:
            logger.warning(f"Could not fetch event statistics: {stats_error}")
            stats = {}
        
        return stats
    
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
    # Run the async main function
    asyncio.run(main())
