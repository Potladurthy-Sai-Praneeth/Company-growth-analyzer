"""Kafka Queue Service for handling Slack and Gmail messages."""

import json
import logging
from typing import Dict, Any, Optional, Callable, List
from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from kafka.errors import KafkaError, KafkaTimeoutError
from kafka.admin import KafkaAdminClient, NewTopic
import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import time
from utils import load_config, validate_message_format, extract_message_metadata, setup_logging

logger = setup_logging(__name__, "kafka_service")


class KafkaQueueService:
    """Kafka Queue Service for managing message queues."""
    
    def __init__(self, config_path: str = None):
        """
        Initialize Kafka Queue Service.
        
        Args:
            config_path: Path to configuration file.
        """
        self.config = load_config(config_path)
        self.kafka_config = self.config['kafka']
        
        self.producer: Optional[KafkaProducer] = None
        self.consumers: Dict[str, KafkaConsumer] = {}
        self.admin_client: Optional[KafkaAdminClient] = None
        
        self.executor = ThreadPoolExecutor(max_workers=10)
        self._initialized = False
        self._running = False
        self._processing_tasks: List[asyncio.Task] = []
        
        # Message handlers for each topic
        self._message_handlers: Dict[str, Callable] = {}
        
        # Metrics
        self.metrics = {
            'messages_produced': 0,
            'messages_consumed': 0,
            'messages_failed': 0,
            'last_heartbeat': None
        }
    
    async def initialize(self):
        """Initialize Kafka connections and create topics."""
        if self._initialized:
            logger.warning("KafkaQueueService already initialized")
            return
        
        try:
            await self._create_admin_client()
            await self._ensure_topics_exist()
            await self._create_producer()
            self._initialized = True
            logger.info("KafkaQueueService initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize KafkaQueueService: {e}")
            raise
    
    async def _create_admin_client(self):
        """Create Kafka admin client for topic management."""
        try:
            def _create():
                return KafkaAdminClient(
                    bootstrap_servers=self.kafka_config['bootstrap_servers'],
                    client_id='message-queue-admin'
                )
            
            self.admin_client = await asyncio.get_event_loop().run_in_executor(
                self.executor, _create
            )
            logger.info("Kafka admin client created")
        except Exception as e:
            logger.error(f"Failed to create admin client: {e}")
            raise
    
    async def _ensure_topics_exist(self):
        """Ensure required topics exist, create if they don't."""
        try:
            topics_config = self.kafka_config['topics']
            required_topics = [
                topics_config['chat'],
                topics_config['email'],
                f"{topics_config['chat']}-dlq",  # Dead letter queue
                f"{topics_config['email']}-dlq"
            ]
            
            def _check_and_create():
                existing_topics = self.admin_client.list_topics()
                topics_to_create = []
                
                for topic_name in required_topics:
                    if topic_name not in existing_topics:
                        topics_to_create.append(
                            NewTopic(
                                name=topic_name,
                                num_partitions=3,
                                replication_factor=1
                            )
                        )
                
                if topics_to_create:
                    self.admin_client.create_topics(
                        new_topics=topics_to_create,
                        validate_only=False
                    )
                    logger.info(f"Created topics: {[t.name for t in topics_to_create]}")
                else:
                    logger.info("All required topics already exist")
            
            await asyncio.get_event_loop().run_in_executor(
                self.executor, _check_and_create
            )
        except Exception as e:
            logger.error(f"Failed to ensure topics exist: {e}")
            raise
    
    async def _create_producer(self):
        """Create Kafka producer with production-ready settings."""
        try:
            producer_config = self.kafka_config['producer']
            
            def _create():
                return KafkaProducer(
                    bootstrap_servers=self.kafka_config['bootstrap_servers'],
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    acks=producer_config.get('acks', 'all'),
                    retries=producer_config.get('retries', 3),
                    max_in_flight_requests_per_connection=producer_config.get(
                        'max_in_flight_requests_per_connection', 5
                    ),
                    compression_type=producer_config.get('compression_type', 'snappy'),
                    batch_size=producer_config.get('batch_size', 16384),
                    linger_ms=producer_config.get('linger_ms', 10),
                    buffer_memory=producer_config.get('buffer_memory', 33554432),
                    api_version=(2, 5, 0)
                )
            
            self.producer = await asyncio.get_event_loop().run_in_executor(
                self.executor, _create
            )
            logger.info("Kafka producer created")
        except Exception as e:
            logger.error(f"Failed to create producer: {e}")
            raise
    
    async def produce_message(
        self,
        message: Dict[str, Any],
        topic_type: str,
        key: Optional[str] = None
    ) -> bool:
        """
        Produce a message to a specific topic.
        
        Args:
            message: Message dictionary to send.
            topic_type: Type of topic ('chat' or 'email').
            key: Optional message key for partitioning.
            
        Returns:
            True if message was sent successfully, False otherwise.
            
        Raises:
            ValueError: If topic_type is invalid.
            RuntimeError: If service is not initialized.
        """
        if not self._initialized:
            raise RuntimeError("KafkaQueueService not initialized. Call initialize() first.")
        
        if topic_type not in ['chat', 'email']:
            raise ValueError(f"Invalid topic_type: {topic_type}. Must be 'chat' or 'email'")
        
        # Validate message format
        if not validate_message_format(message, topic_type):
            logger.error(f"Invalid message format for topic {topic_type}")
            return False
        
        try:
            topic_name = self.kafka_config['topics'][topic_type]
            
            # Add timestamp if not present
            if 'produced_at' not in message:
                message['produced_at'] = datetime.now().isoformat()
            
            # Prepare key
            message_key = message['metadata']['company_id']
            if key:
                message_key = key.encode('utf-8')
            
            def _send():
                future = self.producer.send(
                    topic_name,
                    value=message,
                    key=message_key
                )
                return future.get(timeout=10)
            
            record_metadata = await asyncio.get_event_loop().run_in_executor(
                self.executor, _send
            )
            
            self.metrics['messages_produced'] += 1
            logger.debug(
                f"Message sent to topic '{topic_name}' "
                f"(partition: {record_metadata.partition}, "
                f"offset: {record_metadata.offset})"
            )
            return True
            
        except KafkaTimeoutError as e:
            logger.error(f"Timeout sending message to {topic_type}: {e}")
            self.metrics['messages_failed'] += 1
            return False
        except KafkaError as e:
            logger.error(f"Kafka error sending message to {topic_type}: {e}")
            self.metrics['messages_failed'] += 1
            return False
        except Exception as e:
            logger.error(f"Unexpected error sending message to {topic_type}: {e}")
            self.metrics['messages_failed'] += 1
            return False
    
    def register_message_handler(
        self,
        topic_type: str,
        handler: Callable[[Dict[str, Any]], asyncio.Future]
    ):
        """
        Register an async handler function for processing messages from a topic.
        
        Args:
            topic_type: Type of topic ('chat' or 'email').
            handler: Async function that processes messages.
        """
        if topic_type not in ['chat', 'email']:
            raise ValueError(f"Invalid topic_type: {topic_type}")
        
        self._message_handlers[topic_type] = handler
        logger.info(f"Registered handler for topic '{topic_type}'")
    
    async def start_consuming(self, topic_types: List[str] = None):
        """
        Start consuming messages from specified topics.
        
        Args:
            topic_types: List of topic types to consume from. If None, consumes from all.
        """
        if not self._initialized:
            raise RuntimeError("KafkaQueueService not initialized. Call initialize() first.")
        
        if topic_types is None:
            topic_types = ['chat', 'email']
        
        self._running = True
        
        # Start consumer for each topic type
        for topic_type in topic_types:
            if topic_type not in self._message_handlers:
                logger.warning(f"No handler registered for topic '{topic_type}', skipping")
                continue
            
            task = asyncio.create_task(
                self._consume_topic(topic_type)
            )
            self._processing_tasks.append(task)
        
        logger.info(f"Started consuming from topics: {topic_types}")
    
    async def _consume_topic(self, topic_type: str):
        """
        Consume messages from a specific topic.
        
        Args:
            topic_type: Type of topic to consume from.
        """
        topic_name = self.kafka_config['topics'][topic_type]
        consumer_config = self.kafka_config.get('consumer', {})
        processing_config = self.config.get('processing', {})
        
        try:
            # Create consumer
            def _create_consumer():
                return KafkaConsumer(
                    topic_name,
                    bootstrap_servers=self.kafka_config['bootstrap_servers'],
                    group_id=self.kafka_config['consumer_group'],
                    auto_offset_reset=self.kafka_config.get('auto_offset_reset', 'earliest'),
                    enable_auto_commit=self.kafka_config.get('enable_auto_commit', False),
                    max_poll_records=self.kafka_config.get('max_poll_records', 100),
                    session_timeout_ms=self.kafka_config.get('session_timeout_ms', 30000),
                    heartbeat_interval_ms=self.kafka_config.get('heartbeat_interval_ms', 10000),
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    api_version=(2, 5, 0)
                )
            
            consumer = await asyncio.get_event_loop().run_in_executor(
                self.executor, _create_consumer
            )
            
            self.consumers[topic_type] = consumer
            logger.info(f"Consumer created for topic '{topic_name}'")
            
            # Get handler
            handler = self._message_handlers[topic_type]
            max_retries = processing_config.get('max_retries', 3)
            retry_delay = processing_config.get('retry_delay', 5)
            
            # Consume messages
            while self._running:
                def _poll():
                    return consumer.poll(timeout_ms=1000, max_records=100)
                
                message_batch = await asyncio.get_event_loop().run_in_executor(
                    self.executor, _poll
                )
                
                for topic_partition, messages in message_batch.items():
                    for message in messages:
                        try:
                            # Process message with handler
                            await self._process_message_with_retry(
                                message.value,
                                handler,
                                max_retries,
                                retry_delay
                            )
                            
                            self.metrics['messages_consumed'] += 1
                            
                            # Commit offset
                            def _commit():
                                consumer.commit()
                            
                            await asyncio.get_event_loop().run_in_executor(
                                self.executor, _commit
                            )
                            
                        except Exception as e:
                            logger.error(
                                f"Failed to process message from {topic_name}: {e}"
                            )
                            self.metrics['messages_failed'] += 1
                            
                            # Send to dead letter queue
                            if processing_config.get('dead_letter_queue', True):
                                await self._send_to_dlq(message.value, topic_type, str(e))
                
                # Update heartbeat
                self.metrics['last_heartbeat'] = datetime.now().isoformat()
                
                # Small delay to prevent busy loop
                await asyncio.sleep(0.1)
                
        except Exception as e:
            logger.error(f"Error in consumer for topic '{topic_name}': {e}")
            raise
        finally:
            if topic_type in self.consumers:
                def _close():
                    self.consumers[topic_type].close()
                
                await asyncio.get_event_loop().run_in_executor(
                    self.executor, _close
                )
                logger.info(f"Consumer closed for topic '{topic_name}'")
    
    async def _process_message_with_retry(
        self,
        message: Dict[str, Any],
        handler: Callable,
        max_retries: int,
        retry_delay: int
    ):
        """
        Process message with retry logic.
        
        Args:
            message: Message to process.
            handler: Handler function to process message.
            max_retries: Maximum number of retries.
            retry_delay: Delay between retries in seconds.
        """
        for attempt in range(max_retries + 1):
            try:
                await handler(message)
                return
            except Exception as e:
                if attempt < max_retries:
                    logger.warning(
                        f"Message processing failed (attempt {attempt + 1}/{max_retries + 1}): {e}"
                    )
                    await asyncio.sleep(retry_delay)
                else:
                    logger.error(f"Message processing failed after {max_retries + 1} attempts: {e}")
                    raise
    
    async def _send_to_dlq(self, message: Dict[str, Any], topic_type: str, error: str):
        """
        Send failed message to dead letter queue.
        
        Args:
            message: Failed message.
            topic_type: Original topic type.
            error: Error message.
        """
        try:
            dlq_topic = f"{self.kafka_config['topics'][topic_type]}-dlq"
            
            dlq_message = {
                'original_message': message,
                'error': error,
                'failed_at': datetime.now().isoformat(),
                'topic_type': topic_type
            }
            
            def _send():
                future = self.producer.send(dlq_topic, value=dlq_message)
                return future.get(timeout=10)
            
            await asyncio.get_event_loop().run_in_executor(
                self.executor, _send
            )
            
            logger.info(f"Message sent to DLQ: {dlq_topic}")
        except Exception as e:
            logger.error(f"Failed to send message to DLQ: {e}")
    
    async def stop_consuming(self):
        """Stop consuming messages from all topics."""
        self._running = False
        
        # Wait for all processing tasks to complete
        if self._processing_tasks:
            await asyncio.gather(*self._processing_tasks, return_exceptions=True)
            self._processing_tasks.clear()
        
        logger.info("Stopped consuming messages")
    
    def get_metrics(self) -> Dict[str, Any]:
        """
        Get service metrics.
        
        Returns:
            Dictionary containing service metrics.
        """
        return {
            **self.metrics,
            'topics': list(self.kafka_config['topics'].values()),
            'consumer_group': self.kafka_config['consumer_group'],
            'running': self._running
        }
    
    async def health_check(self) -> bool:
        """
        Perform health check on Kafka connections.
        
        Returns:
            True if healthy, False otherwise.
        """
        try:
            # Check producer
            if self.producer is None:
                logger.error("Producer is not initialized")
                return False
            
            # Try to get cluster metadata
            def _check():
                return self.producer.bootstrap_connected()
            
            connected = await asyncio.get_event_loop().run_in_executor(
                self.executor, _check
            )
            
            if not connected:
                logger.error("Producer not connected to Kafka cluster")
                return False
            
            logger.info("Kafka health check passed")
            return True
        except Exception as e:
            logger.error(f"Kafka health check failed: {e}")
            return False
    
    async def close(self):
        """Close all Kafka connections gracefully."""
        await self.stop_consuming()
        
        # Close producer
        if self.producer:
            def _close_producer():
                self.producer.flush()
                self.producer.close()
            
            await asyncio.get_event_loop().run_in_executor(
                self.executor, _close_producer
            )
            logger.info("Kafka producer closed")
        
        # Close admin client
        if self.admin_client:
            def _close_admin():
                self.admin_client.close()
            
            await asyncio.get_event_loop().run_in_executor(
                self.executor, _close_admin
            )
            logger.info("Kafka admin client closed")
        
        # Shutdown executor
        self.executor.shutdown(wait=True)
        logger.info("Thread pool executor shutdown")
        
        self._initialized = False
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self.initialize()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()
