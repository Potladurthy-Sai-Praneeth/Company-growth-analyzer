"""Cassandra Database Service for storing messages."""

import os
from datetime import datetime
from typing import List, Dict, Optional, Any
from cassandra.cluster import Cluster, Session
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement, ConsistencyLevel
from cassandra.util import uuid_from_time
from uuid import UUID, uuid4
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy
import asyncio
from concurrent.futures import ThreadPoolExecutor
import logging
from utils import load_config, setup_logging
from dotenv import load_dotenv

load_dotenv()

logger = setup_logging(__name__, "database_service")


class MessageDatabaseService:
    """Manages Cassandra database operations for message storage."""
    
    def __init__(self, config_path: str = None):
        """
        Initialize Message Database Service.
        
        Args:
            config_path: Path to configuration file.
        """
        self.config = load_config(config_path)
        self.cassandra_config = self.config['cassandra']
        
        self.cluster: Optional[Cluster] = None
        self.session: Optional[Session] = None
        self.executor = ThreadPoolExecutor(
            max_workers=self.cassandra_config.get('max_workers', 10)
        )
        
        try:
            self.loop = asyncio.get_running_loop()
        except RuntimeError:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
        
        self._initialized = False
        self.prepared_statements = {}
    
    async def initialize(self):
        """Initialize connection and schema asynchronously."""
        if self._initialized:
            logger.warning("MessageDatabaseService already initialized")
            return
        
        await self._connect()
        await self._create_schema()
        await self._prepare_statements()
        self._initialized = True
        logger.info("MessageDatabaseService initialized successfully")
    
    async def _connect(self):
        """Establish connection to Cassandra cluster."""
        try:
            cassandra_host = self.cassandra_config['host']
            cassandra_port = self.cassandra_config['port']
            
            # Check for authentication
            username = os.getenv("CASSANDRA_USERNAME")
            password = os.getenv("CASSANDRA_PASSWORD")
            
            auth_provider = None
            if username and password:
                auth_provider = PlainTextAuthProvider(
                    username=username,
                    password=password
                )
            
            self.cluster = Cluster(
                contact_points=[cassandra_host],
                port=cassandra_port,
                load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy()),
                protocol_version=4,
                auth_provider=auth_provider
            )
            
            self.session = await self.loop.run_in_executor(
                self.executor, self.cluster.connect
            )
            
            logger.info("Connected to Cassandra cluster")
        except Exception as e:
            logger.error(f"Failed to connect to Cassandra: {e}")
            raise
    
    async def _create_schema(self):
        """Create keyspace and tables if they do not exist."""
        try:
            keyspace = self.cassandra_config.get('keyspace', 'message_queue_db')
            replication_factor = self.cassandra_config.get('replication_factor', 3)
            
            # Create keyspace
            create_keyspace_cql = f"""
            CREATE KEYSPACE IF NOT EXISTS {keyspace}
            WITH REPLICATION = {{ 
                'class' : 'SimpleStrategy', 
                'replication_factor' : {replication_factor} 
            }};
            """
            
            await self.loop.run_in_executor(
                self.executor,
                lambda: self.session.execute(create_keyspace_cql)
            )
            logger.info(f"Keyspace '{keyspace}' ensured")
            
            # Set keyspace
            self.session.set_keyspace(keyspace)
            
            # Get table names
            chat_messages_table = self.cassandra_config['tables'].get('chat_messages', 'chat_messages')
            email_messages_table = self.cassandra_config['tables'].get('email_messages', 'email_messages')
            metadata_table = self.cassandra_config['tables'].get('message_metadata', 'message_metadata')            
            
            # Create chat messages table
            # This table stores chat messages from Slack
            create_chat_messages_table_cql = f"""
            CREATE TABLE IF NOT EXISTS {chat_messages_table} (
                message_id UUID,
                company_id TEXT,
                source TEXT,
                timestamp TIMESTAMP,
                user_id TEXT,
                content TEXT,
                thread_id TEXT,
                channel_id TEXT,
                team_id TEXT,
                client_msg_id TEXT,
                metadata TEXT,
                created_at TIMESTAMP,
                PRIMARY KEY ((company_id), timestamp, message_id)
            ) WITH CLUSTERING ORDER BY (timestamp DESC, message_id DESC)
            AND compaction = {{'class': 'TimeWindowCompactionStrategy'}}
            AND default_time_to_live = 7776000;
            """
            
            await self.loop.run_in_executor(
                self.executor,
                lambda: self.session.execute(create_chat_messages_table_cql)
            )
            logger.info(f"Table '{chat_messages_table}' ensured")
            
            # Create email messages table
            # This table stores email messages from Gmail
            create_email_messages_table_cql = f"""
            CREATE TABLE IF NOT EXISTS {email_messages_table} (
                message_id UUID,
                company_id TEXT,
                source TEXT,
                timestamp TIMESTAMP,
                sender_email TEXT,
                receiver_email TEXT,
                subject TEXT,
                content TEXT,
                thread_id TEXT,
                snippet TEXT,
                label_ids TEXT,
                metadata TEXT,
                created_at TIMESTAMP,
                PRIMARY KEY ((company_id), timestamp, message_id)
            ) WITH CLUSTERING ORDER BY (timestamp DESC, message_id DESC)
            AND compaction = {{'class': 'TimeWindowCompactionStrategy'}}
            AND default_time_to_live = 7776000;
            """
            
            await self.loop.run_in_executor(
                self.executor,
                lambda: self.session.execute(create_email_messages_table_cql)
            )
            logger.info(f"Table '{email_messages_table}' ensured")
            
            # Create message_metadata table for quick lookups
            create_metadata_table_cql = f"""
            CREATE TABLE IF NOT EXISTS {metadata_table} (
                message_id UUID PRIMARY KEY,
                company_id TEXT,
                source TEXT,
                topic TEXT,
                timestamp TIMESTAMP,
                processed_at TIMESTAMP,
                status TEXT,
                retry_count INT
            );
            """
            
            await self.loop.run_in_executor(
                self.executor,
                lambda: self.session.execute(create_metadata_table_cql)
            )
            logger.info(f"Table '{metadata_table}' ensured")
            
            # Create index on message_id in chat messages table for lookups
            create_chat_index_cql = f"""
            CREATE INDEX IF NOT EXISTS chat_messages_by_id 
            ON {chat_messages_table} (message_id);
            """
            
            await self.loop.run_in_executor(
                self.executor,
                lambda: self.session.execute(create_chat_index_cql)
            )
            
            # Create index on message_id in email messages table for lookups
            create_email_index_cql = f"""
            CREATE INDEX IF NOT EXISTS email_messages_by_id 
            ON {email_messages_table} (message_id);
            """
            
            await self.loop.run_in_executor(
                self.executor,
                lambda: self.session.execute(create_email_index_cql)
            )
            
            logger.info("Schema creation completed")
        except Exception as e:
            logger.error(f"Failed to create schema: {e}")
            raise
    
    async def _prepare_statements(self):
        """Prepare frequently used CQL statements."""
        try:
            chat_messages_table = self.cassandra_config['tables'].get('chat_messages', 'chat_messages')
            email_messages_table = self.cassandra_config['tables'].get('email_messages', 'email_messages')
            metadata_table = self.cassandra_config['tables'].get(
                'message_metadata', 'message_metadata'
            )
            
            # Insert chat message
            insert_chat_message_cql = f"""
            INSERT INTO {chat_messages_table} (
                message_id, company_id, source, timestamp, user_id,
                content, thread_id, channel_id, team_id, client_msg_id,
                metadata, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """
            
            # Insert email message
            insert_email_message_cql = f"""
            INSERT INTO {email_messages_table} (
                message_id, company_id, source, timestamp,
                sender_email, receiver_email, subject, content,
                thread_id, snippet, label_ids, metadata, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """
            
            # Insert metadata
            insert_metadata_cql = f"""
            INSERT INTO {metadata_table} (
                message_id, company_id, source, topic, 
                timestamp, processed_at, status, retry_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
            """
            
            # Select chat messages by company
            select_chat_messages_cql = f"""
            SELECT message_id, company_id, source, timestamp, user_id,
                   content, thread_id, channel_id, team_id, client_msg_id,
                   metadata, created_at
            FROM {chat_messages_table}
            WHERE company_id = ?
            LIMIT ?;
            """
            
            # Select chat messages by company and time range
            select_chat_messages_time_range_cql = f"""
            SELECT message_id, company_id, source, timestamp, user_id,
                   content, thread_id, channel_id, team_id, client_msg_id,
                   metadata, created_at
            FROM {chat_messages_table}
            WHERE company_id = ?
            AND timestamp >= ? AND timestamp <= ?
            LIMIT ?;
            """
            
            # Select email messages by company
            select_email_messages_cql = f"""
            SELECT message_id, company_id, source, timestamp,
                   sender_email, receiver_email, subject, content,
                   thread_id, snippet, label_ids, metadata, created_at
            FROM {email_messages_table}
            WHERE company_id = ?
            LIMIT ?;
            """
            
            # Select email messages by company and time range
            select_email_messages_time_range_cql = f"""
            SELECT message_id, company_id, source, timestamp,
                   sender_email, receiver_email, subject, content,
                   thread_id, snippet, label_ids, metadata, created_at
            FROM {email_messages_table}
            WHERE company_id = ?
            AND timestamp >= ? AND timestamp <= ?
            LIMIT ?;
            """
            
            # Get message metadata
            select_metadata_cql = f"""
            SELECT message_id, company_id, source, topic, 
                   timestamp, processed_at, status, retry_count
            FROM {metadata_table}
            WHERE message_id = ?;
            """
            
            # Update metadata status
            update_metadata_status_cql = f"""
            UPDATE {metadata_table}
            SET status = ?, processed_at = ?
            WHERE message_id = ?;
            """
            
            # Prepare all statements
            self.prepared_statements['insert_chat_message'] = await self.loop.run_in_executor(
                self.executor, lambda: self.session.prepare(insert_chat_message_cql)
            )
            self.prepared_statements['insert_email_message'] = await self.loop.run_in_executor(
                self.executor, lambda: self.session.prepare(insert_email_message_cql)
            )
            self.prepared_statements['insert_metadata'] = await self.loop.run_in_executor(
                self.executor, lambda: self.session.prepare(insert_metadata_cql)
            )
            self.prepared_statements['select_chat_messages'] = await self.loop.run_in_executor(
                self.executor, lambda: self.session.prepare(select_chat_messages_cql)
            )
            self.prepared_statements['select_chat_messages_time_range'] = await self.loop.run_in_executor(
                self.executor, lambda: self.session.prepare(select_chat_messages_time_range_cql)
            )
            self.prepared_statements['select_email_messages'] = await self.loop.run_in_executor(
                self.executor, lambda: self.session.prepare(select_email_messages_cql)
            )
            self.prepared_statements['select_email_messages_time_range'] = await self.loop.run_in_executor(
                self.executor, lambda: self.session.prepare(select_email_messages_time_range_cql)
            )
            self.prepared_statements['select_metadata'] = await self.loop.run_in_executor(
                self.executor, lambda: self.session.prepare(select_metadata_cql)
            )
            self.prepared_statements['update_metadata_status'] = await self.loop.run_in_executor(
                self.executor, lambda: self.session.prepare(update_metadata_status_cql)
            )
            
            logger.info("Prepared statements created")
        except Exception as e:
            logger.error(f"Failed to prepare statements: {e}")
            raise
    
    async def store_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """
        Store a message in the database.
        
        Args:
            message: Message dictionary from Kafka.
            
        Returns:
            Dictionary with message_id and status.
        """
        if not self._initialized:
            raise RuntimeError("MessageDatabaseService not initialized. Call initialize() first.")
        
        try:
            # Extract message data
            metadata = message.get('metadata', {})
            company_id = metadata.get('company_id', 'unknown')
            source = metadata.get('source', 'unknown')
            topic = metadata.get('topic', 'unknown')
            
            # Generate message ID
            message_id = uuid4()
            created_at = datetime.now()
            
            # Extract timestamp
            if 'ts' in message:
                # Slack timestamp
                timestamp = datetime.fromtimestamp(float(message['ts']))
            elif 'internalDate' in message:
                # Gmail timestamp (milliseconds)
                timestamp = datetime.fromtimestamp(int(message['internalDate']) / 1000)
            else:
                timestamp = created_at
            
            # Store metadata as JSON string
            import json
            metadata_json = json.dumps(metadata)
            
            def _execute_insert():
                # Insert message based on topic type
                if topic == 'chat':
                    # Extract chat-specific fields
                    user_id = message.get('user', '')
                    content = message.get('text', '')
                    thread_id = message.get('thread_ts')
                    channel_id = message.get('channel')
                    team_id = message.get('team', '')
                    client_msg_id = message.get('client_msg_id', '')
                    
                    # Insert chat message
                    future = self.session.execute_async(
                        self.prepared_statements['insert_chat_message'],
                        (
                            message_id, company_id, source, timestamp, user_id,
                            content, thread_id, channel_id, team_id, client_msg_id,
                            metadata_json, created_at
                        )
                    )
                    future.result()
                else:  # email
                    # Extract email-specific fields from headers
                    headers = {h['name']: h['value'] for h in message.get('payload', {}).get('headers', [])}
                    sender_email = headers.get('From', '')
                    receiver_email = headers.get('To', '')
                    subject = headers.get('Subject', '')
                    
                    # Get content from snippet or body
                    content = message.get('snippet', '')
                    thread_id = message.get('threadId', '')
                    snippet = message.get('snippet', '')
                    label_ids = ','.join(message.get('labelIds', []))
                    
                    # Insert email message
                    future = self.session.execute_async(
                        self.prepared_statements['insert_email_message'],
                        (
                            message_id, company_id, source, timestamp,
                            sender_email, receiver_email, subject, content,
                            thread_id, snippet, label_ids, metadata_json, created_at
                        )
                    )
                    future.result()
                
                # Insert metadata (same for both)
                future = self.session.execute_async(
                    self.prepared_statements['insert_metadata'],
                    (
                        message_id, company_id, source, topic,
                        timestamp, created_at, 'stored', 0
                    )
                )
                future.result()
            
            await self.loop.run_in_executor(self.executor, _execute_insert)
            
            logger.debug(f"Stored message {message_id} from {source} for company {company_id}")
            
            return {
                "message_id": str(message_id),
                "company_id": company_id,
                "source": source,
                "topic": topic,
                "timestamp": timestamp,
                "status": "stored"
            }
            
        except Exception as e:
            logger.error(f"Failed to store message: {e}")
            raise
    
    async def get_messages(
        self,
        company_id: str,
        source: str,
        limit: int = 100,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """
        Retrieve messages for a company from a specific source.
        
        Args:
            company_id: Company identifier.
            source: Message source ('slack' or 'gmail').
            limit: Maximum number of messages to retrieve.
            start_time: Optional start time filter.
            end_time: Optional end time filter.
            
        Returns:
            List of message dictionaries.
        """
        if not self._initialized:
            raise RuntimeError("MessageDatabaseService not initialized. Call initialize() first.")
        
        try:
            # Determine which table to query based on source
            is_chat = source.lower() == 'slack'
            
            def _execute():
                if is_chat:
                    if start_time and end_time:
                        future = self.session.execute_async(
                            self.prepared_statements['select_chat_messages_time_range'],
                            (company_id, start_time, end_time, limit)
                        )
                    else:
                        future = self.session.execute_async(
                            self.prepared_statements['select_chat_messages'],
                            (company_id, limit)
                        )
                else:  # email
                    if start_time and end_time:
                        future = self.session.execute_async(
                            self.prepared_statements['select_email_messages_time_range'],
                            (company_id, start_time, end_time, limit)
                        )
                    else:
                        future = self.session.execute_async(
                            self.prepared_statements['select_email_messages'],
                            (company_id, limit)
                        )
                return future.result()
            
            rows = await self.loop.run_in_executor(self.executor, _execute)
            
            messages = []
            import json
            for row in rows:
                if is_chat:
                    message = {
                        "message_id": str(row.message_id),
                        "company_id": row.company_id,
                        "source": row.source,
                        "timestamp": row.timestamp,
                        "user_id": row.user_id,
                        "content": row.content,
                        "thread_id": row.thread_id,
                        "channel_id": row.channel_id,
                        "team_id": row.team_id,
                        "client_msg_id": row.client_msg_id,
                        "metadata": json.loads(row.metadata) if row.metadata else {},
                        "created_at": row.created_at
                    }
                else:  # email
                    message = {
                        "message_id": str(row.message_id),
                        "company_id": row.company_id,
                        "source": row.source,
                        "timestamp": row.timestamp,
                        "sender_email": row.sender_email,
                        "receiver_email": row.receiver_email,
                        "subject": row.subject,
                        "content": row.content,
                        "thread_id": row.thread_id,
                        "snippet": row.snippet,
                        "label_ids": row.label_ids.split(',') if row.label_ids else [],
                        "metadata": json.loads(row.metadata) if row.metadata else {},
                        "created_at": row.created_at
                    }
                messages.append(message)
            
            logger.info(
                f"Retrieved {len(messages)} messages for company {company_id} from {source}"
            )
            return messages
            
        except Exception as e:
            logger.error(f"Failed to retrieve messages: {e}")
            raise
    
    async def get_message_metadata(self, message_id: str) -> Optional[Dict[str, Any]]:
        """
        Get metadata for a specific message.
        
        Args:
            message_id: Message identifier.
            
        Returns:
            Metadata dictionary or None if not found.
        """
        if not self._initialized:
            raise RuntimeError("MessageDatabaseService not initialized. Call initialize() first.")
        
        try:
            message_uuid = UUID(message_id)
            
            def _execute():
                future = self.session.execute_async(
                    self.prepared_statements['select_metadata'],
                    (message_uuid,)
                )
                return future.result()
            
            row = await self.loop.run_in_executor(self.executor, _execute)
            result = row.one()
            
            if result:
                metadata = {
                    "message_id": str(result.message_id),
                    "company_id": result.company_id,
                    "source": result.source,
                    "topic": result.topic,
                    "timestamp": result.timestamp,
                    "processed_at": result.processed_at,
                    "status": result.status,
                    "retry_count": result.retry_count
                }
                logger.info(f"Retrieved metadata for message {message_id}")
                return metadata
            else:
                logger.info(f"No metadata found for message {message_id}")
                return None
                
        except Exception as e:
            logger.error(f"Failed to retrieve message metadata: {e}")
            raise
    
    async def update_message_status(
        self,
        message_id: str,
        status: str
    ) -> bool:
        """
        Update the status of a message.
        
        Args:
            message_id: Message identifier.
            status: New status ('stored', 'processed', 'failed', etc.).
            
        Returns:
            True if updated successfully.
        """
        if not self._initialized:
            raise RuntimeError("MessageDatabaseService not initialized. Call initialize() first.")
        
        try:
            message_uuid = UUID(message_id)
            processed_at = datetime.now()
            
            def _execute():
                future = self.session.execute_async(
                    self.prepared_statements['update_metadata_status'],
                    (status, processed_at, message_uuid)
                )
                return future.result()
            
            await self.loop.run_in_executor(self.executor, _execute)
            
            logger.info(f"Updated status for message {message_id} to '{status}'")
            return True
            
        except Exception as e:
            logger.error(f"Failed to update message status: {e}")
            raise
    
    async def health_check(self) -> bool:
        """Perform a health check on the Cassandra connection."""
        if not self._initialized:
            logger.error("MessageDatabaseService not initialized")
            return False
        
        try:
            def _execute():
                future = self.session.execute_async("SELECT now() FROM system.local;")
                return future.result()
            
            await self.loop.run_in_executor(self.executor, _execute)
            logger.info("Cassandra health check passed")
            return True
        except Exception as e:
            logger.error(f"Cassandra health check failed: {e}")
            return False
    
    async def close(self):
        """Close the Cassandra connection gracefully."""
        if self.cluster:
            await self.loop.run_in_executor(
                self.executor, self.cluster.shutdown
            )
            logger.info("Cassandra connection closed")
        
        if self.executor:
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
