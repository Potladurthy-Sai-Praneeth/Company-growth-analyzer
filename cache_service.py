"""Redis Cache Service for message caching and polling."""

import redis
from redis.connection import ConnectionPool
from redis.retry import Retry
from redis.backoff import ExponentialBackoff
import json
from typing import List, Dict, Optional, Any, Tuple
import logging
from contextlib import contextmanager
from datetime import datetime
from utils import load_config, setup_logging

logger = setup_logging(__name__, "cache_service")


class MessageCacheService:
    """Redis Cache Service for managing message caching and polling."""
    
    def __init__(self, config_path: str = None):
        """
        Initialize Message Cache Service.
        
        Args:
            config_path: Path to configuration file.
        """
        self.config = load_config(config_path)
        self.redis_config = self.config['redis']
        self.cache_config = self.redis_config.get('cache', {})
        
        self._initialized = False
        self.pool: Optional[ConnectionPool] = None
        self.client: Optional[redis.Redis] = None
        
        try:
            self._create_connection()
            self._initialized = True
            logger.info("MessageCacheService initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize MessageCacheService: {e}")
            raise
    
    def _create_connection(self):
        """Create Redis connection pool and client."""
        try:
            # Create connection pool
            self.pool = ConnectionPool(
                host=self.redis_config['host'],
                port=self.redis_config['port'],
                db=self.redis_config['db'],
                max_connections=self.redis_config.get('max_connections', 50),
                decode_responses=self.redis_config.get('decode_responses', True),
                socket_timeout=self.redis_config.get('socket_timeout', 5),
                socket_connect_timeout=self.redis_config.get('socket_connect_timeout', 5),
                retry_on_timeout=self.redis_config.get('retry_on_timeout', True),
                health_check_interval=self.redis_config.get('health_check_interval', 30)
            )
            
            # Create Redis client with retry logic
            self.client = redis.Redis(
                connection_pool=self.pool,
                retry=Retry(ExponentialBackoff(base=0.1, cap=2), retries=3),
                retry_on_error=[
                    redis.exceptions.ConnectionError,
                    redis.exceptions.TimeoutError
                ]
            )
            
            # Test connection
            if not self.health_check():
                raise ConnectionError("Unable to connect to Redis server")
            
            logger.info("Redis connection pool created")
        except Exception as e:
            logger.error(f"Failed to create Redis connection: {e}")
            raise
    
    def _get_message_queue_key(self, company_id: str, source: str) -> str:
        """
        Generate Redis key for message queue.
        
        Args:
            company_id: Company identifier.
            source: Message source ('slack' or 'gmail').
            
        Returns:
            Redis key string.
        """
        return f"messages:{company_id}:{source}:queue"
    
    def _get_metadata_key(self, company_id: str, source: str) -> str:
        """
        Generate Redis key for metadata.
        
        Args:
            company_id: Company identifier.
            source: Message source.
            
        Returns:
            Redis key string.
        """
        return f"messages:{company_id}:{source}:metadata"
    
    def _get_processing_flag_key(self, company_id: str, source: str) -> str:
        """
        Generate Redis key for processing flag.
        
        Args:
            company_id: Company identifier.
            source: Message source.
            
        Returns:
            Redis key string.
        """
        return f"messages:{company_id}:{source}:processing"
    
    def add_message(
        self,
        message: Dict[str, Any],
        company_id: str,
        source: str
    ) -> Tuple[bool, int]:
        """
        Add a message to the cache.
        
        Args:
            message: Message dictionary to cache.
            company_id: Company identifier.
            source: Message source ('slack' or 'gmail').
            
        Returns:
            Tuple of (should_process, current_count) where should_process
            indicates if the cache has reached the limit for processing.
        """
        if not self._initialized:
            raise RuntimeError("MessageCacheService is not initialized")
        
        try:
            queue_key = self._get_message_queue_key(company_id, source)
            metadata_key = self._get_metadata_key(company_id, source)
            
            # Prepare message data
            message_data = {
                'message': message,
                'cached_at': datetime.now().isoformat()
            }
            message_json = json.dumps(message_data)
            
            # Use pipeline for atomic operations
            pipe = self.client.pipeline()
            
            # Add message to queue
            pipe.rpush(queue_key, message_json)
            
            # Update metadata
            pipe.hset(metadata_key, mapping={
                'last_updated': datetime.now().isoformat(),
                'company_id': company_id,
                'source': source
            })
            
            # Set TTL on keys
            ttl = self.cache_config.get('ttl', 86400)
            pipe.expire(queue_key, ttl)
            pipe.expire(metadata_key, ttl)
            
            # Get queue length
            pipe.llen(queue_key)
            
            # Execute pipeline
            results = pipe.execute()
            current_count = results[-1]  # Last result is the queue length
            
            logger.debug(
                f"Added message to cache for {company_id}/{source} "
                f"(count: {current_count})"
            )
            
            # Check if we should trigger processing
            message_limit = self.cache_config.get('message_limit', 100)
            should_process = current_count >= message_limit
            
            return should_process, current_count
            
        except redis.exceptions.RedisError as e:
            logger.error(f"Redis error adding message: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error adding message: {e}")
            raise
    
    def get_messages(
        self,
        company_id: str,
        source: str,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Retrieve messages from cache.
        
        Args:
            company_id: Company identifier.
            source: Message source.
            limit: Maximum number of messages to retrieve.
            
        Returns:
            List of message dictionaries.
        """
        if not self._initialized:
            raise RuntimeError("MessageCacheService is not initialized")
        
        try:
            queue_key = self._get_message_queue_key(company_id, source)
            
            if limit is not None:
                message_data_list = self.client.lrange(queue_key, 0, limit - 1)
            else:
                message_data_list = self.client.lrange(queue_key, 0, -1)
            
            messages = []
            for data in message_data_list:
                message_wrapper = json.loads(data)
                messages.append(message_wrapper['message'])
            
            logger.info(
                f"Retrieved {len(messages)} messages from cache for {company_id}/{source}"
            )
            return messages
            
        except redis.exceptions.RedisError as e:
            logger.error(f"Redis error retrieving messages: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error retrieving messages: {e}")
            raise
    
    def get_message_count(self, company_id: str, source: str) -> int:
        """
        Get the count of cached messages.
        
        Args:
            company_id: Company identifier.
            source: Message source.
            
        Returns:
            Number of messages in cache.
        """
        if not self._initialized:
            raise RuntimeError("MessageCacheService is not initialized")
        
        try:
            queue_key = self._get_message_queue_key(company_id, source)
            count = self.client.llen(queue_key)
            
            logger.debug(f"Message count for {company_id}/{source}: {count}")
            return count
            
        except redis.exceptions.RedisError as e:
            logger.error(f"Redis error getting message count: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error getting message count: {e}")
            raise
    
    def pop_messages(
        self,
        company_id: str,
        source: str,
        count: int
    ) -> List[Dict[str, Any]]:
        """
        Pop messages from cache (removes them).
        
        Args:
            company_id: Company identifier.
            source: Message source.
            count: Number of messages to pop.
            
        Returns:
            List of popped message dictionaries.
        """
        if not self._initialized:
            raise RuntimeError("MessageCacheService is not initialized")
        
        try:
            queue_key = self._get_message_queue_key(company_id, source)
            
            messages = []
            pipe = self.client.pipeline()
            
            # Pop messages from left (FIFO)
            for _ in range(count):
                pipe.lpop(queue_key)
            
            results = pipe.execute()
            
            for data in results:
                if data:
                    message_wrapper = json.loads(data)
                    messages.append(message_wrapper['message'])
            
            logger.debug(
                f"Popped {len(messages)} messages from cache for {company_id}/{source}"
            )
            return messages
            
        except redis.exceptions.RedisError as e:
            logger.error(f"Redis error popping messages: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error popping messages: {e}")
            raise
    
    def clear_cache(self, company_id: str, source: str) -> bool:
        """
        Clear all cached messages for a company/source.
        
        Args:
            company_id: Company identifier.
            source: Message source.
            
        Returns:
            True if cleared successfully.
        """
        if not self._initialized:
            raise RuntimeError("MessageCacheService is not initialized")
        
        try:
            queue_key = self._get_message_queue_key(company_id, source)
            metadata_key = self._get_metadata_key(company_id, source)
            processing_flag_key = self._get_processing_flag_key(company_id, source)
            
            self.client.delete(queue_key, metadata_key, processing_flag_key)
            
            logger.info(f"Cleared cache for {company_id}/{source}")
            return True
            
        except redis.exceptions.RedisError as e:
            logger.error(f"Redis error clearing cache: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error clearing cache: {e}")
            raise
    
    def poll_for_processing(self) -> List[Dict[str, Any]]:
        """
        Poll all cached queues and return those ready for processing.
        
        Returns:
            List of dictionaries with company_id, source, and count
            for queues that have reached the processing threshold.
        """
        if not self._initialized:
            raise RuntimeError("MessageCacheService is not initialized")
        
        try:
            ready_queues = []
            message_limit = self.cache_config.get('message_limit', 100)
            
            # Get all message queue keys
            pattern = "messages:*:queue"
            cursor = 0
            
            while True:
                cursor, keys = self.client.scan(cursor, match=pattern, count=100)
                
                for key in keys:
                    # Get queue length
                    count = self.client.llen(key)
                    
                    if count >= message_limit:
                        # Parse key to extract company_id and source
                        # Key format: messages:{company_id}:{source}:queue
                        parts = key.split(':')
                        if len(parts) >= 4:
                            company_id = parts[1]
                            source = parts[2]
                            
                            # Check if not already being processed
                            processing_flag_key = self._get_processing_flag_key(
                                company_id, source
                            )
                            
                            # Try to set processing flag with NX (only if not exists)
                            if self.client.set(processing_flag_key, '1', nx=True, ex=300):
                                ready_queues.append({
                                    'company_id': company_id,
                                    'source': source,
                                    'count': count
                                })
                                logger.info(
                                    f"Queue {company_id}/{source} ready for processing "
                                    f"({count} messages)"
                                )
                
                if cursor == 0:
                    break
            
            return ready_queues
            
        except redis.exceptions.RedisError as e:
            logger.error(f"Redis error polling for processing: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error polling for processing: {e}")
            raise
    
    def release_processing_lock(self, company_id: str, source: str) -> bool:
        """
        Release the processing lock for a queue.
        
        Args:
            company_id: Company identifier.
            source: Message source.
            
        Returns:
            True if released successfully.
        """
        if not self._initialized:
            raise RuntimeError("MessageCacheService is not initialized")
        
        try:
            processing_flag_key = self._get_processing_flag_key(company_id, source)
            self.client.delete(processing_flag_key)
            
            logger.debug(f"Released processing lock for {company_id}/{source}")
            return True
            
        except redis.exceptions.RedisError as e:
            logger.error(f"Redis error releasing processing lock: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error releasing processing lock: {e}")
            raise
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """
        Get statistics about all cached queues.
        
        Returns:
            Dictionary containing cache statistics.
        """
        if not self._initialized:
            raise RuntimeError("MessageCacheService is not initialized")
        
        try:
            stats = {
                'total_queues': 0,
                'total_messages': 0,
                'queues': []
            }
            
            # Get all message queue keys
            pattern = "messages:*:queue"
            cursor = 0
            
            while True:
                cursor, keys = self.client.scan(cursor, match=pattern, count=100)
                
                for key in keys:
                    count = self.client.llen(key)
                    
                    # Parse key
                    parts = key.split(':')
                    if len(parts) >= 4:
                        company_id = parts[1]
                        source = parts[2]
                        
                        stats['queues'].append({
                            'company_id': company_id,
                            'source': source,
                            'count': count
                        })
                        stats['total_messages'] += count
                        stats['total_queues'] += 1
                
                if cursor == 0:
                    break
            
            return stats
            
        except redis.exceptions.RedisError as e:
            logger.error(f"Redis error getting cache stats: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error getting cache stats: {e}")
            raise
    
    def health_check(self) -> bool:
        """
        Perform health check on Redis connection.
        
        Returns:
            True if healthy, False otherwise.
        """
        try:
            self.client.ping()
            return True
        except Exception as e:
            logger.error(f"Redis health check failed: {e}")
            return False
    
    def close(self):
        """Close Redis connection and connection pool."""
        if self.client:
            try:
                self.client.close()
                logger.info("Redis connection closed")
            except Exception as e:
                logger.error(f"Error closing Redis connection: {e}")
        
        if self.pool:
            try:
                self.pool.disconnect()
                logger.info("Redis connection pool disconnected")
            except Exception as e:
                logger.error(f"Error disconnecting connection pool: {e}")
        
        self._initialized = False
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
