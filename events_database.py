"""PostgreSQL Database Service for storing significant events."""

import os
import asyncio
import asyncpg
from asyncpg.pool import Pool
from typing import List, Dict, Any, Optional
from datetime import datetime
import json
import logging
from uuid import UUID
from dotenv import load_dotenv

from models import SignificantEvent, EventType, EventSeverity
from utils import load_config, setup_logging

load_dotenv()

logger = setup_logging(__name__, "events_database")


class EventsDatabaseService:
    """PostgreSQL Database Service for managing significant events."""
    
    def __init__(self, config_path: str = None):
        """
        Initialize Events Database Service.
        
        Args:
            config_path: Path to configuration file.
        """
        self.config = load_config(config_path)
        self.postgres_config = self.config.get('postgres', {})
        
        self.pool: Optional[Pool] = None
        self._initialized = False
    
    async def initialize(self):
        """Initialize connection pool and create schema."""
        if self._initialized:
            logger.warning("EventsDatabaseService already initialized")
            return
        
        try:
            await self._create_pool()
            await self._create_schema()
            self._initialized = True
            logger.info("EventsDatabaseService initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize EventsDatabaseService: {e}")
            raise
    
    async def _create_pool(self):
        """Create connection pool to PostgreSQL."""
        try:
            host = self.postgres_config.get('host', os.getenv('POSTGRES_HOST', 'localhost'))
            port = self.postgres_config.get('port', int(os.getenv('POSTGRES_PORT', 5432)))
            database = self.postgres_config.get('database', os.getenv('POSTGRES_DB', 'events_db'))
            user = self.postgres_config.get('user', os.getenv('POSTGRES_USER', 'postgres'))
            password = self.postgres_config.get('password', os.getenv('POSTGRES_PASSWORD', 'postgres'))
            
            self.pool = await asyncpg.create_pool(
                host=host,
                port=port,
                database=database,
                user=user,
                password=password,
                min_size=5,
                max_size=20,
                command_timeout=60
            )
            
            logger.info(f"PostgreSQL connection pool created to {host}:{port}/{database}")
        except Exception as e:
            logger.error(f"Failed to create connection pool: {e}")
            raise
    
    async def _create_schema(self):
        """Create events table and indices."""
        create_enum_sql = """
        DO $$ BEGIN
            CREATE TYPE event_type AS ENUM (
                'user_growth', 'revenue_milestone', 'funding', 'partnership',
                'product_launch', 'hiring', 'enterprise_deal', 'technical_achievement',
                'market_expansion', 'compliance', 'media_coverage', 'investor_interest',
                'customer_success', 'other'
            );
        EXCEPTION
            WHEN duplicate_object THEN null;
        END $$;
        
        DO $$ BEGIN
            CREATE TYPE event_severity AS ENUM ('high', 'medium', 'low');
        EXCEPTION
            WHEN duplicate_object THEN null;
        END $$;
        """
        
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS significant_events (
            id UUID PRIMARY KEY,
            event_type event_type NOT NULL,
            title VARCHAR(200) NOT NULL,
            description TEXT NOT NULL,
            impact TEXT NOT NULL,
            metrics JSONB,
            timeline VARCHAR(255) NOT NULL,
            source_messages JSONB DEFAULT '[]'::jsonb,
            severity event_severity DEFAULT 'medium',
            stakeholders JSONB DEFAULT '[]'::jsonb,
            company_id VARCHAR(100) DEFAULT 'default',
            batch_id VARCHAR(100),
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            is_active BOOLEAN DEFAULT TRUE
        );
        
        CREATE INDEX IF NOT EXISTS idx_events_company_id ON significant_events(company_id);
        CREATE INDEX IF NOT EXISTS idx_events_event_type ON significant_events(event_type);
        CREATE INDEX IF NOT EXISTS idx_events_created_at ON significant_events(created_at);
        CREATE INDEX IF NOT EXISTS idx_events_is_active ON significant_events(is_active);
        CREATE INDEX IF NOT EXISTS idx_events_severity ON significant_events(severity);
        """
        
        async with self.pool.acquire() as conn:
            await conn.execute(create_enum_sql)
            await conn.execute(create_table_sql)
            logger.info("Events schema created successfully")
    
    async def store_event(
        self,
        event: SignificantEvent,
        company_id: str = "default",
        batch_id: Optional[str] = None
    ) -> str:
        """
        Store a significant event in the database.
        
        Args:
            event: SignificantEvent model to store.
            company_id: Company identifier.
            batch_id: Optional batch identifier.
            
        Returns:
            The event ID.
        """
        if not self._initialized:
            raise RuntimeError("EventsDatabaseService not initialized")
        
        try:
            insert_sql = """
            INSERT INTO significant_events (
                id, event_type, title, description, impact, metrics,
                timeline, source_messages, severity, stakeholders,
                company_id, batch_id, created_at, is_active
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
            ON CONFLICT (id) DO UPDATE SET
                event_type = EXCLUDED.event_type,
                title = EXCLUDED.title,
                description = EXCLUDED.description,
                impact = EXCLUDED.impact,
                metrics = EXCLUDED.metrics,
                timeline = EXCLUDED.timeline,
                source_messages = EXCLUDED.source_messages,
                severity = EXCLUDED.severity,
                stakeholders = EXCLUDED.stakeholders,
                updated_at = NOW()
            RETURNING id;
            """
            
            async with self.pool.acquire() as conn:
                result = await conn.fetchval(
                    insert_sql,
                    UUID(event.id),
                    event.event_type,
                    event.title,
                    event.description,
                    event.impact,
                    json.dumps(event.metrics) if event.metrics else None,
                    event.timeline,
                    json.dumps(event.source_messages),
                    event.severity,
                    json.dumps(event.stakeholders),
                    company_id,
                    batch_id,
                    event.created_at,
                    event.is_active
                )
            
            logger.info(f"Stored event {event.id}: {event.title}")
            return str(result)
            
        except Exception as e:
            logger.error(f"Failed to store event: {e}")
            raise
    
    async def store_events_batch(
        self,
        events: List[SignificantEvent],
        company_id: str = "default",
        batch_id: Optional[str] = None
    ) -> List[str]:
        """
        Store multiple events in a batch.
        
        Args:
            events: List of SignificantEvent models.
            company_id: Company identifier.
            batch_id: Optional batch identifier.
            
        Returns:
            List of stored event IDs.
        """
        if not self._initialized:
            raise RuntimeError("EventsDatabaseService not initialized")
        
        stored_ids = []
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                for event in events:
                    event_id = await self.store_event(event, company_id, batch_id)
                    stored_ids.append(event_id)
        
        logger.info(f"Stored {len(stored_ids)} events in batch {batch_id}")
        return stored_ids
    
    async def get_event(self, event_id: str) -> Optional[SignificantEvent]:
        """
        Retrieve a single event by ID.
        
        Args:
            event_id: Event identifier.
            
        Returns:
            SignificantEvent or None if not found.
        """
        if not self._initialized:
            raise RuntimeError("EventsDatabaseService not initialized")
        
        try:
            select_sql = """
            SELECT id, event_type, title, description, impact, metrics,
                   timeline, source_messages, severity, stakeholders,
                   created_at, is_active
            FROM significant_events
            WHERE id = $1;
            """
            
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(select_sql, UUID(event_id))
            
            if row:
                return self._row_to_event(row)
            return None
            
        except Exception as e:
            logger.error(f"Failed to retrieve event: {e}")
            raise
    
    async def get_all_events(
        self,
        company_id: Optional[str] = None,
        event_type: Optional[EventType] = None,
        is_active: bool = True,
        limit: int = 100
    ) -> List[SignificantEvent]:
        """
        Retrieve all events with optional filters.
        
        Args:
            company_id: Filter by company.
            event_type: Filter by event type.
            is_active: Filter by active status.
            limit: Maximum number of events to return.
            
        Returns:
            List of SignificantEvent models.
        """
        if not self._initialized:
            raise RuntimeError("EventsDatabaseService not initialized")
        
        try:
            conditions = ["is_active = $1"]
            params = [is_active]
            param_count = 1
            
            if company_id:
                param_count += 1
                conditions.append(f"company_id = ${param_count}")
                params.append(company_id)
            
            if event_type:
                param_count += 1
                conditions.append(f"event_type = ${param_count}")
                params.append(event_type.value if isinstance(event_type, EventType) else event_type)
            
            param_count += 1
            params.append(limit)
            
            where_clause = " AND ".join(conditions)
            
            select_sql = f"""
            SELECT id, event_type, title, description, impact, metrics,
                   timeline, source_messages, severity, stakeholders,
                   created_at, is_active
            FROM significant_events
            WHERE {where_clause}
            ORDER BY created_at DESC
            LIMIT ${param_count};
            """
            
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(select_sql, *params)
            
            events = [self._row_to_event(row) for row in rows]
            logger.info(f"Retrieved {len(events)} events")
            return events
            
        except Exception as e:
            logger.error(f"Failed to retrieve events: {e}")
            raise
    
    async def get_active_events_for_email(
        self,
        company_id: str = "default"
    ) -> List[SignificantEvent]:
        """
        Retrieve all active events for investor email generation.
        
        Args:
            company_id: Company identifier.
            
        Returns:
            List of active SignificantEvent models.
        """
        return await self.get_all_events(
            company_id=company_id,
            is_active=True,
            limit=500
        )
    
    async def update_event_status(
        self,
        event_id: str,
        is_active: bool
    ) -> bool:
        """
        Update the active status of an event.
        
        Args:
            event_id: Event identifier.
            is_active: New active status.
            
        Returns:
            True if updated successfully.
        """
        if not self._initialized:
            raise RuntimeError("EventsDatabaseService not initialized")
        
        try:
            update_sql = """
            UPDATE significant_events
            SET is_active = $1, updated_at = NOW()
            WHERE id = $2;
            """
            
            async with self.pool.acquire() as conn:
                await conn.execute(update_sql, is_active, UUID(event_id))
            
            logger.info(f"Updated event {event_id} status to {is_active}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to update event status: {e}")
            raise
    
    async def delete_event(self, event_id: str) -> bool:
        """
        Delete an event from the database.
        
        Args:
            event_id: Event identifier.
            
        Returns:
            True if deleted successfully.
        """
        if not self._initialized:
            raise RuntimeError("EventsDatabaseService not initialized")
        
        try:
            delete_sql = "DELETE FROM significant_events WHERE id = $1;"
            
            async with self.pool.acquire() as conn:
                await conn.execute(delete_sql, UUID(event_id))
            
            logger.info(f"Deleted event {event_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete event: {e}")
            raise
    
    async def get_event_statistics(
        self,
        company_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get statistics about stored events.
        
        Args:
            company_id: Optional company filter.
            
        Returns:
            Dictionary with event statistics.
        """
        if not self._initialized:
            raise RuntimeError("EventsDatabaseService not initialized")
        
        try:
            company_filter = ""
            params = []
            if company_id:
                company_filter = "WHERE company_id = $1"
                params = [company_id]
            
            stats_sql = f"""
            SELECT 
                COUNT(*) as total_events,
                COUNT(*) FILTER (WHERE is_active = TRUE) as active_events,
                COUNT(*) FILTER (WHERE severity = 'high') as high_severity,
                COUNT(*) FILTER (WHERE severity = 'medium') as medium_severity,
                COUNT(*) FILTER (WHERE severity = 'low') as low_severity,
                MIN(created_at) as earliest_event,
                MAX(created_at) as latest_event
            FROM significant_events
            {company_filter};
            """
            
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(stats_sql, *params)
            
            # Get event type distribution
            type_dist_sql = f"""
            SELECT event_type, COUNT(*) as count
            FROM significant_events
            {company_filter}
            GROUP BY event_type
            ORDER BY count DESC;
            """
            
            async with self.pool.acquire() as conn:
                type_rows = await conn.fetch(type_dist_sql, *params)
            
            type_distribution = {r['event_type']: r['count'] for r in type_rows}
            
            return {
                'total_events': row['total_events'],
                'active_events': row['active_events'],
                'severity_distribution': {
                    'high': row['high_severity'],
                    'medium': row['medium_severity'],
                    'low': row['low_severity']
                },
                'type_distribution': type_distribution,
                'earliest_event': row['earliest_event'].isoformat() if row['earliest_event'] else None,
                'latest_event': row['latest_event'].isoformat() if row['latest_event'] else None
            }
            
        except Exception as e:
            logger.error(f"Failed to get event statistics: {e}")
            raise
    
    def _row_to_event(self, row) -> SignificantEvent:
        """Convert database row to SignificantEvent model."""
        return SignificantEvent(
            id=str(row['id']),
            event_type=EventType(row['event_type']),
            title=row['title'],
            description=row['description'],
            impact=row['impact'],
            metrics=json.loads(row['metrics']) if row['metrics'] else None,
            timeline=row['timeline'],
            source_messages=json.loads(row['source_messages']) if row['source_messages'] else [],
            severity=EventSeverity(row['severity']),
            stakeholders=json.loads(row['stakeholders']) if row['stakeholders'] else [],
            created_at=row['created_at'],
            is_active=row['is_active']
        )
    
    async def health_check(self) -> bool:
        """Perform health check on PostgreSQL connection."""
        if not self._initialized:
            logger.error("EventsDatabaseService not initialized")
            return False
        
        try:
            async with self.pool.acquire() as conn:
                result = await conn.fetchval("SELECT 1;")
            logger.info("PostgreSQL health check passed")
            return result == 1
        except Exception as e:
            logger.error(f"PostgreSQL health check failed: {e}")
            return False
    
    async def close(self):
        """Close the connection pool."""
        if self.pool:
            await self.pool.close()
            logger.info("PostgreSQL connection pool closed")
        self._initialized = False
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self.initialize()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()
