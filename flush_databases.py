#!/usr/bin/env python3
"""Script to flush all databases before running main.py"""

import asyncio
import redis
import asyncpg
from cassandra.cluster import Cluster


def flush_redis():
    try:
        client = redis.Redis(host='localhost', port=6379, db=0)
        client.flushall()
        print("✓ Redis: FLUSHED ALL DATA")
        client.close()
    except Exception as e:
        print(f"✗ Redis: Failed to flush - {e}")


def flush_cassandra():
    try:
        cluster = Cluster(['localhost'], port=9042)
        session = cluster.connect()
        session.execute('DROP KEYSPACE IF EXISTS message_queue_db;')
        print("✓ Cassandra: Dropped keyspace 'message_queue_db'")
        cluster.shutdown()
    except Exception as e:
        print(f"✗ Cassandra: Failed to flush - {e}")


async def flush_postgres():
    try:
        conn = await asyncpg.connect(
            host='localhost',
            port=5432,
            database='events_db',
            user='postgres',
            password='postgres'
        )
        await conn.execute('TRUNCATE TABLE significant_events;')
        print("✓ PostgreSQL: Truncated 'significant_events' table")
        await conn.close()
    except Exception as e:
        print(f"✗ PostgreSQL: Failed to flush - {e}")


def main():
    print("=" * 50)
    print("FLUSHING ALL DATABASES")
    print("=" * 50)
    
    flush_redis()
    flush_cassandra()
    asyncio.run(flush_postgres())
    
    print("=" * 50)
    print("DATABASE FLUSH COMPLETE")
    print("=" * 50)


if __name__ == "__main__":
    main()
