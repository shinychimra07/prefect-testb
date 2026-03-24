#!/usr/bin/env python3
"""
Ensure all ORM model tables exist in the database.
This script runs after migrations to create any tables defined in ORM models
that don't have corresponding migrations yet (useful for testbot PRs).
"""

import asyncio
import sys
from sqlalchemy.ext.asyncio import create_async_engine
from prefect.server.database.orm_models import Base

async def ensure_tables():
    """Create all database tables from ORM models if they don't exist"""
    try:
        engine = create_async_engine(
            "postgresql+asyncpg://prefect:prefect@postgres:5432/prefect",
            echo=False
        )

        async with engine.begin() as conn:
            # Create all tables defined in ORM models (idempotent - won't recreate existing tables)
            await conn.run_sync(Base.metadata.create_all)

        await engine.dispose()
        print("✓ All ORM model tables ensured in database")
        return 0
    except Exception as e:
        print(f"✗ Error ensuring tables: {e}", file=sys.stderr)
        return 1

if __name__ == "__main__":
    sys.exit(asyncio.run(ensure_tables()))
