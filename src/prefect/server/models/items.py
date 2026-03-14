from typing import Optional, Sequence
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from prefect.server.database import PrefectDBInterface, db_injector, orm_models
from prefect.server.schemas.actions import ItemCreate, ItemUpdate


@db_injector
async def create_item(
    db: PrefectDBInterface, session: AsyncSession, item: ItemCreate
) -> orm_models.Item:
    """Create an item."""
    model = db.Item(**item.model_dump())
    session.add(model)
    await session.flush()
    return model


@db_injector
async def read_item(
    db: PrefectDBInterface, session: AsyncSession, item_id: UUID
) -> Optional[orm_models.Item]:
    """Read an item by id."""
    query = sa.select(db.Item).where(db.Item.id == item_id)
    result = await session.execute(query)
    return result.scalar()


@db_injector
async def read_items(
    db: PrefectDBInterface,
    session: AsyncSession,
    offset: Optional[int] = None,
    limit: Optional[int] = None,
) -> Sequence[orm_models.Item]:
    """Read items with optional pagination."""
    query = sa.select(db.Item).order_by(db.Item.name.asc())

    if offset is not None:
        query = query.offset(offset)
    if limit is not None:
        query = query.limit(limit)

    result = await session.execute(query)
    return result.scalars().unique().all()


@db_injector
async def count_items(
    db: PrefectDBInterface,
    session: AsyncSession,
) -> int:
    """Count items."""
    query = sa.select(sa.func.count()).select_from(db.Item)
    result = await session.execute(query)
    return result.scalar_one()


@db_injector
async def update_item(
    db: PrefectDBInterface,
    session: AsyncSession,
    item_id: UUID,
    item: ItemUpdate,
) -> bool:
    """Update an item by id."""
    query = (
        sa.update(db.Item)
        .where(db.Item.id == item_id)
        .values(**item.model_dump_for_orm(exclude_unset=True))
    )
    result = await session.execute(query)
    return result.rowcount > 0


@db_injector
async def delete_item(
    db: PrefectDBInterface, session: AsyncSession, item_id: UUID
) -> bool:
    """Delete an item by id."""
    query = sa.delete(db.Item).where(db.Item.id == item_id)
    result = await session.execute(query)
    return result.rowcount > 0
