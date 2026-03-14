"""
Routes for interacting with item objects
"""

from typing import List
from uuid import UUID

import sqlalchemy as sa
from fastapi import Body, Depends, HTTPException, Path, status
from sqlalchemy.ext.asyncio import AsyncSession

from prefect.server import models
from prefect.server.api.dependencies import LimitBody
from prefect.server.database import (
    PrefectDBInterface,
    orm_models,
    provide_database_interface,
)
from prefect.server.schemas import actions, core
from prefect.server.utilities.server import PrefectRouter


async def get_item_or_404(
    session: AsyncSession, item_id: UUID
) -> orm_models.Item:
    """Returns an item or raises 404 HTTPException if it does not exist"""
    item = await models.items.read_item(session=session, item_id=item_id)
    if not item:
        raise HTTPException(status_code=404, detail="Item not found.")
    return item


router: PrefectRouter = PrefectRouter(
    prefix="/items",
    tags=["Items"],
)


@router.post("/", status_code=status.HTTP_201_CREATED)
async def create_item(
    item: actions.ItemCreate,
    db: PrefectDBInterface = Depends(provide_database_interface),
) -> core.Item:
    """Create an item."""
    async with db.session_context(begin_transaction=True) as session:
        try:
            model = await models.items.create_item(session=session, item=item)
        except sa.exc.IntegrityError:
            raise HTTPException(
                status_code=409,
                detail=f"An item with the name {item.name!r} already exists.",
            )
    return core.Item.model_validate(model, from_attributes=True)


@router.get("/{id:uuid}")
async def read_item(
    item_id: UUID = Path(..., alias="id"),
    db: PrefectDBInterface = Depends(provide_database_interface),
) -> core.Item:
    """Read an item by id."""
    async with db.session_context() as session:
        model = await get_item_or_404(session=session, item_id=item_id)
    return core.Item.model_validate(model, from_attributes=True)


@router.post("/filter")
async def read_items(
    limit: int = LimitBody(),
    offset: int = Body(0, ge=0),
    db: PrefectDBInterface = Depends(provide_database_interface),
) -> List[core.Item]:
    """Read items with optional pagination."""
    async with db.session_context() as session:
        return await models.items.read_items(
            session=session,
            offset=offset,
            limit=limit,
        )


@router.post("/count")
async def count_items(
    db: PrefectDBInterface = Depends(provide_database_interface),
) -> int:
    """Count items."""
    async with db.session_context() as session:
        return await models.items.count_items(session=session)


@router.patch("/{id:uuid}", status_code=status.HTTP_204_NO_CONTENT)
async def update_item(
    item: actions.ItemUpdate,
    item_id: UUID = Path(..., alias="id"),
    db: PrefectDBInterface = Depends(provide_database_interface),
) -> None:
    """Update an item by id."""
    async with db.session_context(begin_transaction=True) as session:
        updated = await models.items.update_item(
            session=session,
            item_id=item_id,
            item=item,
        )
    if not updated:
        raise HTTPException(status_code=404, detail="Item not found.")


@router.delete("/{id:uuid}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_item(
    item_id: UUID = Path(..., alias="id"),
    db: PrefectDBInterface = Depends(provide_database_interface),
) -> None:
    """Delete an item by id."""
    async with db.session_context(begin_transaction=True) as session:
        deleted = await models.items.delete_item(
            session=session, item_id=item_id
        )
    if not deleted:
        raise HTTPException(status_code=404, detail="Item not found.")
