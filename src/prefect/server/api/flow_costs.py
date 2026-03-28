"""Flow Cost Tracking API — SIMPLIFIED (3 endpoints)

Tracks execution costs for Prefect flows including compute time, memory usage,
and external service costs. Enables cost allocation and budget enforcement.
"""

from typing import Optional
from datetime import datetime
from uuid import UUID

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field

router = APIRouter(prefix="/flow-costs", tags=["flow-costs"])


class FlowCostCreate(BaseModel):
    flow_run_id: UUID
    compute_seconds: float = Field(ge=0)
    memory_mb: float = Field(ge=0)
    external_cost_usd: float = Field(ge=0, default=0.0)
    cost_center: Optional[str] = None


class FlowCostResponse(BaseModel):
    id: UUID
    flow_run_id: UUID
    compute_seconds: float
    memory_mb: float
    external_cost_usd: float
    total_cost_usd: float
    cost_center: Optional[str]
    recorded_at: datetime


class FlowCostSummary(BaseModel):
    total_records: int
    total_compute_seconds: float
    total_memory_mb: float
    total_external_cost_usd: float
    total_cost_usd: float
    avg_cost_per_run: float


COST_PER_COMPUTE_SECOND = 0.00012
COST_PER_MB_SECOND = 0.000002
_cost_store: dict[str, dict] = {}


def _calculate_total_cost(compute_seconds: float, memory_mb: float, external_cost_usd: float) -> float:
    compute_cost = compute_seconds * COST_PER_COMPUTE_SECOND
    memory_cost = memory_mb * COST_PER_MB_SECOND * compute_seconds
    return round(compute_cost + memory_cost + external_cost_usd, 6)


@router.post("/", status_code=status.HTTP_201_CREATED, response_model=FlowCostResponse)
async def record_flow_cost(body: FlowCostCreate):
    """Record execution cost for a flow run."""
    import uuid
    cost_id = uuid.uuid4()
    total = _calculate_total_cost(body.compute_seconds, body.memory_mb, body.external_cost_usd)
    record = {
        "id": cost_id,
        "flow_run_id": body.flow_run_id,
        "compute_seconds": body.compute_seconds,
        "memory_mb": body.memory_mb,
        "external_cost_usd": body.external_cost_usd,
        "total_cost_usd": total,
        "cost_center": body.cost_center,
        "recorded_at": datetime.utcnow(),
    }
    _cost_store[str(cost_id)] = record
    return FlowCostResponse(**record)


@router.get("/{cost_id}", response_model=FlowCostResponse)
async def get_flow_cost(cost_id: UUID):
    """Retrieve a specific flow cost record."""
    record = _cost_store.get(str(cost_id))
    if not record:
        raise HTTPException(status_code=404, detail="Cost record not found")
    return FlowCostResponse(**record)


@router.get("/summary/", response_model=FlowCostSummary)
async def get_cost_summary(cost_center: Optional[str] = None):
    """Get aggregated cost summary, optionally filtered by cost center."""
    records = list(_cost_store.values())
    if cost_center:
        records = [r for r in records if r["cost_center"] == cost_center]
    if not records:
        return FlowCostSummary(
            total_records=0, total_compute_seconds=0, total_memory_mb=0,
            total_external_cost_usd=0, total_cost_usd=0, avg_cost_per_run=0,
        )
    return FlowCostSummary(
        total_records=len(records),
        total_compute_seconds=sum(r["compute_seconds"] for r in records),
        total_memory_mb=sum(r["memory_mb"] for r in records),
        total_external_cost_usd=sum(r["external_cost_usd"] for r in records),
        total_cost_usd=sum(r["total_cost_usd"] for r in records),
        avg_cost_per_run=sum(r["total_cost_usd"] for r in records) / len(records),
    )
