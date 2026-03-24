"""Flow Cost Tracking API — ORIGINAL (8 endpoints)

Tracks execution costs for Prefect flows including compute time, memory usage,
and external service costs. Enables cost allocation, budget enforcement, and
cost anomaly detection.
"""

from typing import Optional, List
from datetime import datetime, timedelta
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query, status
from pydantic import BaseModel, Field

router = APIRouter(prefix="/flow-costs", tags=["flow-costs"])


class FlowCostCreate(BaseModel):
    flow_run_id: UUID
    compute_seconds: float = Field(ge=0)
    memory_mb: float = Field(ge=0)
    external_cost_usd: float = Field(ge=0, default=0.0)
    cost_center: Optional[str] = None


class FlowCostUpdate(BaseModel):
    compute_seconds: Optional[float] = Field(None, ge=0)
    memory_mb: Optional[float] = Field(None, ge=0)
    external_cost_usd: Optional[float] = Field(None, ge=0)
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


class CostBudget(BaseModel):
    cost_center: str
    monthly_budget_usd: float = Field(gt=0)
    alert_threshold_pct: float = Field(ge=0, le=100, default=80.0)


class CostBudgetResponse(BaseModel):
    id: UUID
    cost_center: str
    monthly_budget_usd: float
    alert_threshold_pct: float
    current_spend_usd: float
    budget_remaining_usd: float
    pct_used: float
    created_at: datetime


class CostAnomaly(BaseModel):
    flow_run_id: UUID
    expected_cost_usd: float
    actual_cost_usd: float
    deviation_pct: float
    severity: str


COST_PER_COMPUTE_SECOND = 0.00012
COST_PER_MB_SECOND = 0.000002
_cost_store: dict[str, dict] = {}
_budget_store: dict[str, dict] = {}


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

    # Check budget threshold
    if body.cost_center:
        budget = _budget_store.get(body.cost_center)
        if budget:
            current_spend = sum(
                r["total_cost_usd"] for r in _cost_store.values()
                if r["cost_center"] == body.cost_center
            )
            threshold = budget["monthly_budget_usd"] * (budget["alert_threshold_pct"] / 100)
            if current_spend >= threshold:
                record["budget_warning"] = True

    return FlowCostResponse(**record)


@router.get("/{cost_id}", response_model=FlowCostResponse)
async def get_flow_cost(cost_id: UUID):
    """Retrieve a specific flow cost record."""
    record = _cost_store.get(str(cost_id))
    if not record:
        raise HTTPException(status_code=404, detail="Cost record not found")
    return FlowCostResponse(**record)


@router.put("/{cost_id}", response_model=FlowCostResponse)
async def update_flow_cost(cost_id: UUID, body: FlowCostUpdate):
    """Update fields on an existing cost record. Recalculates total cost."""
    record = _cost_store.get(str(cost_id))
    if not record:
        raise HTTPException(status_code=404, detail="Cost record not found")

    if body.compute_seconds is not None:
        record["compute_seconds"] = body.compute_seconds
    if body.memory_mb is not None:
        record["memory_mb"] = body.memory_mb
    if body.external_cost_usd is not None:
        record["external_cost_usd"] = body.external_cost_usd
    if body.cost_center is not None:
        record["cost_center"] = body.cost_center

    record["total_cost_usd"] = _calculate_total_cost(
        record["compute_seconds"], record["memory_mb"], record["external_cost_usd"],
    )
    return FlowCostResponse(**record)


@router.delete("/{cost_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_flow_cost(cost_id: UUID):
    """Delete a flow cost record."""
    if str(cost_id) not in _cost_store:
        raise HTTPException(status_code=404, detail="Cost record not found")
    del _cost_store[str(cost_id)]


@router.get("/summary/", response_model=FlowCostSummary)
async def get_cost_summary(
    cost_center: Optional[str] = None,
    since: Optional[datetime] = None,
):
    """Get aggregated cost summary with optional filters."""
    records = list(_cost_store.values())
    if cost_center:
        records = [r for r in records if r["cost_center"] == cost_center]
    if since:
        records = [r for r in records if r["recorded_at"] >= since]
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


@router.post("/budgets/", status_code=status.HTTP_201_CREATED, response_model=CostBudgetResponse)
async def create_cost_budget(body: CostBudget):
    """Create a monthly cost budget for a cost center."""
    import uuid
    if body.cost_center in _budget_store:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Budget for cost center '{body.cost_center}' already exists",
        )
    budget_id = uuid.uuid4()
    current_spend = sum(
        r["total_cost_usd"] for r in _cost_store.values()
        if r["cost_center"] == body.cost_center
    )
    record = {
        "id": budget_id,
        "cost_center": body.cost_center,
        "monthly_budget_usd": body.monthly_budget_usd,
        "alert_threshold_pct": body.alert_threshold_pct,
        "current_spend_usd": current_spend,
        "budget_remaining_usd": max(body.monthly_budget_usd - current_spend, 0),
        "pct_used": round((current_spend / body.monthly_budget_usd) * 100, 2) if body.monthly_budget_usd else 0,
        "created_at": datetime.utcnow(),
    }
    _budget_store[body.cost_center] = record
    return CostBudgetResponse(**record)


@router.get("/budgets/{cost_center}", response_model=CostBudgetResponse)
async def get_cost_budget(cost_center: str):
    """Get the budget status for a cost center."""
    record = _budget_store.get(cost_center)
    if not record:
        raise HTTPException(status_code=404, detail="Budget not found for this cost center")
    # Recalculate current spend
    current_spend = sum(
        r["total_cost_usd"] for r in _cost_store.values()
        if r["cost_center"] == cost_center
    )
    record["current_spend_usd"] = current_spend
    record["budget_remaining_usd"] = max(record["monthly_budget_usd"] - current_spend, 0)
    record["pct_used"] = round((current_spend / record["monthly_budget_usd"]) * 100, 2)
    return CostBudgetResponse(**record)


@router.get("/anomalies/", response_model=List[CostAnomaly])
async def detect_cost_anomalies(
    threshold_pct: float = Query(default=200.0, ge=100, description="Deviation threshold percentage"),
):
    """Detect cost anomalies by comparing each flow run cost to the rolling average."""
    records = sorted(_cost_store.values(), key=lambda r: r["recorded_at"])
    if len(records) < 3:
        return []

    anomalies = []
    for i, record in enumerate(records):
        if i < 2:
            continue
        prior = records[:i]
        avg_cost = sum(r["total_cost_usd"] for r in prior) / len(prior)
        if avg_cost == 0:
            continue
        deviation = ((record["total_cost_usd"] - avg_cost) / avg_cost) * 100
        if abs(deviation) >= threshold_pct:
            severity = "critical" if abs(deviation) >= 500 else "warning"
            anomalies.append(CostAnomaly(
                flow_run_id=record["flow_run_id"],
                expected_cost_usd=round(avg_cost, 6),
                actual_cost_usd=record["total_cost_usd"],
                deviation_pct=round(deviation, 2),
                severity=severity,
            ))
    return anomalies
