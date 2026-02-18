from datetime import datetime, date
from pydantic import BaseModel, Field
from typing import Any, List

class SyncDataDetail(BaseModel):
    total_fetched: int
    inserted: int
    skipped: int

class SyncDataResponse(BaseModel):
    status: str
    carlistmy: SyncDataDetail
    mudahmy: SyncDataDetail

class BrandCount(BaseModel):
    brand: str
    count: int

class APIKeyCreateRequest(BaseModel):
    client_name: str = Field(..., example="Client A")
    rate_limit: int = Field(..., example=1000)
    purpose: str = Field(..., example="testing")

class APIKeyCreateResponse(BaseModel):
    id: int
    client_name: str
    api_key: str
    rate_limit: int
    purpose: str

class ColumnDifference(BaseModel):
    id: int
    column: str
    local_value: Any
    remote_value: Any

class CarsStandardComparisonResult(BaseModel):
    status: str
    checked_at: datetime
    local_count: int
    remote_count: int
    missing_in_local: List[int]
    missing_in_remote: List[int]
    differences: List[ColumnDifference]

class CarsStandardSyncResult(BaseModel):
    direction: str
    table: str
    synced_ids: List[int]
    missing_in_source: List[int]
    total_synced: int


class TelegramSourceMetric(BaseModel):
    source: str
    today_count: int
    all_time_count: int


class TelegramTotals(BaseModel):
    total_today: int
    total_all: int
    unique_today: int
    unique_all: int


class TelegramDailyMetricsResponse(BaseModel):
    report_date: date
    sources: List[str]
    rows: List[TelegramSourceMetric]
    totals: TelegramTotals
