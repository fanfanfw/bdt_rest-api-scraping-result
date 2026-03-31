from datetime import datetime, date
from pydantic import BaseModel, Field
from typing import Any, List, Optional

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


class DashboardCompetitorResultRow(BaseModel):
    competitor: Optional[str] = None
    source: str
    location: Optional[str] = None
    model: str
    listed_price: Optional[int] = None
    vs_your_price: Optional[int] = None
    distance: Optional[int] = None
    last_updated: Optional[datetime] = None


class DashboardCompetitorResultMeta(BaseModel):
    total: int
    limit: int
    offset: int
    months: int


class DashboardCompetitorResultSummary(BaseModel):
    competitor_count: int
    market_average: Optional[float] = None
    your_price: Optional[int] = None
    gap: Optional[float] = None


class DashboardCompetitorResult(BaseModel):
    filters: dict
    summary: DashboardCompetitorResultSummary
    meta: DashboardCompetitorResultMeta
    data: List[DashboardCompetitorResultRow]


class DashboardCompetitorBulkItemInput(BaseModel):
    brand: Optional[str] = Field(None, example="TOYOTA")
    model: Optional[str] = Field(None, example="YARIS")
    variant: Optional[str] = Field(None, example="E")
    year: Optional[int] = Field(None, example=2020)
    your_price: Optional[int] = Field(None, example=65500)


class DashboardCompetitorBulkRequest(BaseModel):
    source: Optional[str] = Field(None, example="mudahmy")
    status: Optional[str] = Field(None, example="active,sold")
    months: int = Field(1, ge=1, le=24, example=1)
    limit: int = Field(10, ge=1, le=100, example=10)
    offset: int = Field(0, ge=0, example=0)
    items: List[DashboardCompetitorBulkItemInput]


class DashboardCompetitorBulkMeta(BaseModel):
    total: int
    months: int
    limit: int
    offset: int
    source: Optional[str] = None
    status: List[str]


class DashboardCompetitorBulkResultItem(BaseModel):
    result: DashboardCompetitorResult


class DashboardCompetitorBulkResponse(BaseModel):
    meta: DashboardCompetitorBulkMeta
    data: List[DashboardCompetitorBulkResultItem]


class InventoryPriceMonitorItemInput(BaseModel):
    brand: str = Field(..., example="TOYOTA")
    model: str = Field(..., example="YARIS")
    variant: str = Field(..., example="E")
    year: int = Field(..., example=2020)
    your_price: Optional[int] = Field(None, example=65500)


class InventoryPriceMonitorRequest(BaseModel):
    source: Optional[str] = Field(None, example="mudahmy")
    status: Optional[str] = Field(None, example="active,sold")
    months: int = Field(1, ge=1, le=24, example=1)
    items: List[InventoryPriceMonitorItemInput]


class InventoryPriceMonitorMeta(BaseModel):
    total: int
    months: int
    source: Optional[str] = None
    status: List[str]


class InventoryPriceMonitorResultItem(BaseModel):
    brand: str
    model: str
    variant: str
    year: int
    vehicle: str
    your_price: Optional[int] = None
    market_average: Optional[float] = None
    gap: Optional[float] = None
    competitor_count: int


class InventoryPriceMonitorResponse(BaseModel):
    meta: InventoryPriceMonitorMeta
    data: List[InventoryPriceMonitorResultItem]


class DashboardDetailPriceResponse(BaseModel):
    source: Optional[str] = None
    brand: str
    model: str
    variant: str
    year: int
    your_price: int
    data_points: int
    lowest_price: Optional[int] = None
    average_price: Optional[float] = None
    highest_price: Optional[int] = None
    last_updated: Optional[datetime] = None
