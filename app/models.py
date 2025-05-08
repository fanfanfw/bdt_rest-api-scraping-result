from pydantic import BaseModel, Field
from typing import List

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