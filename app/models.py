from pydantic import BaseModel
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