from pydantic import BaseModel
from typing import List, Optional

class NewClientRequest(BaseModel):
    client_name: str
    rate_limit: Optional[int] = 1000  # default jika tidak dikirim

class BrandModelVariantResponse(BaseModel):
    brand: str
    model: str
    variant: str

class BrandsModelsVariantsResponse(BaseModel):
    brands_models_variants: List[BrandModelVariantResponse]

class ResponseMessage(BaseModel):
    status: str

class RankCarResponse(BaseModel):
    id: int
    brand: str
    model: str
    variant: str
    price: int
    millage: int
    year: int
    ranking: int

class RankPriceResponse(BaseModel):
    user_rank: int
    message: str
    top_5: Optional[List[RankCarResponse]] = None
    bottom_5: Optional[List[RankCarResponse]] = None
    all_data: Optional[List[RankCarResponse]] = None 

class RankPriceRequest(BaseModel):
    brand: str
    model: str
    variant: str
    price: int
    millage: int
    year: int

class SourceRequest(BaseModel):
    source: str

class BrandCount(BaseModel):
    brand: str
    count: int

class LocationCount(BaseModel):
    location: str
    count: int

class BrandCount(BaseModel):
    brand: str
    count: int

class LocationCount(BaseModel):
    location: str
    count: int

class PriceDropItem(BaseModel):
    car_id: int
    brand: str
    model: str
    variant: str
    old_price: int
    new_price: int
    drop_amount: int
    changed_at: str

class OptimalPriceItem(BaseModel):
    brand: str
    model: str
    variant: Optional[str]
    jumlah_iklan: int
    rata_rata_harga: float
    median_harga: float
    harga_terendah: float
    harga_tertinggi: float

