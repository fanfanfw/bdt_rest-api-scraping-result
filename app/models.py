from pydantic import BaseModel
from typing import List, Optional

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


class CarMudahMy(BaseModel):
    id: int
    brand: str
    model: str
    variant: str
    price: int
    millage: int
    year: int
    lokasi: Optional[str] = None

class SearchCarsResponse(BaseModel):
    page: int
    size: int
    total_pages: int
    total_items: int
    data: List[CarMudahMy]

class CarCarlistMy(BaseModel):
    id: int
    brand: str
    model: str
    variant: str
    price: int
    millage: int
    year: int
    lokasi: Optional[str] = None  # Pastikan kolom 'lokasi' memang ada di tabel cars_carlistmy

class SearchCarsCarlistMyResponse(BaseModel):
    page: int
    size: int
    total_pages: int
    total_items: int
    data: List[CarCarlistMy]

class BrandCount(BaseModel):
    brand: str
    count: int

class PriceSummary(BaseModel):
    total_listing: Optional[int]
    min_price: Optional[float]
    max_price: Optional[float]
    avg_price: Optional[float]
    median_price: Optional[float]

class LocationCount(BaseModel):
    location: str
    count: int

class BrandCount(BaseModel):
    brand: str
    count: int

class PriceSummary(BaseModel):
    total_listing: Optional[int]
    min_price: Optional[float]
    max_price: Optional[float]
    avg_price: Optional[float]
    median_price: Optional[float]

class LocationCount(BaseModel):
    location: str
    count: int
