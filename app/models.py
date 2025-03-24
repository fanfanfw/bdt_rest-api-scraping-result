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