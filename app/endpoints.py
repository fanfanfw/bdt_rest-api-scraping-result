from fastapi import APIRouter, HTTPException
from app.services import fetch_brands_models_variants_by_source, sync_data_from_remote, get_price_rank
from app.models import BrandsModelsVariantsResponse, ResponseMessage, RankPriceResponse, RankPriceRequest

router = APIRouter()

@router.post("/cars/brands_models_variants", response_model=BrandsModelsVariantsResponse, description="Fetch available brands, models, and variants based on the source.")
async def get_brands_models_variants(request: dict):
    source = request.get('source')
    try:
        data = await fetch_brands_models_variants_by_source(source)
        return {"brands_models_variants": data}
    except HTTPException as e:
        raise e


@router.post("/sync_data", response_model=ResponseMessage, include_in_schema=False)
async def sync_data():
    result = await sync_data_from_remote()
    return result

@router.post("/cars/rank_price", response_model=RankPriceResponse)
async def rank_price(request: RankPriceRequest):  
    try:
        result = await get_price_rank(request.dict())
        return result
    except HTTPException as e:
        raise e