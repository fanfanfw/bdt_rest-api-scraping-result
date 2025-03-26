from typing import Optional, List
from fastapi import APIRouter, HTTPException, Query
from app.services import (
    fetch_brands_models_variants_by_source, sync_data_from_remote, get_price_rank_carlistmy, 
    get_price_rank_mudahmy, search_cars_mudahmy, search_cars_carlistmy,get_brand_distribution_mudahmy,
    get_price_summary_mudahmy, get_top_locations_mudahmy,
    get_brand_distribution_carlistmy, get_price_summary_carlistmy, get_top_locations_carlistmy
)
from app.models import (
    BrandsModelsVariantsResponse, ResponseMessage, RankPriceResponse, 
    RankPriceRequest, SourceRequest, SearchCarsResponse, SearchCarsCarlistMyResponse,
    BrandCount, PriceSummary, LocationCount
)
router = APIRouter()

@router.post(
    "/cars/brands_models_variants", 
    response_model=BrandsModelsVariantsResponse,
    description="Fetch available brands, models, and variants based on the source. souurce : mudahmy or source : carlistmy"
)
async def get_brands_models_variants(request: SourceRequest):
    """
    Endpoint ini akan mengambil data brand, model, dan varian mobil berdasarkan source.
    """
    source = request.source
    try:
        data = await fetch_brands_models_variants_by_source(source)
        return {"brands_models_variants": data}
    except HTTPException as e:
        raise e

@router.post("/sync_data", response_model=ResponseMessage, include_in_schema=False)
async def sync_data():
    result = await sync_data_from_remote()
    return result

@router.post("/cars/rank_price_carlistmy",
             response_model=RankPriceResponse,
             description="car ranking From Carlist site based on brand, model, variant, and year. then enter the price you want to sell in integer format."
             )
async def rank_price(request: RankPriceRequest):
    """
    Endpoint untuk menghitung ranking berdasarkan price dan millage.
    """
    try:
        result = await get_price_rank_carlistmy(request.dict())
        return result
    except HTTPException as e:
        raise e
    
@router.post(
    "/cars/rank_price_mudahmy",
    response_model=RankPriceResponse,
    description="Car ranking from MudahMY site based on brand, model, variant, and year. Then enter the price in integer format."
)
async def rank_price_mudahmy(request: RankPriceRequest):
    """
    Endpoint untuk menghitung ranking berdasarkan price dan millage di tabel `cars_mudahmy`.
    """
    try:
        return await get_price_rank_mudahmy(request.dict())
    except HTTPException as e:
        raise e
    
@router.get(
    "/cars/search_mudahmy",
    response_model=SearchCarsResponse,
    description="Mencari mobil di tabel cars_mudahmy dengan filter dinamis dan pagination."
)
async def search_mudahmy(
    brand: Optional[str] = None,
    model: Optional[str] = None,
    variant: Optional[str] = None,
    min_price: Optional[int] = None,
    max_price: Optional[int] = None,
    year: Optional[int] = None,
    location: Optional[str] = None,
    page: int = 1,
    size: int = 10,
):
    """
    Endpoint untuk mencari mobil di tabel `cars_mudahmy` berdasarkan filter:
    - brand, model, variant (exact match)
    - min_price, max_price
    - year
    - location (partial match)
    - pagination (page, size)
    """
    result = await search_cars_mudahmy(
        brand=brand,
        model=model,
        variant=variant,
        min_price=min_price,
        max_price=max_price,
        year=year,
        location=location,
        page=page,
        size=size
    )
    return result

@router.get(
    "/cars/search_carlistmy",
    response_model=SearchCarsCarlistMyResponse,
    description="Mencari mobil di tabel cars_carlistmy dengan filter dinamis dan pagination."
)
async def search_carlistmy(
    brand: Optional[str] = Query(None, description="Filter berdasarkan brand (ILIKE)"),
    model: Optional[str] = Query(None, description="Filter berdasarkan model (exact match)"),
    variant: Optional[str] = Query(None, description="Filter berdasarkan varian (exact match)"),
    min_price: Optional[int] = Query(None, description="Harga minimum"),
    max_price: Optional[int] = Query(None, description="Harga maksimum"),
    year: Optional[int] = Query(None, description="Tahun pembuatan"),
    location: Optional[str] = Query(None, description="Filter lokasi (partial match)"),
    page: int = 1,
    size: int = 10
):
    """
    Endpoint untuk mencari mobil di tabel `cars_carlistmy` berdasarkan filter:
    - brand, model, variant
    - min_price, max_price
    - year
    - location
    - pagination (page, size)
    """
    result = await search_cars_carlistmy(
        brand=brand,
        model=model,
        variant=variant,
        min_price=min_price,
        max_price=max_price,
        year=year,
        location=location,
        page=page,
        size=size
    )
    return result

@router.get(
    "/analytics/mudahmy/brand_distribution",
    response_model=List[BrandCount],
    description="Menampilkan jumlah listing per-brand di cars_mudahmy."
)
async def brand_distribution_mudahmy():
    """
    Endpoint untuk mendapatkan jumlah listing per brand.
    Urutkan dari yang terbanyak ke yang paling sedikit.
    """
    return await get_brand_distribution_mudahmy()

@router.get(
    "/analytics/mudahmy/price_summary",
    response_model=PriceSummary,
    description="Menampilkan ringkasan statistik harga (min, max, avg, median) di cars_mudahmy dengan filter opsional, plus total listing."
)
async def price_summary_mudahmy(
    brand: Optional[str] = Query(None, description="Filter brand (exact match)"),
    model: Optional[str] = Query(None, description="Filter model (exact match)"),
    variant: Optional[str] = Query(None, description="Filter variant (exact match)"),
    year: Optional[int] = Query(None, description="Filter tahun (exact match)")
):
    """
    Endpoint untuk menampilkan ringkasan statistik harga di tabel `cars_mudahmy`.
    Dapat memfilter berdasarkan brand, model, variant, year.
    Menyertakan total_listing agar tahu berapa jumlah listing yang ter-filter.
    """
    result = await get_price_summary_mudahmy(
        brand=brand,
        model=model,
        variant=variant,
        year=year
    )
    return result

@router.get(
    "/analytics/mudahmy/top_locations",
    response_model=List[LocationCount],
    description="Menampilkan lokasi teratas yang paling banyak listing di cars_mudahmy."
)
async def top_locations_mudahmy(limit: int = Query(10, description="Jumlah lokasi teratas")):
    """
    Endpoint untuk mendapatkan daftar lokasi dengan listing terbanyak.
    """
    return await get_top_locations_mudahmy(limit)

@router.get(
    "/analytics/carlistmy/brand_distribution",
    response_model=List[BrandCount],
    description="Menampilkan jumlah listing per-brand di cars_carlistmy."
)
async def brand_distribution_carlistmy():
    """
    Endpoint untuk mendapatkan jumlah listing per brand di `cars_carlistmy`.
    Urutkan dari yang terbanyak ke yang paling sedikit.
    """
    return await get_brand_distribution_carlistmy()


@router.get(
    "/analytics/carlistmy/price_summary",
    response_model=PriceSummary,
    description="Menampilkan ringkasan statistik harga di cars_carlistmy (min, max, avg, median, total_listing), dengan filter opsional."
)
async def price_summary_carlistmy(
    brand: Optional[str] = Query(None, description="Filter brand (exact match)"),
    model: Optional[str] = Query(None, description="Filter model (exact match)"),
    variant: Optional[str] = Query(None, description="Filter variant (exact match)"),
    year: Optional[int] = Query(None, description="Filter tahun (exact match)")
):
    """
    Endpoint untuk menampilkan ringkasan statistik harga di tabel `cars_carlistmy`.
    Bisa memfilter berdasarkan brand, model, variant, year.
    """
    return await get_price_summary_carlistmy(
        brand=brand,
        model=model,
        variant=variant,
        year=year
    )


@router.get(
    "/analytics/carlistmy/top_locations",
    response_model=List[LocationCount],
    description="Menampilkan lokasi teratas dengan jumlah listing terbanyak di cars_carlistmy."
)
async def top_locations_carlistmy(limit: int = Query(10, description="Jumlah lokasi teratas")):
    """
    Endpoint untuk mendapatkan daftar lokasi dengan listing terbanyak di `cars_carlistmy`.
    """
    return await get_top_locations_carlistmy(limit)