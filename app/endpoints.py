from typing import Optional, List
from fastapi import APIRouter, HTTPException, Query, Request
from app.services import (
    fetch_brands_models_variants_by_source, sync_data_from_remote, get_price_rank_carlistmy,
    get_price_rank_mudahmy,
    get_top_locations_carlistmy, get_top_price_drops, get_price_drop_top,
    get_top_locations_by_brand, get_brand_model_distribution, get_available_brands_models, get_optimal_price_recommendations,
    get_price_vs_millage_filtered, get_all_dropdown_options
)
from app.models import (
    BrandsModelsVariantsResponse, ResponseMessage, RankPriceResponse,
    RankCarResponse, RankPriceRequest, SourceRequest,
    BrandCount, LocationCount, PriceDropItem, OptimalPriceItem
)
from app.services import get_cars_for_datatables
from app.database import get_local_db_connection
router = APIRouter()
# app.include_router(router)
# app.include_router(router, prefix="/api")

@router.get(
    "/analytics/{source}/dropdown_options",
    description="Mengembalikan data brand, model, variant, dan year dalam satu endpoint untuk kebutuhan dropdown",
    tags=["Dropdown Support"]
)
async def dropdown_options(
    source: str,
    brand: Optional[str] = Query(None),
    model: Optional[str] = Query(None),
    variant: Optional[str] = Query(None)
):
    return await get_all_dropdown_options(source, brand, model, variant)

@router.get(
    "/analytics/{source}/price_vs_millage_filtered",
    response_model=List[dict],
    description="Menampilkan price vs millage berdasarkan filter brand, model, variant, dan year (opsional)",
    tags=["Analytics Umum"]
)
async def price_vs_millage_filtered(
    source: str,
    brand: str = Query(...),
    model: str = Query(...),
    variant: Optional[str] = Query(None),
    year: Optional[int] = Query(None),
):
    return await get_price_vs_millage_filtered(source, brand, model, variant, year)


@router.get("/analytics/{source}/optimal_price_recommendations", response_model=List[OptimalPriceItem])
async def optimal_price_recommendations(source: str):
    if source not in ["carlistmy", "mudahmy"]:
        raise HTTPException(status_code=400, detail="Invalid source. Use 'carlistmy' or 'mudahmy'")

    try:
        data = await get_optimal_price_recommendations(source)
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
@router.get(
    "/analytics/{source}/available_brands_models",
    description="Menampilkan daftar brand dan model yang tersedia untuk dropdown filter lokasi",
    tags=["Dropdown Support"]
)
async def available_brands_models(source: str):
    return await get_available_brands_models(source)

@router.get("/analytics/{source}/brand_model_distribution", response_model=List[BrandCount], tags=["Analytics Umum"])
async def brand_model_distribution(source: str):
    return await get_brand_model_distribution(source)

@router.get("/analytics/{source}/top_locations_by_brand", response_model=List[LocationCount], tags=["Analytics Umum"])
async def top_locations_by_brand(source: str, brand: str, model: Optional[str] = None):
    return await get_top_locations_by_brand(source, brand, model)

@router.get("/analytics/{source}/price_drop_top", response_model=List[PriceDropItem], tags=["Analytics Umum"])
async def top_price_drop(source: str, limit: int = 10):
    return await get_top_price_drops(source, limit)

@router.get(
    "/analytics/{source}/price_drop_top",
    response_model=List[RankCarResponse],
    description="Menampilkan 10 penurunan harga terbesar dari listing aktif",
    tags=["Analytics Umum"]
)
async def price_drop_top(source: str, limit: int = 10):
    return await get_price_drop_top(source, limit)

@router.get("/analytics/{source}/price_vs_millage", response_model=List[dict])
async def price_vs_millage(source: str):
    if source not in ["mudahmy", "carlistmy"]:
        raise HTTPException(status_code=400, detail="Source must be 'mudahmy' or 'carlistmy'")

    table = f"cars_{source}"
    query = f"""
        SELECT price, millage FROM {table}
        WHERE price IS NOT NULL AND millage IS NOT NULL
    """

    conn = await get_local_db_connection()
    try:
        rows = await conn.fetch(query)
        return [{"price": row["price"], "millage": row["millage"]} for row in rows]
    finally:
        await conn.close()

@router.get("/analytics/summary_count")
async def summary_count(source: str = Query(...)):
    conn = await get_local_db_connection()
    try:
        table = "cars_mudahmy" if source == "mudahmy" else "cars_carlistmy"
        total = await conn.fetchval(f"SELECT COUNT(*) FROM {table}")
        active = await conn.fetchval(f"SELECT COUNT(*) FROM {table} WHERE status = 'available'")
        sold = await conn.fetchval(f"SELECT COUNT(*) FROM {table} WHERE status = 'sold'")
        return {
            "total": total,
            "active": active,
            "sold": sold
        }
    finally:
        await conn.close()

@router.get("/cars/datatables")
async def datatables_server_side(
    request: Request,
    source: str = Query(...),
    draw: int = Query(1),
    start: int = Query(0),
    length: int = Query(10),
    search: str = Query("", alias="search[value]"),
    order_col: int = Query(0, alias="order[0][column]"),
    order_dir: str = Query("asc", alias="order[0][dir]")
):
    try:
        total, filtered, data = await get_cars_for_datatables(
            source=source,
            start=start,
            length=length,
            search=search,
            order_column=order_col,
            order_dir=order_dir
        )

        return {
            "draw": draw,
            "recordsTotal": total,
            "recordsFiltered": filtered,
            "data": data
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")

@router.post(
    "/cars/brands_models_variants",
    response_model=BrandsModelsVariantsResponse,
    description="Fetch available brands, models, and variants based on the source. souurce : mudahmy or source : carlistmy",
    tags=["Data Access"]
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
             description="car ranking From Carlist site based on brand, model, variant, and year. then enter the price you want to sell in integer format.",
             tags=["Ranking / Analysis"]
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
    description="Car ranking from MudahMY site based on brand, model, variant, and year. Then enter the price in integer format.",
    tags=["Ranking / Analysis"]
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
    "/analytics/carlistmy/top_locations",
    response_model=List[LocationCount],
    description="Showing top locations with the most listings on carlistmy.",
    tags=["Analytics Umum"]
)
async def top_locations_carlistmy(limit: int = Query(10, description="Jumlah lokasi teratas")):
    """
    Endpoint untuk mendapatkan daftar lokasi dengan listing terbanyak di `cars_carlistmy`.
    """
    return await get_top_locations_carlistmy(limit)