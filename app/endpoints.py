from typing import Optional, List
from fastapi import APIRouter, HTTPException, Query, Header, status
from fastapi.responses import StreamingResponse
from app.services import (
    get_price_vs_mileage_filtered, generate_scatter_plot,
    create_api_key, clear_rate_limit, get_brands_list, get_models_list,
    get_variants_list, get_years_list, get_car_records, get_car_detail,
    get_statistics, get_today_data_count, get_price_estimation, get_brand_car_counts,
    get_telegram_daily_metrics,
)
from app.database import get_local_db_connection
from typing import Optional
from app.models import (
    APIKeyCreateRequest,
    APIKeyCreateResponse,
    TelegramDailyMetricsResponse,
)
import os
from datetime import datetime

router = APIRouter()
admin_router = APIRouter()
django_router = APIRouter()  # Unlimited access for Django
telegram_router = APIRouter()  # Dedicated auth for Telegram report consumer
# app.include_router(router)
# app.include_router(router, prefix="/api")

ADMIN_KEY = os.getenv("ADMIN_SECRET_KEY", "changeme")
DJANGO_KEY = os.getenv("DJANGO_SECRET_KEY", "django-unlimited-access")
TELEGRAM_SECRET_KEY = os.getenv("TELEGRAM_BOT_SECRET_KEY")

@admin_router.post("/api_keys", response_model=APIKeyCreateResponse, tags=["Admin"])
async def create_api_key_endpoint(
    payload: APIKeyCreateRequest,
    x_admin_key: str = Header(...)
):
    if x_admin_key != ADMIN_KEY:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid admin key")
    return await create_api_key(payload)

@admin_router.post("/rate-limit/clear/{api_key}", tags=["Admin"])
async def clear_rate_limit_endpoint(
    api_key: str,
    x_admin_key: str = Header(...)
):
    """
    Clear rate limit for a specific API key.
    Only administrators can perform this action.
    """
    if x_admin_key != ADMIN_KEY:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid admin key")
    
    return await clear_rate_limit(api_key)

@router.get("/analytics/scatter_plot", tags=["Analytics"])
async def scatter_plot(
        source: Optional[str] = Query(None, description="mudahmy, carlistmy atau carsome (kosongkan jika datanya tanpa filter sumber)"),
        brand: Optional[str] = Query(None),
        model: Optional[str] = Query(None),
        variant: Optional[str] = Query(None),
        year: Optional[int] = Query(None)
):
    try:
        data = await get_price_vs_mileage_filtered(source, brand, model, variant, year)

        if not data:
            raise HTTPException(status_code=404, detail="No data found for the given filters.")

        buf = await generate_scatter_plot(data)

        return StreamingResponse(buf, media_type="image/png")

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/analytics/price_vs_mileage", tags=["Analytics"])
async def price_vs_mileage(
    source: Optional[str] = Query(None, description="mudahmy, carlistmy atau carsome (kosongkan jika datanya tanpa filter sumber)"),
    brand: Optional[str] = Query(None),
    model: Optional[str] = Query(None),
    variant: Optional[str] = Query(None),
    year: Optional[int] = Query(None),
    limit: int = Query(100, ge=1, le=1000, description="Jumlah data per halaman"),
    offset: int = Query(0, ge=0, description="Offset data (untuk pagination)"),
    sort_by: Optional[str] = Query("scraped_at", description="Kolom untuk pengurutan. Options: scraped_at, ads_date"),
    sort_direction: Optional[str] = Query("desc", description="Arah pengurutan. Options: asc, desc"),
):
    conn = await get_local_db_connection()
    try:
        data = await get_price_vs_mileage_filtered(
            source=source,
            brand=brand,
            model=model,
            variant=variant,
            year=year,
            limit=limit,
            offset=offset,
            sort_by=sort_by,
            sort_direction=sort_direction,
            conn=conn,
        )

        from app.services import get_price_vs_mileage_total_count
        total = await get_price_vs_mileage_total_count(
            source=source,
            brand=brand,
            model=model,
            variant=variant,
            year=year,
            conn=conn,
        )

        return {
            "data": data,
            "meta": {
                "total": total,
                "limit": limit,
                "offset": offset
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        await conn.close()


# Django Endpoints (Unlimited Access)
def verify_django_key(x_django_key: str = Header(...)):
    """Verify Django secret key"""
    if x_django_key != DJANGO_KEY:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid Django key")

def verify_telegram_secret_key(x_telegram_bot_secret_key: str = Header(...)):
    """Verify Telegram report secret key (separate from API clients auth)."""
    if not TELEGRAM_SECRET_KEY:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="TELEGRAM_BOT_SECRET_KEY is not configured on server",
        )
    if x_telegram_bot_secret_key != TELEGRAM_SECRET_KEY:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid Telegram secret key")

def _parse_yyyy_mm_dd(value: str):
    return datetime.strptime(value, "%Y-%m-%d").date()


@telegram_router.get(
    "/telegram/reports/cars/daily-metrics",
    tags=["Telegram"],
    response_model=TelegramDailyMetricsResponse,
)
async def telegram_cars_daily_metrics(
    date: Optional[str] = Query(None, description="YYYY-MM-DD (default: server local today)"),
    sources: Optional[str] = Query(None, description="Comma-separated sources, e.g. mudahmy,carlistmy"),
    x_telegram_bot_secret_key: str = Header(...),
):
    verify_telegram_secret_key(x_telegram_bot_secret_key)

    report_date = _parse_yyyy_mm_dd(date) if date else datetime.now().date()
    source_list: Optional[List[str]] = None
    if sources and sources.strip():
        source_list = [s.strip() for s in sources.split(",") if s.strip()]
    return await get_telegram_daily_metrics(report_date=report_date, sources=source_list)


@django_router.get("/django/brands", tags=["Django"])
async def get_brands_for_django(x_django_key: str = Header(...)):
    """Get all brands for Django dropdown"""
    verify_django_key(x_django_key)
    try:
        brands = await get_brands_list()
        return brands
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@django_router.get("/django/models", tags=["Django"])
async def get_models_for_django(
    brand: str = Query(...),
    x_django_key: str = Header(...)
):
    """Get models for selected brand"""
    verify_django_key(x_django_key)
    try:
        models = await get_models_list(brand)
        return models
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@django_router.get("/django/variants", tags=["Django"])
async def get_variants_for_django(
    brand: str = Query(...),
    model: str = Query(...),
    x_django_key: str = Header(...)
):
    """Get variants for selected brand and model"""
    verify_django_key(x_django_key)
    try:
        variants = await get_variants_list(brand, model)
        return variants
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@django_router.get("/django/years", tags=["Django"])
async def get_years_for_django(
    brand: str = Query(...),
    model: str = Query(...),
    variant: str = Query(...),
    x_django_key: str = Header(...)
):
    """Get years for selected brand, model, and variant"""
    verify_django_key(x_django_key)
    try:
        years = await get_years_list(brand, model, variant)
        return years
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@django_router.get("/django/cars", tags=["Django"])
async def get_cars_for_django(
    draw: int = Query(1),
    start: int = Query(0),
    length: int = Query(10),
    search: Optional[str] = Query(None),
    order_column: Optional[str] = Query(None),
    order_direction: Optional[str] = Query("asc"),
    source_filter: Optional[str] = Query(None),
    year_filter: Optional[str] = Query(None),
    price_filter: Optional[str] = Query(None),
    brand_filter: Optional[str] = Query(None),
    model_filter: Optional[str] = Query(None),
    variant_filter: Optional[str] = Query(None),
    year_value: Optional[int] = Query(None),
    x_django_key: str = Header(...)
):
    """Get car records for Django DataTables"""
    verify_django_key(x_django_key)
    try:
        result = await get_car_records(
            draw=draw,
            start=start,
            length=length,
            search=search,
            order_column=order_column,
            order_direction=order_direction,
            source_filter=source_filter,
            year_filter=year_filter,
            price_filter=price_filter,
            brand_filter=brand_filter,
            model_filter=model_filter,
            variant_filter=variant_filter,
            year_value=year_value
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@django_router.get("/django/car/{car_id}", tags=["Django"])
async def get_car_detail_for_django(
    car_id: int,
    source: Optional[str] = Query(None),
    x_django_key: str = Header(...)
):
    """Get detailed car information"""
    verify_django_key(x_django_key)
    try:
        car_detail = await get_car_detail(car_id, source)
        return car_detail
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@django_router.get("/django/statistics", tags=["Django"])
async def get_statistics_for_django(x_django_key: str = Header(...)):
    """Get dashboard statistics"""
    verify_django_key(x_django_key)
    try:
        stats = await get_statistics()
        return stats
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@django_router.get("/django/today-count", tags=["Django"])
async def get_today_count_for_django(x_django_key: str = Header(...)):
    """Get today's data count"""
    verify_django_key(x_django_key)
    try:
        count = await get_today_data_count()
        return {"count": count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@django_router.post("/django/price-estimation", tags=["Django"])
async def get_price_estimation_for_django(
    brand: str,
    model: str,
    variant: str,
    year: int,
    mileage: Optional[int] = None,
    x_django_key: str = Header(...)
):
    """Get price estimation for car"""
    verify_django_key(x_django_key)
    try:
        estimation = await get_price_estimation(brand, model, variant, year, mileage)
        return estimation
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@django_router.get("/django/brand-car-counts", tags=["Django"])
async def get_brand_car_counts_for_django(x_django_key: str = Header(...)):
    """Get car counts for all brands in bulk"""
    verify_django_key(x_django_key)
    try:
        counts = await get_brand_car_counts()
        return counts
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
