from typing import Optional, List
from fastapi import APIRouter, HTTPException, Query, Header, status
from fastapi.responses import StreamingResponse
from app.services import (
    get_price_vs_mileage_filtered, generate_scatter_plot, 
    create_api_key, clear_rate_limit, get_brands_list, get_models_list, 
    get_variants_list, get_years_list, get_car_records, get_car_detail, 
    get_statistics, get_today_data_count, get_price_estimation, get_brand_car_counts
)
from typing import Optional
from app.models import APIKeyCreateRequest, APIKeyCreateResponse
import os

router = APIRouter()
admin_router = APIRouter()
django_router = APIRouter()  # Unlimited access for Django
# app.include_router(router)
# app.include_router(router, prefix="/api")

ADMIN_KEY = os.getenv("ADMIN_SECRET_KEY", "changeme")
DJANGO_KEY = os.getenv("DJANGO_SECRET_KEY", "django-unlimited-access")

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
        source: Optional[str] = Query(None, description="mudahmy atau carlistmy"),
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
    source: Optional[str] = Query(None, description="mudahmy atau carlistmy, jika tidak disertakan akan menampilkan data dari kedua sumber"),
    brand: Optional[str] = Query(None),
    model: Optional[str] = Query(None),
    variant: Optional[str] = Query(None),
    year: Optional[int] = Query(None),
    limit: int = Query(100, ge=1, le=1000, description="Jumlah data per halaman"),
    offset: int = Query(0, ge=0, description="Offset data (untuk pagination)"),
    sort_by: Optional[str] = Query("scraped_at", description="Kolom untuk pengurutan. Options: scraped_at, ads_date"),
    sort_direction: Optional[str] = Query("desc", description="Arah pengurutan. Options: asc, desc")
):
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
            sort_direction=sort_direction
        )

        from app.services import get_price_vs_mileage_total_count
        total = await get_price_vs_mileage_total_count(
            source=source,
            brand=brand,
            model=model,
            variant=variant,
            year=year
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


# Django Endpoints (Unlimited Access)
def verify_django_key(x_django_key: str = Header(...)):
    """Verify Django secret key"""
    if x_django_key != DJANGO_KEY:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid Django key")


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
            price_filter=price_filter
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@django_router.get("/django/car/{car_id}", tags=["Django"])
async def get_car_detail_for_django(
    car_id: int,
    x_django_key: str = Header(...)
):
    """Get detailed car information"""
    verify_django_key(x_django_key)
    try:
        car_detail = await get_car_detail(car_id)
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