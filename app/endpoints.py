from typing import Optional
from fastapi import APIRouter, HTTPException, Query, Header, status
from fastapi.responses import StreamingResponse
from app.services import sync_data_from_remote, get_price_vs_mileage_filtered, generate_scatter_plot, create_api_key
from typing import Optional
from app.models import SyncDataResponse, APIKeyCreateRequest, APIKeyCreateResponse
import os

router = APIRouter()
admin_router = APIRouter()
# app.include_router(router)
# app.include_router(router, prefix="/api")

ADMIN_KEY = os.getenv("ADMIN_SECRET_KEY", "changeme")

@admin_router.post("/api_keys", response_model=APIKeyCreateResponse, tags=["Admin"])
async def create_api_key_endpoint(
    payload: APIKeyCreateRequest,
    x_admin_key: str = Header(...)
):
    if x_admin_key != ADMIN_KEY:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid admin key")
    return await create_api_key(payload)

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
):
    try:
        data = await get_price_vs_mileage_filtered(
            source=source,
            brand=brand,
            model=model,
            variant=variant,
            year=year,
            limit=limit,
            offset=offset
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
