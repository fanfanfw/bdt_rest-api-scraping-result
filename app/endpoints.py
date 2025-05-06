from typing import Optional
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from app.services import (
    sync_data_from_remote, get_price_vs_mileage_filtered, generate_scatter_plot
)
from typing import Optional
from app.models import SyncDataResponse

router = APIRouter()
# app.include_router(router)
# app.include_router(router, prefix="/api")

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
):
    try:
        data = await get_price_vs_mileage_filtered(
            source=source,
            brand=brand,
            model=model,
            variant=variant,
            year=year
        )
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/sync_data", response_model=SyncDataResponse, include_in_schema=False)
async def sync_data():
    result = await sync_data_from_remote()
    return result