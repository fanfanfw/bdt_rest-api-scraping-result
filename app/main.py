import time
from datetime import datetime, timezone

from fastapi import FastAPI, Depends, Response
from fastapi.middleware.cors import CORSMiddleware
from app.endpoints import router, admin_router, django_router
from app.dependencies import verify_api_key

_START_TIME = time.monotonic()

app = FastAPI(
    title="Car Analytics API",
    version="1.0.0",
    root_path="/api"  
)

@app.get("/health", tags=["Health"])
@app.get("/healthz", tags=["Health"])
async def health_check_get():
    return {
        "status": "ok",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "uptime_s": int(time.monotonic() - _START_TIME),
    }

@app.head("/health", include_in_schema=False)
@app.head("/healthz", include_in_schema=False)
async def health_check_head():
    return Response(status_code=200)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # ganti dengan domain tertentu jika perlu
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router, dependencies=[Depends(verify_api_key)])
app.include_router(admin_router)
app.include_router(django_router)  # Unlimited access for Django
