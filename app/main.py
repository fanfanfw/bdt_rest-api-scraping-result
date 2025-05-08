from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from app.endpoints import router, admin_router
from app.dependencies import verify_api_key

app = FastAPI(
    title="Car Analytics API",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # ganti dengan domain tertentu jika perlu
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Endpoint publik dengan API key
app.include_router(router, dependencies=[Depends(verify_api_key)])

# Endpoint admin tanpa API key tapi dengan admin key
app.include_router(admin_router)
