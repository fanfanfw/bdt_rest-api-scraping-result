from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.endpoints import router

app = FastAPI()

# CORS setup
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # bisa diganti dengan domain spesifik seperti ["http://localhost:8000"]
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(router)
