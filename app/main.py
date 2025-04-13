from fastapi import FastAPI, Depends
from app.endpoints import router
from app.dependencies import verify_api_key

app = FastAPI()
app.include_router(router, dependencies=[Depends(verify_api_key)])
