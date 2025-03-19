import asyncpg
from app.config import DATABASE_URL

async def get_local_db_connection():
    conn = await asyncpg.connect(DATABASE_URL)
    return conn
