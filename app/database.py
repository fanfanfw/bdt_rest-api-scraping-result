import asyncpg
import logging
import os
from dotenv import load_dotenv

load_dotenv(override=True)

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

REMOTE_DB_HOST = os.getenv("REMOTE_DB_HOST", "192.168.1.207")
REMOTE_DB_PORT = os.getenv("REMOTE_DB_PORT", DB_PORT)
REMOTE_DB_USER = os.getenv("REMOTE_DB_USER", DB_USER)
REMOTE_DB_PASSWORD = os.getenv("REMOTE_DB_PASSWORD", DB_PASSWORD)
REMOTE_DB_NAME = os.getenv("REMOTE_DB_NAME", "db_fastapi_scrap")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
REMOTE_DATABASE_URL = (
    f"postgresql://{REMOTE_DB_USER}:{REMOTE_DB_PASSWORD}"
    f"@{REMOTE_DB_HOST}:{REMOTE_DB_PORT}/{REMOTE_DB_NAME}"
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def get_local_db_connection():
    conn = await asyncpg.connect(DATABASE_URL)
    return conn

async def get_remote_db_connection():
    """
    Connect to the production database.
    Credentials fall back to the local values when dedicated env vars are absent.
    """
    conn = await asyncpg.connect(REMOTE_DATABASE_URL)
    return conn
