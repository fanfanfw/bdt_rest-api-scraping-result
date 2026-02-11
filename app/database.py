import asyncpg
import logging
import os
import inspect
from typing import Optional
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

_local_pool: Optional[asyncpg.Pool] = None
_remote_pool: Optional[asyncpg.Pool] = None


class _PooledConnection:
    def __init__(self, pool: asyncpg.Pool, conn: asyncpg.Connection):
        self._pool = pool
        self._conn = conn
        self._released = False

    def __getattr__(self, name):
        return getattr(self._conn, name)

    async def close(self):
        if self._released:
            return
        self._released = True
        result = self._pool.release(self._conn)
        if inspect.isawaitable(result):
            await result


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        logger.warning("Invalid %s=%r; using %d", name, raw, default)
        return default
    return value


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    value = raw.strip().lower()
    if value in {"1", "true", "yes", "y", "on"}:
        return True
    if value in {"0", "false", "no", "n", "off"}:
        return False
    logger.warning("Invalid %s=%r; using %s", name, raw, default)
    return default


async def init_db_pools():
    global _local_pool, _remote_pool
    if _local_pool is None:
        _local_pool = await asyncpg.create_pool(
            DATABASE_URL,
            min_size=_env_int("DB_POOL_MIN_SIZE", 1),
            max_size=_env_int("DB_POOL_MAX_SIZE", 10),
            command_timeout=_env_int("DB_COMMAND_TIMEOUT_S", 60),
        )

    if _env_bool("ENABLE_REMOTE_DB_POOL", False) and _remote_pool is None:
        _remote_pool = await asyncpg.create_pool(
            REMOTE_DATABASE_URL,
            min_size=_env_int("REMOTE_DB_POOL_MIN_SIZE", 1),
            max_size=_env_int("REMOTE_DB_POOL_MAX_SIZE", 5),
            command_timeout=_env_int("REMOTE_DB_COMMAND_TIMEOUT_S", 60),
        )


async def close_db_pools():
    global _local_pool, _remote_pool
    if _local_pool is not None:
        await _local_pool.close()
        _local_pool = None
    if _remote_pool is not None:
        await _remote_pool.close()
        _remote_pool = None


async def get_local_db_connection():
    if _local_pool is None:
        return await asyncpg.connect(DATABASE_URL)
    conn = await _local_pool.acquire()
    return _PooledConnection(_local_pool, conn)

async def get_remote_db_connection():
    """
    Connect to the production database.
    Credentials fall back to the local values when dedicated env vars are absent.
    """
    if _remote_pool is None:
        return await asyncpg.connect(REMOTE_DATABASE_URL)
    conn = await _remote_pool.acquire()
    return _PooledConnection(_remote_pool, conn)
