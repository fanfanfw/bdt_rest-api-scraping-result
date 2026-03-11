from fastapi import HTTPException, Request, Security, status
from fastapi.security import APIKeyHeader
from datetime import datetime, timedelta
from app.database import get_local_db_connection
import os

ALLOWED_IPS = [ip.strip() for ip in os.getenv("ALLOWED_IP", "").split(",") if ip.strip()]
api_key_header = APIKeyHeader(name="x-api-key", scheme_name="ApiKeyAuth", auto_error=False)

async def verify_api_key(
    request: Request,
    x_api_key: str | None = Security(api_key_header),
):
    # Check if client IP is whitelisted
    client_ip = request.client.host if request else None
    is_whitelisted = client_ip in ALLOWED_IPS if ALLOWED_IPS else False

    if not x_api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing x-api-key header",
        )

    conn = await get_local_db_connection()
    try:
        row = await conn.fetchrow("""
            SELECT id, client_name, request_count, last_reset, rate_limit
            FROM api_clients
            WHERE api_key = $1 AND is_active = TRUE
        """, x_api_key)

        if row is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Unauthorized: Invalid API Key",
            )

        # Skip rate limit checks if IP is whitelisted
        if not is_whitelisted:
            now = datetime.utcnow()
            last_reset = row["last_reset"] or now
            if now - last_reset > timedelta(days=1):
                # reset count
                await conn.execute("""
                    UPDATE api_clients
                    SET request_count = 1, last_reset = $1
                    WHERE id = $2
                """, now, row["id"])
            else:
                if row["request_count"] >= row["rate_limit"]:
                    raise HTTPException(
                        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                        detail="Rate limit exceeded",
                    )

                await conn.execute("""
                    UPDATE api_clients
                    SET request_count = request_count + 1
                    WHERE id = $1
                """, row["id"])

    finally:
        await conn.close()
