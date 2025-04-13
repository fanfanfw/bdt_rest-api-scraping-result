from fastapi import Header, HTTPException, Depends
from datetime import datetime, timedelta
from app.database import get_local_db_connection

async def verify_api_key(x_api_key: str = Header(...)):
    conn = await get_local_db_connection()
    try:
        row = await conn.fetchrow("""
            SELECT id, client_name, request_count, last_reset, rate_limit
            FROM api_clients
            WHERE api_key = $1 AND is_active = TRUE
        """, x_api_key)

        if row is None:
            raise HTTPException(status_code=401, detail="Unauthorized: Invalid API Key")

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
                raise HTTPException(status_code=429, detail="Rate limit exceeded")

            await conn.execute("""
                UPDATE api_clients
                SET request_count = request_count + 1
                WHERE id = $1
            """, row["id"])

    finally:
        await conn.close()
