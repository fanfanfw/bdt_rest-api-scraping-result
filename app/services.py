import logging
import os
import re
import time
import pandas as pd
import matplotlib.pyplot as plt
import io
import numpy as np
import secrets
from datetime import datetime, timedelta, date
from typing import Optional, List, Tuple, Dict, Any, Set
from fastapi import HTTPException
from app.database import get_local_db_connection, get_remote_db_connection
from app.models import (
    BrandCount,
    APIKeyCreateRequest,
    APIKeyCreateResponse,
    CarsStandardComparisonResult,
    CarsStandardSyncResult,
    ColumnDifference,
)

logger = logging.getLogger(__name__)

# Remote database variables removed - no longer needed
TB_UNIFIED = os.getenv("TB_UNIFIED", "cars_unified")
TB_PRICE_HISTORY = os.getenv("TB_PRICE_HISTORY", "price_history_unified")
TB_CARS_STANDARD = os.getenv("TB_CARS_STANDARD", "cars_standard")
TB_CARSOME = os.getenv("TB_CARSOME", "carsome")

def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        logger.warning("Invalid %s=%r; using %d", name, raw, default)
        return default


_PRICE_VS_MILEAGE_TOTAL_TTL_S = _env_int("PRICE_VS_MILEAGE_TOTAL_TTL_S", 300)
_price_vs_mileage_total_cache: Dict[Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[int]], Tuple[int, float]] = {}


def _normalize_source(source: Optional[str]) -> Optional[str]:
    if not source:
        return None
    normalized = source.strip().lower()
    return normalized or None


def _cache_get_total_count(key):
    cached = _price_vs_mileage_total_cache.get(key)
    if not cached:
        return None
    value, expires_at = cached
    if time.monotonic() >= expires_at:
        _price_vs_mileage_total_cache.pop(key, None)
        return None
    return value


def _cache_set_total_count(key, value: int):
    if _PRICE_VS_MILEAGE_TOTAL_TTL_S <= 0:
        return
    _price_vs_mileage_total_cache[key] = (value, time.monotonic() + _PRICE_VS_MILEAGE_TOTAL_TTL_S)


def _extract_schema_and_table(table_identifier: str) -> Tuple[str, str]:
    """Split table identifier into schema and table parts."""
    if "." in table_identifier:
        schema, table = table_identifier.split(".", 1)
    else:
        schema, table = "public", table_identifier

    return schema.replace('"', "").lower(), table.replace('"', "").lower()

async def _get_table_columns(conn, table_name: str) -> List[str]:
    schema, plain_table = _extract_schema_and_table(table_name)
    query = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = $1
          AND table_name = $2
        ORDER BY ordinal_position
    """
    rows = await conn.fetch(query, schema, plain_table)
    return [row["column_name"] for row in rows]

async def _fetch_table_rows(conn, table_name: str, columns: List[str]) -> Dict[Any, Dict[str, Any]]:
    columns_sql = ", ".join(columns)
    query = f"SELECT {columns_sql} FROM {table_name}"
    rows = await conn.fetch(query)
    result: Dict[Any, Dict[str, Any]] = {}
    for row in rows:
        row_dict = {col: row[col] for col in columns}
        result[row_dict["id"]] = row_dict
    return result

async def _fetch_cars_standard_snapshots() -> Tuple[List[str], Dict[Any, Dict[str, Any]], Dict[Any, Dict[str, Any]]]:
    """
    Ambil struktur kolom dan snapshot isi tabel cars_standard di lokal & remote.
    """
    local_conn = await get_local_db_connection()
    remote_conn = await get_remote_db_connection()

    try:
        columns = await _get_table_columns(local_conn, TB_CARS_STANDARD)
        if not columns:
            raise HTTPException(
                status_code=404,
                detail=f"Table {TB_CARS_STANDARD} has no columns in the local database"
            )

        local_rows = await _fetch_table_rows(local_conn, TB_CARS_STANDARD, columns)
        remote_rows = await _fetch_table_rows(remote_conn, TB_CARS_STANDARD, columns)

        return columns, local_rows, remote_rows

    finally:
        await local_conn.close()
        await remote_conn.close()

def _detect_mismatched_ids(
    columns: List[str],
    left_rows: Dict[Any, Dict[str, Any]],
    right_rows: Dict[Any, Dict[str, Any]],
) -> Set[Any]:
    """
    Cari ID yang hilang di right_rows atau kolomnya berbeda dibanding left_rows.
    """
    mismatched: Set[Any] = set()
    for record_id, left_row in left_rows.items():
        right_row = right_rows.get(record_id)
        if right_row is None:
            mismatched.add(record_id)
            continue

        for column in columns:
            if column == "id":
                continue
            if left_row[column] != right_row[column]:
                mismatched.add(record_id)
                break
    return mismatched

def _build_upsert_query(table_name: str, columns: List[str]) -> str:
    col_list = ", ".join(columns)
    placeholders = ", ".join(f"${idx}" for idx in range(1, len(columns) + 1))
    update_cols = [col for col in columns if col != "id"]
    if not update_cols:
        raise HTTPException(status_code=500, detail="Table must contain non-ID columns for syncing")
    update_set = ", ".join(f"{col}=EXCLUDED.{col}" for col in update_cols)
    return (
        f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders}) "
        f"ON CONFLICT (id) DO UPDATE SET {update_set}"
    )

async def compare_cars_standard_tables(max_differences: int = 100) -> CarsStandardComparisonResult:
    """
    Compare local and remote cars_standard tables and highlight mismatches.
    """
    if max_differences <= 0:
        raise HTTPException(status_code=400, detail="max_differences must be greater than 0")

    columns, local_rows, remote_rows = await _fetch_cars_standard_snapshots()

    local_ids: Set[Any] = set(local_rows.keys())
    remote_ids: Set[Any] = set(remote_rows.keys())

    missing_in_remote = sorted(local_ids - remote_ids)
    missing_in_local = sorted(remote_ids - local_ids)

    differences: List[ColumnDifference] = []
    shared_ids = sorted(local_ids & remote_ids)
    reached_limit = False

    for record_id in shared_ids:
        local_row = local_rows[record_id]
        remote_row = remote_rows[record_id]

        for column in columns:
            if column == "id":
                continue

            if local_row[column] != remote_row[column]:
                differences.append(
                    ColumnDifference(
                        id=record_id,
                        column=column,
                        local_value=local_row[column],
                        remote_value=remote_row[column],
                    )
                )

                if len(differences) >= max_differences:
                    reached_limit = True
                    break

        if reached_limit:
            break

    status = "OK"
    if missing_in_local or missing_in_remote or differences:
        status = "MISMATCH"

    return CarsStandardComparisonResult(
        status=status,
        checked_at=datetime.utcnow(),
        local_count=len(local_rows),
        remote_count=len(remote_rows),
        missing_in_local=missing_in_local,
        missing_in_remote=missing_in_remote,
        differences=differences,
    )

async def sync_cars_standard_tables(
    direction: str,
    ids: Optional[List[int]] = None,
    sync_all: bool = False,
) -> CarsStandardSyncResult:
    """
    Sinkronkan data cars_standard berdasarkan arah yang dipilih.

    direction:
        - "remote-to-local": remote dijadikan sumber, lokal ditimpa.
        - "local-to-remote": lokal dijadikan sumber, remote ditimpa.
    ids:
        - Jika diisi, hanya ID tersebut yang disinkronkan (abaikan jika tidak ada di sumber).
        - Jika kosong dan sync_all=False, otomatis pakai daftar ID yang berbeda antara kedua sisi.
    sync_all:
        - Bila True, semua baris dari sumber akan disalin ke target (mengabaikan deteksi mismatch).
    """
    normalized_direction = direction.lower()
    if normalized_direction not in ("remote-to-local", "local-to-remote"):
        raise HTTPException(
            status_code=400,
            detail="direction must be either 'remote-to-local' or 'local-to-remote'",
        )

    columns, local_rows, remote_rows = await _fetch_cars_standard_snapshots()

    if normalized_direction == "remote-to-local":
        source_rows = remote_rows
        target_rows = local_rows
        target_conn_factory = get_local_db_connection
    else:
        source_rows = local_rows
        target_rows = remote_rows
        target_conn_factory = get_remote_db_connection

    if sync_all:
        ids_to_sync = sorted(source_rows.keys())
        missing_in_source = []
    elif ids:
        ids_to_sync = [record_id for record_id in ids if record_id in source_rows]
        missing_in_source = sorted(record_id for record_id in ids if record_id not in source_rows)
    else:
        ids_to_sync = sorted(_detect_mismatched_ids(columns, source_rows, target_rows))
        missing_in_source = []

    if not ids_to_sync:
        return CarsStandardSyncResult(
            direction=normalized_direction,
            table=TB_CARS_STANDARD,
            synced_ids=[],
            missing_in_source=missing_in_source,
            total_synced=0,
        )

    query = _build_upsert_query(TB_CARS_STANDARD, columns)
    target_conn = await target_conn_factory()
    try:
        for record_id in ids_to_sync:
            row = source_rows[record_id]
            values = [row[col] for col in columns]
            await target_conn.execute(query, *values)
    finally:
        await target_conn.close()

    return CarsStandardSyncResult(
        direction=normalized_direction,
        table=TB_CARS_STANDARD,
        synced_ids=ids_to_sync,
        missing_in_source=missing_in_source,
        total_synced=len(ids_to_sync),
    )

def convert_price(price_str):
    if isinstance(price_str, int):
        return price_str  
    if isinstance(price_str, str) and 'RM' in price_str:
        return int(price_str.replace('RM', '').replace(',', '').strip())
    return None

def convert_mileage(mileage_str):
    if isinstance(mileage_str, int):
        return mileage_str  
    if isinstance(mileage_str, str):
        numbers = re.findall(r'\d+', mileage_str)
        if numbers:
            mileage_value = int(numbers[-1])
            
            if mileage_value >= 1000:
                return mileage_value
            else:
                return mileage_value * 1000
    return None

def parse_datetime(value):
    if isinstance(value, str):
        try:
            return datetime.strptime(value, "%Y-%m-%d %H:%M:%S") 
        except ValueError:
            return None
    elif isinstance(value, datetime):
        return value
    return None

# fetch_data_from_remote_db function removed - no longer needed

# verify_remote_tables function removed - no longer needed

# get_remote_db_connection function removed - no longer needed

def clean_and_standardize_brand(text):
    if not text or text.strip() == "-":
        return "UNKNOWN BRAND"
    text = text.replace("-", " ")
    text = re.sub(r'[^\w\s]', '', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip().upper() 

def clean_and_standardize_variant(text):
    if not text or text.strip() == "-":
        return "NO VARIANT"
    text = re.sub(r'[^\w\s]', '', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip().upper()

def _build_price_vs_mileage_cte() -> str:
    """
    Common CTE that unifies rows from cars_unified and carsome with aligned columns
    and already-normalized brand/model/variant taken from cars_standard.
    Only listings that have a valid cars_standard_id will appear in the combined set.
    """
    return _build_price_vs_mileage_cte_for_source(None)


def _build_price_vs_mileage_cte_for_source(source: Optional[str]) -> str:
    normalized_source = _normalize_source(source)

    unified_select = f"""
        SELECT
            c.id,
            c.cars_standard_id,
            cs.brand_norm AS brand,
            cs.model_norm AS model,
            cs.variant_norm AS variant,
            c.price,
            c.mileage,
            c.year,
            c.source,
            c.status,
            c.last_scraped_at,
            c.information_ads_date
        FROM {TB_UNIFIED} c
        INNER JOIN {TB_CARS_STANDARD} cs ON c.cars_standard_id = cs.id
        WHERE c.status IN ('active', 'sold')
    """

    carsome_select = f"""
        SELECT
            co.id,
            co.cars_standard_id,
            cs.brand_norm AS brand,
            cs.model_norm AS model,
            cs.variant_norm AS variant,
            co.price,
            co.mileage,
            co.year,
            COALESCE(co.source, 'carsome') AS source,
            co.status,
            co.last_updated_at AS last_scraped_at,
            co.created_at::date AS information_ads_date
        FROM {TB_CARSOME} co
        INNER JOIN {TB_CARS_STANDARD} cs ON co.cars_standard_id = cs.id
        WHERE co.status IN ('active', 'sold')
    """

    if normalized_source == "carsome":
        combined = carsome_select
    elif normalized_source in {"mudahmy", "carlistmy"}:
        combined = unified_select
    else:
        combined = f"{unified_select}\n\n        UNION ALL\n\n        {carsome_select}"

    return f"WITH combined AS (\n{combined}\n)"

# insert_or_update_data_into_local_db function removed - sync now handled by sync_cars.py

# get_id_mapping function removed - no longer needed

# insert_or_update_price_history_by_listing_url function removed - sync now handled by sync_cars.py

# sync_data_from_remote function removed - sync now handled by sync_cars.py command

# fetch_price_history_from_remote_db function removed - no longer needed

# insert_or_update_price_history function removed - sync now handled by sync_cars.py

async def get_brand_distribution_carlistmy() -> List[BrandCount]:
    """
    Mengembalikan jumlah listing untuk setiap brand di tabel cars_carlistmy.
    Urutkan dari brand dengan listing terbanyak ke paling sedikit.
    """
    conn = await get_local_db_connection()
    try:
        query = f"""
            SELECT cs.brand_norm AS brand, COUNT(*) AS total
            FROM {TB_UNIFIED} c
            INNER JOIN {TB_CARS_STANDARD} cs ON c.cars_standard_id = cs.id
            WHERE cs.brand_norm IS NOT NULL
              AND c.source = 'carlistmy'
              AND c.status IN ('active', 'sold')
            GROUP BY cs.brand_norm
            ORDER BY total DESC
        """
        rows = await conn.fetch(query)

        results = []
        for row in rows:
            results.append(
                BrandCount(
                    brand=row["brand"],
                    count=row["total"]
                )
            )
        return results
    finally:
        await conn.close()

async def get_price_vs_mileage_filtered(
    source: Optional[str] = None,
    brand: Optional[str] = None,
    model: Optional[str] = None,
    variant: Optional[str] = None,
    year: Optional[int] = None,
    limit: int = 100,
    offset: int = 0,
    sort_by: str = "scraped_at",
    sort_direction: str = "desc",
    conn=None,
) -> List[dict]:
    owns_conn = False
    if conn is None:
        conn = await get_local_db_connection()
        owns_conn = True
    try:
        conditions = []
        values = []
        param_index = 1  

        if brand:
            conditions.append(f"c.brand = ${param_index}")
            values.append(brand.strip().upper())
            param_index += 1
        
        if model:
            conditions.append(f"c.model = ${param_index}")
            values.append(model.strip().upper())
            param_index += 1
        
        if variant:
            conditions.append(f"c.variant = ${param_index}")
            values.append(variant.strip().upper())
            param_index += 1
        
        if year:
            conditions.append(f"c.year = ${param_index}")
            values.append(year)
            param_index += 1

        # Add source filter if specified
        if source:
            conditions.append(f"c.source = ${param_index}")
            values.append(source)
            param_index += 1

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        # Create query for unified table
        limit_param = param_index
        offset_param = param_index + 1
        
        sort_column = "information_ads_date" if sort_by == "ads_date" else "last_scraped_at"
        sort_order = "ASC" if sort_direction.lower() == "asc" else "DESC"
        
        combined_cte = _build_price_vs_mileage_cte_for_source(source)
        final_query = f"""
            {combined_cte}
            SELECT 
                c.brand,
                c.model,
                c.variant,
                c.price,
                c.mileage,
                c.year,
                c.source,
                c.last_scraped_at as scraped_at,
                c.information_ads_date as ads_date
            FROM combined c
            WHERE {where_clause}
            ORDER BY c.{sort_column} {sort_order} NULLS LAST, c.id DESC
            LIMIT ${limit_param} OFFSET ${offset_param}
        """

        values.extend([limit, offset])
        started = time.monotonic()
        result = await conn.fetch(final_query, *values)
        elapsed_s = time.monotonic() - started
        if elapsed_s >= 2.0:
            logger.warning(
                "Slow price_vs_mileage query: %.3fs (source=%r brand=%r model=%r variant=%r year=%r sort_by=%r offset=%d limit=%d)",
                elapsed_s, source, brand, model, variant, year, sort_by, offset, limit
            )

        data = []
        for row in result:
            data.append({
                "brand": row["brand"],
                "model": row["model"],
                "variant": row["variant"],
                "price": row["price"],
                "mileage": row["mileage"],
                "year": row["year"],
                "source": row["source"],
                "scraped_at": row["scraped_at"].strftime("%Y-%m-%d %H:%M:%S") if row["scraped_at"] else None,
                "ads_date": row["ads_date"].strftime("%Y-%m-%d") if row["ads_date"] else None
            })

        return data

    except Exception as e:
        logger.error(f"Error in get_price_vs_mileage_filtered: {str(e)}")
        raise
    finally:
        if owns_conn and conn is not None:
            await conn.close()

async def generate_scatter_plot(data: list) -> io.BytesIO:
    prices = [item['price'] for item in data]
    mileages = [item['mileage'] for item in data]

    fig, ax = plt.subplots(figsize=(10, 8), dpi=300)

    scatter = ax.scatter(prices, mileages, c='red', alpha=0.5)

    ax.set_title('Scatter Plot: Price vs Mileage', fontsize=16)
    ax.set_xlabel('Price (RM)', fontsize=14)
    ax.set_ylabel('Mileage (Km)', fontsize=14)

    ax.grid(True)

    buf = io.BytesIO()
    plt.savefig(buf, format='png')
    buf.seek(0)
    plt.close(fig)
    return buf

async def create_api_key(data: APIKeyCreateRequest) -> APIKeyCreateResponse:
    api_key = f"key_{secrets.token_hex(16)}"
    conn = await get_local_db_connection()

    try:
        row = await conn.fetchrow("""
            INSERT INTO api_clients (client_name, api_key, is_active, request_count, last_reset, rate_limit, purpose)
            VALUES ($1, $2, TRUE, 0, now(), $3, $4)
            RETURNING id, client_name, api_key, rate_limit, purpose
        """, data.client_name, api_key, data.rate_limit, data.purpose)

        return APIKeyCreateResponse(**row)
    finally:
        await conn.close()

async def get_price_vs_mileage_total_count(
    source: Optional[str] = None,
    brand: Optional[str] = None,
    model: Optional[str] = None,
    variant: Optional[str] = None,
    year: Optional[int] = None,
    conn=None,
) -> int:
    cache_key = (
        _normalize_source(source),
        brand.strip().upper() if brand else None,
        model.strip().upper() if model else None,
        variant.strip().upper() if variant else None,
        year,
    )
    cached = _cache_get_total_count(cache_key)
    if cached is not None:
        return cached

    owns_conn = False
    if conn is None:
        conn = await get_local_db_connection()
        owns_conn = True
    try:
        conditions = []
        values = []
        param_index = 1  

        if brand:
            conditions.append(f"c.brand = ${param_index}")
            values.append(brand.strip().upper())
            param_index += 1
        
        if model:
            conditions.append(f"c.model = ${param_index}")
            values.append(model.strip().upper())
            param_index += 1
        
        if variant:
            conditions.append(f"c.variant = ${param_index}")
            values.append(variant.strip().upper())
            param_index += 1
        
        if year:
            conditions.append(f"c.year = ${param_index}")
            values.append(year)
            param_index += 1

        # Add source filter if specified
        if source:
            conditions.append(f"c.source = ${param_index}")
            values.append(source)
            param_index += 1

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        combined_cte = _build_price_vs_mileage_cte_for_source(source)
        count_query = f"""
            {combined_cte}
            SELECT COUNT(*) AS total
            FROM combined c
            WHERE {where_clause}
        """
        
        started = time.monotonic()
        result = await conn.fetchval(count_query, *values)
        elapsed_s = time.monotonic() - started
        if elapsed_s >= 2.0:
            logger.warning(
                "Slow price_vs_mileage COUNT: %.3fs (source=%r brand=%r model=%r variant=%r year=%r)",
                elapsed_s, source, brand, model, variant, year
            )
        _cache_set_total_count(cache_key, int(result))
        return result

    finally:
        if owns_conn and conn is not None:
            await conn.close()


async def clear_rate_limit(api_key: str) -> dict:
    """
    Reset the request count and last reset time for a specific API key.
    """
    conn = await get_local_db_connection()
    try:
        # First check if the API key exists and is active
        row = await conn.fetchrow("""
            SELECT id, client_name
            FROM api_clients
            WHERE api_key = $1 AND is_active = TRUE
        """, api_key)

        if not row:
            raise HTTPException(status_code=404, detail="API key not found or inactive")

        # Reset the rate limit counters
        now = datetime.utcnow()
        await conn.execute("""
            UPDATE api_clients
            SET request_count = 0, last_reset = $1
            WHERE id = $2
        """, now, row['id'])

        return {
            "status": "success",
            "message": f"Rate limit cleared for client: {row['client_name']}",
            "client_name": row['client_name'],
            "reset_time": now
        }

    finally:
        await conn.close()


# Django Service Functions
async def get_brands_list() -> List[str]:
    """Get all unique brands from cars_standard where cars exist in either cars_unified or carsome with active/sold status"""
    conn = await get_local_db_connection()
    try:
        query = f"""
            WITH combined AS (
                SELECT DISTINCT cars_standard_id
                FROM {TB_UNIFIED}
                WHERE status IN ('active', 'sold') AND cars_standard_id IS NOT NULL

                UNION

                SELECT DISTINCT cars_standard_id
                FROM {TB_CARSOME}
                WHERE status IN ('active', 'sold') AND cars_standard_id IS NOT NULL
            )
            SELECT DISTINCT cs.brand_norm
            FROM combined c
            INNER JOIN {TB_CARS_STANDARD} cs ON cs.id = c.cars_standard_id
            WHERE cs.brand_norm IS NOT NULL
            ORDER BY cs.brand_norm
        """
        rows = await conn.fetch(query)
        return [row['brand_norm'] for row in rows]
    finally:
        await conn.close()


async def get_models_list(brand: str) -> List[str]:
    """Get models for specific brand where cars exist in either cars_unified or carsome with active/sold status"""
    conn = await get_local_db_connection()
    try:
        query = f"""
            WITH combined AS (
                SELECT DISTINCT cars_standard_id
                FROM {TB_UNIFIED}
                WHERE status IN ('active', 'sold') AND cars_standard_id IS NOT NULL

                UNION

                SELECT DISTINCT cars_standard_id
                FROM {TB_CARSOME}
                WHERE status IN ('active', 'sold') AND cars_standard_id IS NOT NULL
            )
            SELECT DISTINCT cs.model_norm
            FROM combined c
            INNER JOIN {TB_CARS_STANDARD} cs ON cs.id = c.cars_standard_id
            WHERE cs.brand_norm = $1
              AND cs.model_norm IS NOT NULL
            ORDER BY cs.model_norm
        """
        rows = await conn.fetch(query, brand)
        return [row['model_norm'] for row in rows]
    finally:
        await conn.close()


async def get_variants_list(brand: str, model: str) -> List[str]:
    """Get variants for specific brand and model where cars exist in either cars_unified or carsome with active/sold status"""
    conn = await get_local_db_connection()
    try:
        query = f"""
            WITH combined AS (
                SELECT DISTINCT cars_standard_id
                FROM {TB_UNIFIED}
                WHERE status IN ('active', 'sold') AND cars_standard_id IS NOT NULL

                UNION

                SELECT DISTINCT cars_standard_id
                FROM {TB_CARSOME}
                WHERE status IN ('active', 'sold') AND cars_standard_id IS NOT NULL
            )
            SELECT DISTINCT cs.variant_norm
            FROM combined c
            INNER JOIN {TB_CARS_STANDARD} cs ON cs.id = c.cars_standard_id
            WHERE cs.brand_norm = $1
              AND cs.model_norm = $2
              AND cs.variant_norm IS NOT NULL
            ORDER BY cs.variant_norm
        """
        rows = await conn.fetch(query, brand, model)
        return [row['variant_norm'] for row in rows]
    finally:
        await conn.close()


async def get_years_list(brand: str, model: str, variant: str) -> List[int]:
    """Get years for specific brand, model, and variant"""
    conn = await get_local_db_connection()
    try:
        # First get cars_standard_id
        standard_query = f"""
            SELECT id FROM {TB_CARS_STANDARD}
            WHERE brand_norm = $1 AND model_norm = $2 AND variant_norm = $3
            LIMIT 1
        """
        standard_row = await conn.fetchrow(standard_query, brand, model, variant)
        
        if not standard_row:
            return []
        
        # Get years from both cars_unified and carsome
        years_query = f"""
            SELECT DISTINCT year
            FROM (
                SELECT year
                FROM {TB_UNIFIED}
                WHERE cars_standard_id = $1
                  AND year IS NOT NULL
                  AND status IN ('active', 'sold')

                UNION

                SELECT year
                FROM {TB_CARSOME}
                WHERE cars_standard_id = $1
                  AND year IS NOT NULL
                  AND status IN ('active', 'sold')
            ) AS combined_years
            ORDER BY year DESC
        """
        rows = await conn.fetch(years_query, standard_row['id'])
        return [row['year'] for row in rows]
    finally:
        await conn.close()


async def get_car_records(
    draw: int = 1,
    start: int = 0,
    length: int = 10,
    search: Optional[str] = None,
    order_column: Optional[str] = None,
    order_direction: str = "asc",
    source_filter: Optional[str] = None,
    year_filter: Optional[str] = None,
    price_filter: Optional[str] = None,
    brand_filter: Optional[str] = None,
    model_filter: Optional[str] = None,
    variant_filter: Optional[str] = None,
    year_value: Optional[int] = None
) -> Dict[str, Any]:
    """Get car records for DataTables with pagination and filtering"""
    conn = await get_local_db_connection()
    try:
        conditions = ["1=1"]
        params: List[Any] = []
        param_index = 1

        if search:
            search_conditions = [
                f"c.brand ILIKE ${param_index}",
                f"c.model ILIKE ${param_index}",
                f"c.variant ILIKE ${param_index}",
                f"COALESCE(c.location, '') ILIKE ${param_index}"
            ]
            conditions.append(f"({' OR '.join(search_conditions)})")
            params.append(f"%{search}%")
            param_index += 1

        if source_filter:
            conditions.append(f"c.source = ${param_index}")
            params.append(source_filter)
            param_index += 1

        if brand_filter:
            conditions.append(f"LOWER(c.brand) = LOWER(${param_index})")
            params.append(brand_filter)
            param_index += 1

        if model_filter:
            conditions.append(f"LOWER(c.model) = LOWER(${param_index})")
            params.append(model_filter)
            param_index += 1

        if variant_filter:
            conditions.append(f"LOWER(c.variant) = LOWER(${param_index})")
            params.append(variant_filter)
            param_index += 1

        if year_value:
            conditions.append(f"c.year = ${param_index}")
            params.append(year_value)
            param_index += 1

        if year_filter:
            if isinstance(year_filter, str) and year_filter.isdigit():
                conditions.append(f"c.year = ${param_index}")
                params.append(int(year_filter))
                param_index += 1
            elif year_filter == "2024-":
                conditions.append(f"c.year >= ${param_index}")
                params.append(2024)
                param_index += 1
            elif year_filter == "2020-2023":
                conditions.append(f"c.year BETWEEN ${param_index} AND ${param_index + 1}")
                params.extend([2020, 2023])
                param_index += 2
            elif year_filter == "2015-2019":
                conditions.append(f"c.year BETWEEN ${param_index} AND ${param_index + 1}")
                params.extend([2015, 2019])
                param_index += 2
            elif year_filter == "2010-2014":
                conditions.append(f"c.year BETWEEN ${param_index} AND ${param_index + 1}")
                params.extend([2010, 2014])
                param_index += 2
            elif year_filter == "-2009":
                conditions.append(f"c.year < ${param_index}")
                params.append(2010)
                param_index += 1

        if price_filter:
            if price_filter == "0-50000":
                conditions.append(f"c.price BETWEEN ${param_index} AND ${param_index + 1}")
                params.extend([0, 50000])
                param_index += 2
            elif price_filter == "50000-100000":
                conditions.append(f"c.price BETWEEN ${param_index} AND ${param_index + 1}")
                params.extend([50000, 100000])
                param_index += 2
            elif price_filter == "100000-200000":
                conditions.append(f"c.price BETWEEN ${param_index} AND ${param_index + 1}")
                params.extend([100000, 200000])
                param_index += 2
            elif price_filter == "200000-":
                conditions.append(f"c.price >= ${param_index}")
                params.append(200000)
                param_index += 1

        where_clause = " AND ".join(conditions)

        combined_cte = f"""
            WITH combined AS (
                SELECT 
                    c.id,
                    COALESCE(c.source, 'carlistmy') AS source,
                    cs.brand_norm AS brand,
                    cs.model_norm AS model,
                    cs.variant_norm AS variant,
                    c.year,
                    c.price,
                    c.mileage,
                    c.location,
                    c.condition,
                    c.listing_url,
                    c.last_scraped_at,
                    c.information_ads_date,
                    c.cars_standard_id,
                    c.last_scraped_at AS created_at
                FROM {TB_UNIFIED} c
                INNER JOIN {TB_CARS_STANDARD} cs ON c.cars_standard_id = cs.id
                WHERE c.status IN ('active', 'sold')

                UNION ALL

                SELECT
                    co.id,
                    COALESCE(co.source, 'carsome') AS source,
                    cs.brand_norm AS brand,
                    cs.model_norm AS model,
                    cs.variant_norm AS variant,
                    co.year,
                    co.price,
                    co.mileage,
                    NULL AS location,
                    NULL AS condition,
                    NULL AS listing_url,
                    co.last_updated_at AS last_scraped_at,
                    co.created_at::date AS information_ads_date,
                    co.cars_standard_id,
                    co.created_at AS created_at
                FROM {TB_CARSOME} co
                INNER JOIN {TB_CARS_STANDARD} cs ON co.cars_standard_id = cs.id
                WHERE co.status IN ('active', 'sold')
            )
        """

        total_query = f"""
            {combined_cte}
            SELECT COUNT(*)
            FROM combined c
            WHERE {where_clause}
        """
        total_records = await conn.fetchval(total_query, *params)

        if order_column:
            order_col_map = {
                "0": "c.id",
                "1": "c.source",
                "2": "c.brand",
                "3": "c.model",
                "4": "c.variant",
                "5": "c.year",
                "6": "c.mileage",
                "7": "c.price"
            }
            chosen_col = order_col_map.get(order_column, "c.id")
            direction = "ASC" if order_direction.lower() == "asc" else "DESC"
            order_sql = f"ORDER BY {chosen_col} {direction}, c.id DESC"
        else:
            order_sql = "ORDER BY COALESCE(c.last_scraped_at, c.created_at) DESC NULLS LAST, c.id DESC"

        limit_param = param_index
        offset_param = param_index + 1
        data_query = f"""
            {combined_cte}
            SELECT
                c.id,
                c.source,
                c.brand,
                c.model,
                c.variant,
                c.year,
                c.price,
                c.mileage,
                c.location,
                c.condition,
                c.listing_url,
                c.last_scraped_at,
                c.information_ads_date,
                c.cars_standard_id,
                c.created_at
            FROM combined c
            WHERE {where_clause}
            {order_sql}
            LIMIT ${limit_param} OFFSET ${offset_param}
        """

        params_for_data = params + [length, start]
        rows = await conn.fetch(data_query, *params_for_data)

        data = []
        for row in rows:
            unique_identifier = f"{row['source']}:{row['id']}"
            data.append([
                row['id'],
                row['source'],
                row['brand'] or "-",
                row['model'] or "-",
                row['variant'] or "-",
                row['year'] or "-",
                row['mileage'] if row['mileage'] else "",
                row['price'] if row['price'] else "",
                unique_identifier
            ])

        return {
            "draw": draw,
            "recordsTotal": total_records,
            "recordsFiltered": total_records,
            "data": data
        }
    finally:
        await conn.close()


async def get_car_detail(car_id: int, source: Optional[str] = None) -> Dict[str, Any]:
    """Get detailed car information by ID"""
    conn = await get_local_db_connection()
    try:
        unified_params: List[Any] = [car_id]
        unified_query = f"""
            SELECT c.*, cs.brand_norm, cs.model_norm, cs.variant_norm
            FROM {TB_UNIFIED} c
            INNER JOIN {TB_CARS_STANDARD} cs ON c.cars_standard_id = cs.id
            WHERE c.id = $1
        """

        if source and source != "carsome":
            unified_params.append(source)
            unified_query += " AND c.source = $2"

        row = None
        if source != "carsome":
            row = await conn.fetchrow(unified_query, *unified_params)

        if row:
            ads_tag = row["ads_tag"] if "ads_tag" in row.keys() else None
            created_at_value = row["created_at"] if "created_at" in row.keys() else None
            listing_id_value = row["listing_id"] if "listing_id" in row.keys() else None
            series_value = row["series"] if "series" in row.keys() else None
            type_value = row["type"] if "type" in row.keys() else None
            images_value = row["images"]
            if images_value is None:
                images_value = []
            elif not isinstance(images_value, list):
                images_value = [images_value]
            return {
                'id': row['id'],
                'source': row['source'],
                'listing_id': listing_id_value,
                'listing_url': row['listing_url'],
                'brand': row['brand_norm'] or row['brand'],
                'brand_raw': row['brand'],
                'model': row['model_norm'] or row['model'],
                'model_raw': row['model'],
                'variant': row['variant_norm'] or row['variant'],
                'variant_raw': row['variant'],
                'series': series_value,
                'type': type_value,
                'condition': row['condition'],
                'year': row['year'],
                'mileage': row['mileage'],
                'transmission': row['transmission'],
                'seat_capacity': row['seat_capacity'],
                'engine_cc': row['engine_cc'],
                'fuel_type': row['fuel_type'],
                'price': row['price'],
                'location': row['location'],
                'information_ads': row['information_ads'],
                'images': images_value,
                'status': row['status'],
                'ads_tag': ads_tag,
                'last_scraped_at': row['last_scraped_at'].isoformat() if row['last_scraped_at'] else None,
                'information_ads_date': row['information_ads_date'].isoformat() if row['information_ads_date'] else None,
                'created_at': created_at_value.isoformat() if created_at_value else None,
                'standard_info': {
                    'brand_norm': row['brand_norm'],
                    'model_norm': row['model_norm'],
                    'variant_norm': row['variant_norm']
                } if row['brand_norm'] else None
            }

        carsome_params: List[Any] = [car_id]
        carsome_query = f"""
            SELECT co.*, cs.brand_norm, cs.model_norm, cs.variant_norm
            FROM {TB_CARSOME} co
            INNER JOIN {TB_CARS_STANDARD} cs ON co.cars_standard_id = cs.id
            WHERE co.id = $1
        """

        carsome_row = await conn.fetchrow(carsome_query, *carsome_params)

        if carsome_row:
            created_at = carsome_row['created_at']
            created_at_iso = created_at.isoformat() if created_at else None
            last_updated = carsome_row['last_updated_at']
            last_updated_iso = last_updated.isoformat() if last_updated else created_at_iso
            carsome_images = []
            if carsome_row.get("image"):
                carsome_images = [carsome_row["image"]]

            return {
                'id': carsome_row['id'],
                'source': carsome_row['source'] or 'carsome',
                'listing_url': None,
                'brand': carsome_row['brand_norm'] or carsome_row['brand'],
                'brand_raw': carsome_row['brand'],
                'model': carsome_row['model_norm'] or carsome_row['model'],
                'model_raw': carsome_row['model'],
                'variant': carsome_row['variant_norm'] or carsome_row['variant'],
                'variant_raw': carsome_row['variant'],
                'condition': None,
                'year': carsome_row['year'],
                'mileage': carsome_row['mileage'],
                'transmission': None,
                'seat_capacity': None,
                'engine_cc': None,
                'fuel_type': None,
                'price': carsome_row['price'],
                'location': None,
                'information_ads': None,
                'images': carsome_images,
                'status': carsome_row['status'],
                'ads_tag': None,
                'last_scraped_at': last_updated_iso,
                'information_ads_date': created_at.date().isoformat() if created_at else None,
                'created_at': created_at_iso,
                'standard_info': {
                    'brand_norm': carsome_row['brand_norm'],
                    'model_norm': carsome_row['model_norm'],
                    'variant_norm': carsome_row['variant_norm']
                } if carsome_row['brand_norm'] else None
            }

        raise HTTPException(status_code=404, detail="Car not found")
    finally:
        await conn.close()


async def get_statistics() -> Dict[str, Any]:
    """Get dashboard statistics"""
    conn = await get_local_db_connection()
    try:
        combined_cte = f"""
            WITH combined AS (
                SELECT c.id, c.cars_standard_id, cs.brand_norm AS brand, cs.model_norm AS model
                FROM {TB_UNIFIED} c
                INNER JOIN {TB_CARS_STANDARD} cs ON c.cars_standard_id = cs.id
                WHERE c.status IN ('active', 'sold')

                UNION ALL

                SELECT co.id, co.cars_standard_id, cs.brand_norm AS brand, cs.model_norm AS model
                FROM {TB_CARSOME} co
                INNER JOIN {TB_CARS_STANDARD} cs ON co.cars_standard_id = cs.id
                WHERE co.status IN ('active', 'sold')
            )
        """

        car_count_query = f"""
            {combined_cte}
            SELECT COUNT(*) FROM combined
        """
        brand_count_query = f"""
            {combined_cte}
            SELECT COUNT(DISTINCT combined.brand)
            FROM combined
            WHERE combined.brand IS NOT NULL
        """
        model_count_query = f"""
            {combined_cte}
            SELECT COUNT(DISTINCT combined.model)
            FROM combined
            WHERE combined.model IS NOT NULL
        """

        car_records = await conn.fetchval(car_count_query)
        total_brands = await conn.fetchval(brand_count_query)
        total_models = await conn.fetchval(model_count_query)

        return {
            'car_records': car_records,
            'total_brands': total_brands,
            'total_models': total_models
        }
    finally:
        await conn.close()


async def get_today_data_count() -> int:
    """Get today's data count based on information_ads_date"""
    conn = await get_local_db_connection()
    try:
        today = date.today()
        query = f"""
            SELECT 
                (
                    SELECT COUNT(*)
                    FROM {TB_UNIFIED} c
                    INNER JOIN {TB_CARS_STANDARD} cs ON c.cars_standard_id = cs.id
                    WHERE c.information_ads_date = $1
                      AND c.status IN ('active', 'sold')
                )
                +
                (
                    SELECT COUNT(*)
                    FROM {TB_CARSOME} co
                    INNER JOIN {TB_CARS_STANDARD} cs ON co.cars_standard_id = cs.id
                    WHERE DATE(co.created_at) = $1
                      AND co.status IN ('active', 'sold')
                ) AS total_count
        """
        count = await conn.fetchval(query, today)
        return count
    finally:
        await conn.close()


async def get_price_estimation(
    brand: str, 
    model: str, 
    variant: str, 
    year: int, 
    mileage: Optional[int] = None
) -> Dict[str, Any]:
    """Get price estimation for car based on similar records"""
    conn = await get_local_db_connection()
    try:
        # First get cars_standard_id
        standard_query = f"""
            SELECT id FROM {TB_CARS_STANDARD}
            WHERE brand_norm = $1 AND model_norm = $2 AND variant_norm = $3
            LIMIT 1
        """
        standard_row = await conn.fetchrow(standard_query, brand, model, variant)
        
        if not standard_row:
            raise HTTPException(status_code=404, detail="Car variant not found")
        
        # Get price data for exact year match from both sources
        price_query = f"""
            SELECT price, mileage, year, '{TB_UNIFIED}' as source
            FROM {TB_UNIFIED}
            WHERE cars_standard_id = $1
              AND price IS NOT NULL
              AND price > 0
              AND year = $2
              AND status IN ('active', 'sold')

            UNION ALL

            SELECT price, mileage, year, '{TB_CARSOME}' as source
            FROM {TB_CARSOME}
            WHERE cars_standard_id = $1
              AND price IS NOT NULL
              AND price > 0
              AND year = $2
              AND status IN ('active', 'sold')

            ORDER BY price ASC
        """

        rows = await conn.fetch(price_query, standard_row['id'], year)
        
        if not rows:
            raise HTTPException(status_code=404, detail="No price data available")
        
        prices = [row['price'] for row in rows]
        
        # Calculate statistics
        avg_price = sum(prices) / len(prices)
        min_price = min(prices)
        max_price = max(prices)
        
        # If mileage provided, try to adjust estimation
        estimated_price = avg_price
        if mileage and len(rows) > 3:  # Lower threshold since we're using exact year
            # Simple mileage adjustment (higher mileage = lower price)
            mileages_with_data = [row['mileage'] for row in rows if row['mileage']]
            if mileages_with_data:
                avg_mileage = sum(mileages_with_data) / len(mileages_with_data)
                if avg_mileage > 0:
                    mileage_factor = max(0.8, min(1.2, avg_mileage / mileage))
                    estimated_price = avg_price * mileage_factor
        
        # Calculate average mileage from the data
        mileages_with_data = [row['mileage'] for row in rows if row['mileage']]
        avg_mileage = sum(mileages_with_data) / len(mileages_with_data) if mileages_with_data else 100000

        return {
            'brand': brand,
            'model': model,
            'variant': variant,
            'year': year,
            'estimated_price': round(estimated_price),
            'price_range': {
                'min': min_price,
                'max': max_price,
                'avg': round(avg_price)
            },
            'sample_size': len(prices),
            'confidence': 'high' if len(prices) >= 5 else 'medium' if len(prices) >= 3 else 'low',
            'statistics': {
                'average_price': round(avg_price),
                'average_mileage': round(avg_mileage),
                'data_count': len(prices)
            }
        }
        
    finally:
        await conn.close()

async def get_brand_car_counts() -> Dict[str, int]:
    """Get car count for all brands in bulk"""
    conn = await get_local_db_connection()
    try:
        query = f"""
            WITH combined AS (
                SELECT 
                    cs.brand_norm AS brand_name
                FROM {TB_UNIFIED} c
                INNER JOIN {TB_CARS_STANDARD} cs ON c.cars_standard_id = cs.id
                WHERE c.status IN ('active', 'sold')

                UNION ALL

                SELECT 
                    cs.brand_norm AS brand_name
                FROM {TB_CARSOME} co
                INNER JOIN {TB_CARS_STANDARD} cs ON co.cars_standard_id = cs.id
                WHERE co.status IN ('active', 'sold')
            )
            SELECT brand_name, COUNT(*) AS car_count
            FROM combined
            WHERE brand_name IS NOT NULL
            GROUP BY brand_name
            ORDER BY brand_name
        """
        rows = await conn.fetch(query)
        
        result: Dict[str, int] = {}
        for row in rows:
            result[row['brand_name']] = row['car_count']
        
        return result
    finally:
        await conn.close()
