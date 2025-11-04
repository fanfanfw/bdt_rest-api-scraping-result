import logging
import os
import re
import pandas as pd
import matplotlib.pyplot as plt
import io
import numpy as np
import secrets
from datetime import datetime, timedelta, date
from typing import Optional, List, Tuple, Dict, Any
from fastapi import HTTPException
from app.database import get_local_db_connection
from app.models import BrandCount,APIKeyCreateRequest, APIKeyCreateResponse

logger = logging.getLogger(__name__)

# Remote database variables removed - no longer needed
TB_UNIFIED = os.getenv("TB_UNIFIED", "cars_unified")
TB_PRICE_HISTORY = os.getenv("TB_PRICE_HISTORY", "price_history_unified")
TB_CARS_STANDARD = os.getenv("TB_CARS_STANDARD", "cars_standard")
TB_CARSOME = os.getenv("TB_CARSOME", "carsome")

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
            SELECT brand, COUNT(*) AS total
            FROM {TB_UNIFIED}
            WHERE brand IS NOT NULL AND source = 'carlistmy' AND status IN ('active', 'sold')
            GROUP BY brand
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
    sort_direction: str = "desc"
) -> List[dict]:
    conn = await get_local_db_connection()
    try:
        conditions = []
        values = []
        param_index = 1  

        if brand:
            conditions.append(f"(cs.brand_norm ILIKE ${param_index} OR c.brand ILIKE ${param_index})")
            values.append(f"%{brand}%")
            param_index += 1
        
        if model:
            conditions.append(f"(cs.model_norm ILIKE ${param_index} OR c.model ILIKE ${param_index})")
            values.append(f"%{model}%")
            param_index += 1
        
        if variant:
            conditions.append(f"c.variant ILIKE ${param_index}")
            values.append(f"%{variant}%")
            param_index += 1
        
        if year:
            conditions.append(f"c.year = ${param_index}")
            values.append(year)
            param_index += 1

        # Add source filter if specified
        if source and source in ["mudahmy", "carlistmy"]:
            conditions.append(f"c.source = ${param_index}")
            values.append(source)
            param_index += 1

        # Always filter by status (active or sold only)
        conditions.append(f"c.status IN (${param_index}, ${param_index + 1})")
        values.extend(['active', 'sold'])
        param_index += 2

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        # Create query for unified table
        limit_param = param_index
        offset_param = param_index + 1
        
        sort_column = "information_ads_date" if sort_by == "ads_date" else "last_scraped_at"
        sort_order = "ASC" if sort_direction.lower() == "asc" else "DESC"
        
        final_query = f"""
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
            FROM {TB_UNIFIED} c
            LEFT JOIN {TB_CARS_STANDARD} cs ON c.cars_standard_id = cs.id
            WHERE {where_clause}
            ORDER BY c.{sort_column} {sort_order} NULLS LAST, c.id DESC
            LIMIT ${limit_param} OFFSET ${offset_param}
        """

        values.extend([limit, offset])
        result = await conn.fetch(final_query, *values)

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
) -> int:
    conn = await get_local_db_connection()
    try:
        conditions = []
        values = []
        param_index = 1  

        if brand:
            conditions.append(f"(cs.brand_norm ILIKE ${param_index} OR c.brand ILIKE ${param_index})")
            values.append(f"%{brand}%")
            param_index += 1
        
        if model:
            conditions.append(f"(cs.model_norm ILIKE ${param_index} OR c.model ILIKE ${param_index})")
            values.append(f"%{model}%")
            param_index += 1
        
        if variant:
            conditions.append(f"(cs.variant_norm ILIKE ${param_index} OR c.variant ILIKE ${param_index})")
            values.append(f"%{variant}%")
            param_index += 1
        
        if year:
            conditions.append(f"c.year = ${param_index}")
            values.append(year)
            param_index += 1

        # Add source filter if specified
        if source and source in ["mudahmy", "carlistmy"]:
            conditions.append(f"c.source = ${param_index}")
            values.append(source)
            param_index += 1

        # Always filter by status (active or sold only)
        conditions.append(f"c.status IN (${param_index}, ${param_index + 1})")
        values.extend(['active', 'sold'])
        param_index += 2

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        count_query = f"""
            SELECT COUNT(*) AS total
            FROM {TB_UNIFIED} c
            LEFT JOIN {TB_CARS_STANDARD} cs ON c.cars_standard_id = cs.id
            WHERE {where_clause}
        """
        
        result = await conn.fetchval(count_query, *values)
        return result

    finally:
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
                    c.last_scraped_at AS created_at
                FROM {TB_UNIFIED} c
                WHERE c.status IN ('active', 'sold')

                UNION ALL

                SELECT
                    co.id,
                    co.source,
                    co.brand,
                    co.model,
                    co.variant,
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
            LEFT JOIN {TB_CARS_STANDARD} cs ON c.cars_standard_id = cs.id
            WHERE c.id = $1
        """

        if source and source != "carsome":
            unified_params.append(source)
            unified_query += " AND c.source = $2"

        row = None
        if source != "carsome":
            row = await conn.fetchrow(unified_query, *unified_params)

        if row:
            return {
                'id': row['id'],
                'source': row['source'],
                'listing_url': row['listing_url'],
                'brand': row['brand'],
                'model': row['model'],
                'variant': row['variant'],
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
                'images': row['images'],
                'status': row['status'],
                'ads_tag': row['ads_tag'],
                'last_scraped_at': row['last_scraped_at'].isoformat() if row['last_scraped_at'] else None,
                'information_ads_date': row['information_ads_date'].isoformat() if row['information_ads_date'] else None,
                'created_at': row['last_scraped_at'].isoformat() if row['last_scraped_at'] else None,
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
            LEFT JOIN {TB_CARS_STANDARD} cs ON co.cars_standard_id = cs.id
            WHERE co.id = $1
        """

        carsome_row = await conn.fetchrow(carsome_query, *carsome_params)

        if carsome_row:
            created_at = carsome_row['created_at']
            created_at_iso = created_at.isoformat() if created_at else None
            last_updated = carsome_row['last_updated_at']
            last_updated_iso = last_updated.isoformat() if last_updated else created_at_iso

            return {
                'id': carsome_row['id'],
                'source': carsome_row['source'] or 'carsome',
                'listing_url': None,
                'brand': carsome_row['brand'],
                'model': carsome_row['model'],
                'variant': carsome_row['variant'],
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
                'images': carsome_row['image'],
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
                SELECT c.id, c.cars_standard_id, c.brand, c.model
                FROM {TB_UNIFIED} c
                WHERE c.status IN ('active', 'sold')

                UNION ALL

                SELECT co.id, co.cars_standard_id, co.brand, co.model
                FROM {TB_CARSOME} co
                WHERE co.status IN ('active', 'sold')
            )
        """

        car_count_query = f"""
            {combined_cte}
            SELECT COUNT(*) FROM combined
        """
        brand_count_query = f"""
            {combined_cte}
            SELECT COUNT(DISTINCT COALESCE(cs.brand_norm, combined.brand))
            FROM combined
            LEFT JOIN {TB_CARS_STANDARD} cs ON combined.cars_standard_id = cs.id
            WHERE COALESCE(cs.brand_norm, combined.brand) IS NOT NULL
        """
        model_count_query = f"""
            {combined_cte}
            SELECT COUNT(DISTINCT COALESCE(cs.model_norm, combined.model))
            FROM combined
            LEFT JOIN {TB_CARS_STANDARD} cs ON combined.cars_standard_id = cs.id
            WHERE COALESCE(cs.model_norm, combined.model) IS NOT NULL
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
                    FROM {TB_UNIFIED}
                    WHERE information_ads_date = $1
                      AND status IN ('active', 'sold')
                )
                +
                (
                    SELECT COUNT(*)
                    FROM {TB_CARSOME}
                    WHERE DATE(created_at) = $1
                      AND status IN ('active', 'sold')
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
                    COALESCE(cs.brand_norm, c.brand) AS brand_name
                FROM {TB_UNIFIED} c
                LEFT JOIN {TB_CARS_STANDARD} cs ON c.cars_standard_id = cs.id
                WHERE c.status IN ('active', 'sold')

                UNION ALL

                SELECT 
                    COALESCE(cs.brand_norm, co.brand) AS brand_name
                FROM {TB_CARSOME} co
                LEFT JOIN {TB_CARS_STANDARD} cs ON co.cars_standard_id = cs.id
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
