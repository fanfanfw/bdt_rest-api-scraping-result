import logging
import asyncpg
import os
import re
import pandas as pd
import matplotlib.pyplot as plt
import io
import numpy as np
import secrets
from datetime import datetime, timedelta
from typing import Optional, List, Tuple
from fastapi import HTTPException
from app.database import get_local_db_connection
from app.models import BrandCount,APIKeyCreateRequest, APIKeyCreateResponse

logger = logging.getLogger(__name__)

DB_CARLISTMY = os.getenv("DB_CARLISTNY", "scrap_carlistmy_old")
DB_CARLISTMY_USERNAME = os.getenv("DB_CARLISTNY_USERNAME", "fanfan")
DB_CARLISTMY_PASSWORD = os.getenv("DB_CARLISTNY_PASSWORD", "cenanun")
DB_CARLISTMY_HOST = os.getenv("DB_CARLISTMY_HOST", "192.168.1.207")
DB_MUDAHMY = os.getenv("DB_MUDAHMY", "scrap_mudahmy")
DB_MUDAHMY_USERNAME = os.getenv("DB_MUDAHMY_USERNAME", "fanfan")
DB_MUDAHMY_PASSWORD = os.getenv("DB_MUDAHMY_PASSWORD", "cenanun")
DB_MUDAHMY_HOST = os.getenv("DB_MUDAHMY_HOST", "192.168.1.207")
TB_CARLISTMY = os.getenv("TB_CARLISTMY", "cars_carlistmy")
TB_MUDAHMY = os.getenv("TB_MUDAHMY", "cars_mudahmy")
TB_PRICE_HISTORY_MUDAHMY = os.getenv("TB_PRICE_HISTORY_MUDAHMY", "price_history_mudahmy")
TB_PRICE_HISTORY_CARLISTMY = os.getenv("TB_PRICE_HISTORY_CARLISTMY", "price_history_carlistmy")

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

async def fetch_data_from_remote_db(conn, source=None):
    """
    Mengambil data dari database remote berdasarkan sumber.
    """
    if source == 'carlistmy':
        query = """
            SELECT 
                id, listing_url, brand, model, variant, informasi_iklan, 
                lokasi, price, year, millage as mileage, transmission, 
                seat_capacity, gambar, last_scraped_at, version, created_at, 
                sold_at, status
            FROM public.cars
            WHERE last_scraped_at >= NOW() - INTERVAL '30 days'
        """
    elif source == 'mudahmy':
        query = """
            SELECT 
                id, listing_url, brand, model, variant, informasi_iklan, 
                lokasi, price, year, millage as mileage, transmission, 
                seat_capacity, gambar, last_scraped_at, version, created_at, 
                sold_at, status
            FROM public.cars
            WHERE last_scraped_at >= NOW() - INTERVAL '30 days'
        """
    else:
        raise HTTPException(status_code=400, detail="Invalid source specified")
        
    rows = await conn.fetch(query)  
    return rows

async def get_remote_db_connection(db_name, db_user, db_host, db_password):
    conn = await asyncpg.connect(
        user=db_user,
        password=db_password,
        database=db_name,
        host=db_host
    )
    return conn

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

async def insert_or_update_data_into_local_db(data, table_name, source):
    from tqdm import tqdm
    import json

    conn = await get_local_db_connection()
    skipped_records = []
    inserted_count = 0
    skipped_count = 0

    try:
        print(f"\n🚀 Memulai proses insert/update untuk {source.upper()}...")
        for row in tqdm(data, desc=f"Inserting {source}"):
            id_ = row[0]
            listing_url = row[1]
            brand = clean_and_standardize_brand(row[2]) if row[2] else None 
            model = clean_and_standardize_variant(row[3])
            variant = clean_and_standardize_variant(row[4])
            informasi_iklan = row[5]
            lokasi = row[6]
            price = row[7]
            year = row[8]
            mileage = row[9]
            transmission = row[10]
            seat_capacity = row[11]
            gambar = row[12]
            last_scraped_at = parse_datetime(row[13])
            version = row[14]
            created_at = parse_datetime(row[15])
            sold_at = parse_datetime(row[16])
            status = row[17]

            price_int = convert_price(price)
            year_int = int(year) if year else None
            mileage_int = convert_mileage(mileage)

            if not all([brand, model, variant, price_int, mileage_int, year_int]):
                skipped_count += 1
                skipped_records.append({
                    "source": source,
                    "id": id_,
                    "brand": brand,
                    "model": model,
                    "variant": variant,
                    "price": price,
                    "year": year,
                    "mileage": mileage,
                    "reason": "Incomplete data"
                })
                continue

            if isinstance(gambar, str):
                try:
                    gambar = json.loads(gambar)
                except Exception:
                    gambar = []
            if not isinstance(gambar, list):
                gambar = []

            query_check = f"SELECT cars_standard_id FROM {table_name} WHERE id = $1"
            existing_standard_id = await conn.fetchval(query_check, id_)

            cars_standard_id = existing_standard_id
            if not existing_standard_id:
                norm_query = """
                    SELECT id FROM cars_standard
                    WHERE UPPER(brand_norm) = $1
                      AND (UPPER(model_norm) = $2 OR UPPER(model_raw) = $2)
                      AND $3 IN (UPPER(variant_norm), UPPER(variant_raw), UPPER(variant_raw2))
                    LIMIT 1
                """
                norm_result = await conn.fetchrow(norm_query, brand, model, variant)
                if norm_result:
                    cars_standard_id = norm_result['id']

            raw1 = None
            raw2 = None
            iklan_date = None
            
            if source == 'mudahmy':
                # Skip if URGENT untuk mudahmy
                if informasi_iklan and informasi_iklan.strip().upper() == "URGENT":
                    skipped_count += 1
                    skipped_records.append({
                        "source": source,
                        "id": id_,
                        "reason": "URGENT listing"
                    })
                    continue
                    
                # Convert informasi_iklan untuk mudahmy
                raw1, raw2, iklan_date = convert_informasi_iklan(informasi_iklan, last_scraped_at)
            elif source == 'carlistmy':
                # Convert informasi_iklan untuk carlistmy
                raw1, raw2, iklan_date = convert_informasi_iklan_carlistmy(informasi_iklan)
            elif source == 'carlistmy':
                # Convert informasi_iklan untuk carlistmy
                raw1, raw2, iklan_date = convert_informasi_iklan_carlistmy(informasi_iklan)

            # Insert/Update
            # Query berbeda untuk mudahmy dan carlistmy
            if source == 'mudahmy':
                await conn.execute(f"""
                    INSERT INTO {table_name} (
                        id, listing_url, brand, model, variant, informasi_iklan,
                        informasi_iklan_raw1, informasi_iklan_raw2, informasi_iklan_date,
                        lokasi, price, year, mileage, transmission, seat_capacity,
                        gambar, last_scraped_at, version, created_at, sold_at, status,
                        cars_standard_id, source
                    )
                    VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                        $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
                        $21, $22, $23
                    )
                    ON CONFLICT (id) DO UPDATE SET
                    listing_url = EXCLUDED.listing_url,
                    brand = EXCLUDED.brand,
                    model = EXCLUDED.model,
                    variant = EXCLUDED.variant,
                    informasi_iklan = EXCLUDED.informasi_iklan,
                    informasi_iklan_raw1 = EXCLUDED.informasi_iklan_raw1,
                    informasi_iklan_raw2 = EXCLUDED.informasi_iklan_raw2,
                    informasi_iklan_date = EXCLUDED.informasi_iklan_date,
                    lokasi = EXCLUDED.lokasi,
                    price = EXCLUDED.price,
                    year = EXCLUDED.year,
                    mileage = EXCLUDED.mileage,
                    transmission = EXCLUDED.transmission,
                    seat_capacity = EXCLUDED.seat_capacity,
                    gambar = EXCLUDED.gambar,
                    last_scraped_at = EXCLUDED.last_scraped_at,
                    version = EXCLUDED.version,
                    created_at = EXCLUDED.created_at,
                    sold_at = EXCLUDED.sold_at,
                    status = EXCLUDED.status,
                    cars_standard_id = COALESCE(EXCLUDED.cars_standard_id, {table_name}.cars_standard_id),
                    source = EXCLUDED.source
            """,
            id_, listing_url, brand, model, variant, informasi_iklan,
            raw1, raw2, iklan_date,
            lokasi, price_int, year_int, mileage_int, transmission, seat_capacity,
            gambar, last_scraped_at, version, created_at, sold_at, status,
            cars_standard_id, source)
            elif source == 'carlistmy':
                await conn.execute(f"""
                    INSERT INTO {table_name} (
                        id, listing_url, brand, model, variant, informasi_iklan,
                        informasi_iklan_raw1, informasi_iklan_raw2, informasi_iklan_date,
                        lokasi, price, year, mileage, transmission, seat_capacity,
                        gambar, last_scraped_at, version, created_at, sold_at, status,
                        cars_standard_id, source
                    )
                    VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                        $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
                        $21, $22, $23
                    )
                    ON CONFLICT (id) DO UPDATE SET
                    listing_url = EXCLUDED.listing_url,
                    brand = EXCLUDED.brand,
                    model = EXCLUDED.model,
                    variant = EXCLUDED.variant,
                    informasi_iklan = EXCLUDED.informasi_iklan,
                    informasi_iklan_raw1 = EXCLUDED.informasi_iklan_raw1,
                    informasi_iklan_raw2 = EXCLUDED.informasi_iklan_raw2,
                    informasi_iklan_date = EXCLUDED.informasi_iklan_date,
                    lokasi = EXCLUDED.lokasi,
                    price = EXCLUDED.price,
                    year = EXCLUDED.year,
                    mileage = EXCLUDED.mileage,
                    transmission = EXCLUDED.transmission,
                    seat_capacity = EXCLUDED.seat_capacity,
                    gambar = EXCLUDED.gambar,
                    last_scraped_at = EXCLUDED.last_scraped_at,
                    version = EXCLUDED.version,
                    created_at = EXCLUDED.created_at,
                    sold_at = EXCLUDED.sold_at,
                    status = EXCLUDED.status,
                    cars_standard_id = COALESCE(EXCLUDED.cars_standard_id, {table_name}.cars_standard_id),
                    source = EXCLUDED.source
            """,
            id_, listing_url, brand, model, variant, informasi_iklan,
            raw1, raw2, iklan_date,
            lokasi, price_int, year_int, mileage_int, transmission, seat_capacity,
            gambar, last_scraped_at, version, created_at, sold_at, status,
            cars_standard_id, source)

            inserted_count += 1

        # Save skipped records
        if skipped_records:
            skipped_df = pd.DataFrame(skipped_records)
            skipped_df.to_csv(f"skipped_{source}.csv", index=False)
            print(f"⚠️ {len(skipped_records)} data yang dilewati disimpan ke file skipped_{source}.csv")

        return inserted_count, skipped_count

    finally:
        await conn.close()

async def sync_data_from_remote():
    logger.info("🚀 Memulai proses sinkronisasi data dari remote database...")

    result_summary = {}

    # Sinkronisasi dengan CarlistMY
    logger.info("[CarlistMY] Membuka koneksi database remote...")
    remote_conn_carlistmy = await get_remote_db_connection(DB_CARLISTMY, DB_CARLISTMY_USERNAME, DB_CARLISTMY_HOST, DB_CARLISTMY_PASSWORD)
    logger.info("[CarlistMY] Berhasil terkoneksi.")
    
    data_carlistmy = await fetch_data_from_remote_db(remote_conn_carlistmy, 'carlistmy')
    logger.info(f"[CarlistMY] Total data yang diambil: {len(data_carlistmy)}")

    inserted_carlistmy, skipped_carlistmy = await insert_or_update_data_into_local_db(data_carlistmy, f'{TB_CARLISTMY}', 'carlistmy')
    logger.info(f"[CarlistMY] Inserted: {inserted_carlistmy}, Skipped: {skipped_carlistmy}")

    # Sinkronisasi price_history dari CarlistMY
    data_price_history_carlistmy = await fetch_price_history_from_remote_db(remote_conn_carlistmy, 'carlistmy')
    await insert_or_update_price_history(data_price_history_carlistmy, f'{TB_PRICE_HISTORY_CARLISTMY}')
    logger.info("[CarlistMY] Sinkronisasi price_history selesai.")

    result_summary['carlistmy'] = {
        'total_fetched': len(data_carlistmy),
        'inserted': inserted_carlistmy,
        'skipped': skipped_carlistmy
    }

    # Sinkronisasi dengan MudahMY
    logger.info("[MudahMY] Membuka koneksi database remote...")
    remote_conn_mudahmy = await get_remote_db_connection(DB_MUDAHMY, DB_MUDAHMY_USERNAME, DB_MUDAHMY_HOST, DB_MUDAHMY_PASSWORD)
    logger.info("[MudahMY] Berhasil terkoneksi.")
    
    data_mudahmy = await fetch_data_from_remote_db(remote_conn_mudahmy, 'mudahmy')
    logger.info(f"[MudahMY] Total data yang diambil: {len(data_mudahmy)}")

    inserted_mudahmy, skipped_mudahmy = await insert_or_update_data_into_local_db(data_mudahmy, f'{TB_MUDAHMY}', 'mudahmy')
    logger.info(f"[MudahMY] Inserted: {inserted_mudahmy}, Skipped: {skipped_mudahmy}")

    # Sinkronisasi price_history dari MudahMY
    data_price_history_mudahmy = await fetch_price_history_from_remote_db(remote_conn_mudahmy, 'mudahmy')
    await insert_or_update_price_history(data_price_history_mudahmy, f'{TB_PRICE_HISTORY_MUDAHMY}')
    logger.info("[MudahMY] Sinkronisasi price_history selesai.")

    result_summary['mudahmy'] = {
        'total_fetched': len(data_mudahmy),
        'inserted': inserted_mudahmy,
        'skipped': skipped_mudahmy
    }

    logger.info("✅ Proses sinkronisasi semua sumber selesai.")
    result_summary["status"] = "success"
    return result_summary

async def fetch_price_history_from_remote_db(conn, source):
    """
    Mengambil data price history berdasarkan sumbernya, apakah carlistmy atau mudahmy.
    """
    if source == 'carlistmy':
        query = """
            SELECT car_id, old_price, new_price, changed_at 
            FROM public.price_history_combined
            WHERE changed_at >= NOW() - INTERVAL '30 days'
        """
    elif source == 'mudahmy':
        query = """
            SELECT car_id, old_price, new_price, changed_at 
            FROM public.price_history_combined
            WHERE changed_at >= NOW() - INTERVAL '30 days'
        """
    else:
        raise HTTPException(status_code=400, detail="Invalid source for price history.")

    rows = await conn.fetch(query)
    return rows

async def insert_or_update_price_history(data, table_name):
    conn = await get_local_db_connection()
    try:
        if table_name == f"{TB_PRICE_HISTORY_CARLISTMY}":
            cars_table = TB_CARLISTMY
        elif table_name == f"{TB_PRICE_HISTORY_MUDAHMY}":
            cars_table = TB_MUDAHMY
        else:
            raise Exception("Unknown table name for price history.")

        existing_ids = await conn.fetch(f"SELECT id FROM {cars_table}")
        existing_ids_set = set(row['id'] for row in existing_ids)

        inserted = 0
        skipped = 0

        for row in data:
            car_id = row['car_id']
            old_price = row['old_price']
            new_price = row['new_price']
            changed_at = row['changed_at']

            if car_id not in existing_ids_set:
                skipped += 1
                continue

            await conn.execute(f"""
                INSERT INTO {table_name} (car_id, old_price, new_price, changed_at)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (car_id, changed_at) 
                DO UPDATE SET 
                    old_price = EXCLUDED.old_price,
                    new_price = EXCLUDED.new_price,
                    changed_at = EXCLUDED.changed_at
            """, car_id, old_price, new_price, changed_at)
            inserted += 1

        logger.info(f"[{table_name}] Inserted {inserted} records, Skipped {skipped} records due to missing cars.")

    finally:
        await conn.close()

async def get_brand_distribution_carlistmy() -> List[BrandCount]:
    """
    Mengembalikan jumlah listing untuk setiap brand di tabel cars_carlistmy.
    Urutkan dari brand dengan listing terbanyak ke paling sedikit.
    """
    conn = await get_local_db_connection()
    try:
        query = f"""
            SELECT brand, COUNT(*) AS total
            FROM {TB_CARLISTMY}
            WHERE brand IS NOT NULL
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
    offset: int = 0
) -> List[dict]:
    conn = await get_local_db_connection()
    try:
        if source and source in ["mudahmy", "carlistmy"]:
            tables = [f"cars_{source}"]
        else:
            tables = ["cars_mudahmy", "cars_carlistmy"]

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

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        queries = []
        for table in tables:
            query = f"""
                SELECT 
                    COALESCE(cs.brand_norm, c.brand) AS brand,
                    COALESCE(cs.model_norm, c.model) AS model,
                    COALESCE(cs.variant_norm, c.variant) AS variant,
                    c.price,
                    c.mileage,
                    c.year,
                    '{table.replace('cars_', '')}' AS source,
                    c.informasi_iklan AS ads_information
                FROM {table} c
                LEFT JOIN cars_standard cs ON c.cars_standard_id = cs.id
                WHERE {where_clause}
            """
            queries.append(query)

        final_query = " UNION ALL ".join(queries) + " ORDER BY brand, model, variant LIMIT $%d OFFSET $%d" % (param_index, param_index + 1)
        values.extend([limit, offset])

        rows = await conn.fetch(final_query, *values)
        
        return [
            {
                "brand": row["brand"],
                "model": row["model"],
                "variant": row["variant"],
                "price": row["price"],
                "mileage": row["mileage"],
                "year": row["year"],
                "source": row["source"],
                "ads_information": row["ads_information"]
            }
            for row in rows
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
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
        if source and source in ["mudahmy", "carlistmy"]:
            tables = [f"cars_{source}"]
        else:
            tables = ["cars_mudahmy", "cars_carlistmy"]

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

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        count_queries = []
        for table in tables:
            count_query = f"""
                SELECT COUNT(*) AS total
                FROM {table} c
                LEFT JOIN cars_standard cs ON c.cars_standard_id = cs.id
                WHERE {where_clause}
            """
            count_queries.append(count_query)

        final_count_query = " UNION ALL ".join(count_queries)
        rows = await conn.fetch(final_count_query, *values)

        total_count = sum(row['total'] for row in rows)
        return total_count

    finally:
        await conn.close()

def convert_informasi_iklan(informasi_iklan: str, last_scraped_at: datetime) -> tuple:
    """
    Convert informasi_iklan string into raw1, raw2, and date components for mudahmy data.
    
    Args:
        informasi_iklan: Original informasi_iklan string
        last_scraped_at: Timestamp when the data was scraped
    
    Returns:
        Tuple of (raw1, raw2, iklan_date)
    """
    if not informasi_iklan:
        return None, None, None

    # Split based on "posted"
    parts = informasi_iklan.split("posted")
    
    if len(parts) == 2:
        raw1 = parts[0].strip()
        raw2 = parts[1].strip()
    else:
        raw1 = ""
        raw2 = informasi_iklan.strip()
    
    # Convert the date part
    month_to_num = {
        'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05', 'Jun': '06',
        'Jul': '07', 'Aug': '08', 'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'
    }

    iklan_date = None
    
    # Handle "X days ago"
    if 'days ago' in raw2:
        try:
            days_ago = int(raw2.split()[0])
            iklan_date = last_scraped_at - timedelta(days=days_ago)
        except (ValueError, IndexError):
            pass

    # Handle "X day ago"
    elif 'day ago' in raw2:
        try:
            days_ago = int(raw2.split()[0])
            iklan_date = last_scraped_at - timedelta(days=days_ago)
        except (ValueError, IndexError):
            pass

    # Handle "X hours ago"
    elif 'hours ago' in raw2 or 'hour ago' in raw2:
        try:
            hours_ago = int(raw2.split()[0])
            iklan_date = last_scraped_at - timedelta(hours=hours_ago)
        except (ValueError, IndexError):
            pass

    # Handle "X mins ago"
    elif 'mins ago' in raw2 or 'min ago' in raw2:
        try:
            mins_ago = int(raw2.split()[0])
            iklan_date = last_scraped_at - timedelta(minutes=mins_ago)
        except (ValueError, IndexError):
            pass

    # Handle "DD Mon YYYY" or "DD Mon"
    else:
        parts = raw2.split()
        if len(parts) >= 2:
            try:
                day = int(parts[0])
                month = month_to_num.get(parts[1][:3].capitalize())
                year = int(parts[2]) if len(parts) > 2 else last_scraped_at.year

                if month and 1 <= day <= 31:
                    date_str = f"{year}-{month}-{day:02d}"
                    iklan_date = datetime.strptime(date_str, '%Y-%m-%d')
            except (ValueError, IndexError):
                pass

    return raw1, raw2, iklan_date

def convert_informasi_iklan_carlistmy(informasi_iklan: str) -> tuple:
    """
    Convert informasi_iklan string into raw1, raw2, and date components for carlistmy data.
    Format expected: "Updated on: April 02, 2025"
    
    Args:
        informasi_iklan: Original informasi_iklan string
    
    Returns:
        Tuple of (raw1, raw2, iklan_date)
    """
    if not informasi_iklan:
        return None, None, None

    month_to_num = {
        'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05', 'Jun': '06',
        'Jul': '07', 'Aug': '08', 'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'
    }

    raw1 = None
    raw2 = None
    iklan_date = None

    if informasi_iklan.startswith("Updated on: "):
        raw1 = "Updated on: "
        raw2 = informasi_iklan[len(raw1):]
    else:
        raw1 = ""
        raw2 = informasi_iklan

    try:
        parts = raw2.split()
        if len(parts) == 3:
            month_str, day_str, year = parts
            day_str = day_str.replace(',', '')
            
            month = month_to_num.get(month_str[:3], None)
            
            if month and day_str.isdigit() and year.isdigit():
                date_str = f"{year}-{month}-{int(day_str):02d}"
                iklan_date = datetime.strptime(date_str, '%Y-%m-%d')
    except (ValueError, IndexError):
        pass

    return raw1, raw2, iklan_date
