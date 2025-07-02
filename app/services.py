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

DB_CARLISTMY = os.getenv("DB_CARLISTMY", "db_scrap")
DB_CARLISTMY_USERNAME = os.getenv("DB_CARLISTMY_USERNAME", "fanfan")
DB_CARLISTMY_PASSWORD = os.getenv("DB_CARLISTMY_PASSWORD", "cenanun")
DB_CARLISTMY_HOST = os.getenv("DB_CARLISTMY_HOST", "192.168.1.207")
DB_MUDAHMY = os.getenv("DB_MUDAHMY", "db_scrap")
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
    if source == 'carlistmy':
        query = """
            SELECT 
                id, listing_url, condition, brand, model_group, model, variant,
                information_ads, location, price, year, mileage,
                transmission, seat_capacity, engine_cc, fuel_type,
                images, last_scraped_at, version, created_at, sold_at, 
                status, last_status_check, information_ads_date, ads_tag,
                is_deleted
            FROM public.cars_scrap_carlistmy
            ORDER BY information_ads_date DESC, last_scraped_at DESC
        """
    elif source == 'mudahmy':
        query = """
            SELECT 
                id, listing_url, condition, brand, model, variant,
                information_ads, location, price, year, mileage,
                transmission, seat_capacity, images, last_scraped_at,
                version, created_at, sold_at, status, last_status_check,
                information_ads_date
            FROM public.cars_scrap_mudahmy
            ORDER BY information_ads_date DESC, last_scraped_at DESC
        """
    else:
        raise HTTPException(status_code=400, detail="Invalid source specified")
        
    rows = await conn.fetch(query)  
    return rows

async def verify_remote_tables(conn):
    tables = await conn.fetch("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
    """)
    logger.info("📋 Daftar tabel di database remote:")
    for table in tables:
        logger.info(f"- {table['table_name']}")

async def get_remote_db_connection(db_name, db_user, db_host, db_password):
    logger.info(f"🔌 Mencoba terkoneksi ke: {db_user}@{db_host}/{db_name}")
    conn = await asyncpg.connect(
        user=db_user,
        password=db_password,
        database=db_name,
        host=db_host
    )
    # Verifikasi koneksi berhasil ke database yang benar
    db_name_connected = await conn.fetchval("SELECT current_database()")
    logger.info(f"✅ Terkoneksi ke database: {db_name_connected}")
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
            # Ambil semua kolom yang relevan (TANPA regex/cleaning, ambil apa adanya)
            id_ = row['id']
            listing_url = row['listing_url']
            condition = row.get('condition')
            brand = row.get('brand')  # tanpa strip/cleaning
            model = row.get('model')
            variant = row.get('variant')
            information_ads = row['information_ads']
            location = row['location']
            price = row['price']
            year = row['year']
            mileage = row['mileage']
            transmission = row['transmission']
            seat_capacity = row['seat_capacity']

            # Khusus carlistmy
            model_group = row.get('model_group') if source == 'carlistmy' else None
            
            # Konversi images dari list ke string JSON atau string biasa (dipisahkan koma)
            images = row['images']
            if isinstance(images, list):  
                images = json.dumps(images)  
            elif isinstance(images, str):  
                images = images
            else:
                images = None

            # Konversi tanggal - PERBAIKAN: Jangan skip data yang information_ads_date nya bukan hari ini
            last_scraped_at = parse_datetime(row['last_scraped_at'])
            version = row['version']
            created_at = parse_datetime(row['created_at'])
            sold_at = parse_datetime(row['sold_at'])
            status = row['status']
            
            # PERBAIKAN: Ambil information_ads_date dari sumber dan jangan filter berdasarkan tanggal hari ini
            information_ads_date = None
            if row.get('information_ads_date'):
                if isinstance(row['information_ads_date'], str):
                    try:
                        information_ads_date = datetime.strptime(row['information_ads_date'], "%Y-%m-%d").date()
                    except ValueError:
                        try:
                            information_ads_date = datetime.strptime(row['information_ads_date'], "%Y-%m-%d %H:%M:%S").date()
                        except ValueError:
                            information_ads_date = None
                elif hasattr(row['information_ads_date'], 'date'):
                    information_ads_date = row['information_ads_date'].date()
                else:
                    information_ads_date = row['information_ads_date']

            # HAPUS FILTER TANGGAL - Ambil semua data, tidak hanya hari ini
            # if information_ads_date and information_ads_date != datetime.today().date():
            #     skipped_count += 1
            #     continue

            # Konversi harga dan mileage
            price_int = convert_price(price)
            year_int = int(year) if year else None
            mileage_int = convert_mileage(mileage)

            # Pastikan field wajib tidak NULL
            if any(val is None for val in [brand, model, variant, mileage_int, year_int]):
                skipped_count += 1
                skipped_records.append({
                    "source": source,
                    "id": id_,
                    "brand": brand,
                    "model": model,
                    "variant": variant,
                    "mileage": mileage,
                    "year": year,
                    "reason": "Field is None"
                })
                continue

            # Cek untuk ID standar mobil berdasarkan brand, model_group, model, dan variant
            query_check = f"SELECT cars_standard_id FROM {table_name} WHERE id = $1"
            existing_standard_id = await conn.fetchval(query_check, id_)

            cars_standard_id = existing_standard_id
            if not existing_standard_id:
                # Query normalisasi baru: cek brand, model_group, model, variant secara berurutan
                norm_query = """
                    SELECT id FROM cars_standard
                    WHERE brand_norm = $1
                      AND ($2 IN (model_group_norm, model_group_raw))
                      AND ($3 IN (model_norm, model_raw))
                      AND ($4 IN (variant_norm, variant_raw, variant_raw2))
                    LIMIT 1
                """
                norm_result = await conn.fetchrow(norm_query, brand, model_group, model, variant)
                if norm_result:
                    cars_standard_id = norm_result['id']

            # Menyusun query INSERT atau UPDATE
            if source == 'mudahmy':
                query = f"""
                    INSERT INTO {table_name} (
                        id, listing_url, condition, brand, model, variant, information_ads,
                        location, price, year, mileage, transmission, seat_capacity,
                        images, last_scraped_at, version, created_at, sold_at, status,
                        cars_standard_id, source, information_ads_date
                    )
                    VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                        $11, $12, $13, $14, $15, $16, $17, $18, $19,
                        $20, $21, $22
                    )
                    ON CONFLICT (id) DO UPDATE SET
                        listing_url = EXCLUDED.listing_url,
                        condition = EXCLUDED.condition,
                        brand = EXCLUDED.brand,
                        model = EXCLUDED.model,
                        variant = EXCLUDED.variant,
                        information_ads = EXCLUDED.information_ads,
                        location = EXCLUDED.location,
                        price = EXCLUDED.price,
                        year = EXCLUDED.year,
                        mileage = EXCLUDED.mileage,
                        transmission = EXCLUDED.transmission,
                        seat_capacity = EXCLUDED.seat_capacity,
                        images = EXCLUDED.images,
                        last_scraped_at = EXCLUDED.last_scraped_at,
                        version = EXCLUDED.version,
                        created_at = EXCLUDED.created_at,
                        sold_at = EXCLUDED.sold_at,
                        status = EXCLUDED.status,
                        cars_standard_id = COALESCE(EXCLUDED.cars_standard_id, {table_name}.cars_standard_id),
                        source = EXCLUDED.source,
                        information_ads_date = EXCLUDED.information_ads_date
                """
                params = [
                    id_, listing_url, condition, brand, model, variant, information_ads,
                    location, price_int, year_int, mileage_int, transmission, seat_capacity,
                    images, last_scraped_at, version, created_at, sold_at, status,
                    cars_standard_id, source, information_ads_date
                ]
            elif source == 'carlistmy':
                query = f"""
                    INSERT INTO {table_name} (
                        id, listing_url, condition, brand, model_group, model, variant,
                        information_ads, location, price, year, mileage,
                        transmission, seat_capacity, images, last_scraped_at,
                        version, created_at, sold_at, status,
                        cars_standard_id, source, information_ads_date
                    )
                    VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                        $11, $12, $13, $14, $15, $16, $17, $18, $19,
                        $20, $21, $22, $23
                    )
                    ON CONFLICT (id) DO UPDATE SET
                        listing_url = EXCLUDED.listing_url,
                        condition = EXCLUDED.condition,
                        brand = EXCLUDED.brand,
                        model_group = EXCLUDED.model_group,
                        model = EXCLUDED.model,
                        variant = EXCLUDED.variant,
                        information_ads = EXCLUDED.information_ads,
                        location = EXCLUDED.location,
                        price = EXCLUDED.price,
                        year = EXCLUDED.year,
                        mileage = EXCLUDED.mileage,
                        transmission = EXCLUDED.transmission,
                        seat_capacity = EXCLUDED.seat_capacity,
                        images = EXCLUDED.images,
                        last_scraped_at = EXCLUDED.last_scraped_at,
                        version = EXCLUDED.version,
                        created_at = EXCLUDED.created_at,
                        sold_at = EXCLUDED.sold_at,
                        status = EXCLUDED.status,
                        cars_standard_id = COALESCE(EXCLUDED.cars_standard_id, {table_name}.cars_standard_id),
                        source = EXCLUDED.source,
                        information_ads_date = EXCLUDED.information_ads_date
                """
                params = [
                    id_, listing_url, condition, brand, model_group, model, variant,
                    information_ads, location, price_int, year_int, mileage_int,
                    transmission, seat_capacity, images, last_scraped_at,
                    version, created_at, sold_at, status,
                    cars_standard_id, source, information_ads_date
                ]

            await conn.execute(query, *params)
            inserted_count += 1

        # Save skipped records
        if skipped_records:
            skipped_df = pd.DataFrame(skipped_records)
            skipped_df.to_csv(f"skipped_{source}.csv", index=False)
            print(f"⚠️ {len(skipped_records)} data yang dilewati disimpan ke file skipped_{source}.csv")

        return inserted_count, skipped_count

    finally:
        await conn.close()

async def get_id_mapping(conn_source, conn_target, table_source, table_target):
    rows_source = await conn_source.fetch(f"SELECT id, listing_url FROM {table_source}")
    rows_target = await conn_target.fetch(f"SELECT id, listing_url FROM {table_target}")
    source_map = {row['listing_url']: row['id'] for row in rows_source}
    target_map = {row['listing_url']: row['id'] for row in rows_target}
    id_map = {}
    for url, source_id in source_map.items():
        target_id = target_map.get(url)
        if target_id:
            id_map[source_id] = target_id
    return id_map

async def insert_or_update_price_history_by_listing_url(conn_source, conn_target, table_price_history_source, table_price_history_target):
    rows = await conn_source.fetch(f"SELECT listing_url, old_price, new_price, changed_at FROM {table_price_history_source}")
    inserted = 0
    skipped = 0

    # Ambil semua listing_url di target
    rows_cars = await conn_target.fetch(f"SELECT listing_url FROM {table_price_history_target.replace('price_history_', 'cars_')}")
    existing_urls = set(row['listing_url'] for row in rows_cars)

    for row in rows:
        listing_url = row['listing_url']
        if listing_url not in existing_urls:
            skipped += 1
            continue
            
        # Determine which constraint name to use based on the table
        if "mudahmy" in table_price_history_target:
            conflict_constraint = "unique_listing_url_changed_at_mudah"
        else:
            conflict_constraint = "unique_listing_url_changed_at"

        # Insert or update the price history
        await conn_target.execute(f"""
            INSERT INTO {table_price_history_target} (listing_url, old_price, new_price, changed_at)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT ON CONSTRAINT {conflict_constraint}
            DO UPDATE SET 
                old_price = EXCLUDED.old_price, 
                new_price = EXCLUDED.new_price
        """, listing_url, row['old_price'], row['new_price'], row['changed_at'])
        inserted += 1

    logger.info(f"[{table_price_history_target}] Inserted {inserted} records, Skipped {skipped} records due to missing listing_url.")
    return inserted, skipped

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

    # Sinkronisasi price_history dari CarlistMY berdasarkan listing_url
    local_conn = await get_local_db_connection()
    await insert_or_update_price_history_by_listing_url(
        remote_conn_carlistmy, local_conn,
        'price_history_scrap_carlistmy', TB_PRICE_HISTORY_CARLISTMY
    )
    await local_conn.close()
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

    # Sinkronisasi price_history dari MudahMY berdasarkan listing_url
    local_conn2 = await get_local_db_connection()
    await insert_or_update_price_history_by_listing_url(
        remote_conn_mudahmy, local_conn2,
        'price_history_scrap_mudahmy', TB_PRICE_HISTORY_MUDAHMY
    )
    await local_conn2.close()
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
            FROM public.price_history_scrap_carlistmy
        """
    elif source == 'mudahmy':
        query = """
            SELECT car_id, old_price, new_price, changed_at 
            FROM public.price_history_scrap_mudahmy
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
    offset: int = 0,
    sort_by: str = "scraped_at",
    sort_direction: str = "desc"
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
            conditions.append(f"c.variant ILIKE ${param_index}")
            values.append(f"%{variant}%")
            param_index += 1
        
        if year:
            conditions.append(f"c.year = ${param_index}")
            values.append(year)
            param_index += 1

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        # Create individual queries for each table
        select_queries = []
        for table in tables:
            source_name = table.replace("cars_", "")
            select_queries.append(f"""
                SELECT 
                    c.id,
                    c.brand,
                    c.model,
                    c.variant,
                    c.price,
                    c.mileage,
                    c.year,
                    '{source_name}' as source,
                    c.last_scraped_at as scraped_at,
                    c.information_ads_date as ads_date,
                    COALESCE(c.information_ads_date, c.last_scraped_at) as sort_date
                FROM {table} c
                LEFT JOIN cars_standard cs ON c.cars_standard_id = cs.id
                WHERE {where_clause}
            """)

        # Combine queries with UNION ALL and add global sorting
        limit_param = param_index
        offset_param = param_index + 1
        
        sort_column = "sort_date" if sort_by == "ads_date" else "scraped_at"
        sort_order = "ASC" if sort_direction.lower() == "asc" else "DESC"
        
        final_query = f"""
            WITH combined_data AS (
                {" UNION ALL ".join(select_queries)}
            )
            SELECT 
                brand,
                model,
                variant,
                price,
                mileage,
                year,
                source,
                scraped_at,
                ads_date
            FROM combined_data
            ORDER BY {sort_column} {sort_order} NULLS LAST, id DESC
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
