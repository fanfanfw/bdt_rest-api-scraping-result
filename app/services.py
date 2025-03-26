import logging
import asyncpg
import os
import re
import json
import numpy as np 
import math
from datetime import datetime
from typing import Optional, List
from fastapi import HTTPException
from app.config import DATABASE_URL
from sklearn.preprocessing import RobustScaler
from app.database import get_local_db_connection
from app.models import (
    RankPriceResponse, RankCarResponse, SearchCarsResponse, CarMudahMy, CarCarlistMy, 
    SearchCarsCarlistMyResponse, BrandCount, PriceSummary, LocationCount,
    BrandCount, PriceSummary, LocationCount
)
logger = logging.getLogger(__name__)

DB_CARLISTMY = os.getenv("DB_CARLISTNY", "scrap_carlistmy")
DB_CARLISTMY_USERNAME = os.getenv("DB_CARLISTNY_USER", "fanfan")
DB_CARLISTMY_PASSWORD = os.getenv("DB_CARLISTNY_PASSWORD", "cenanun")
DB_CARLISTMY_HOST = os.getenv("DB_CARLISTMY_HOST", "192.168.1.207")
DB_MUDAHMY = os.getenv("DB_MUDAHMY", "scrap_mudahmy")
DB_MUDAHMY_USERNAME = os.getenv("DB_MUDAHMY_USER", "funfun")
DB_MUDAHMY_PASSWORD = os.getenv("DB_MUDAHMY_PASSWORD", "cenanun")
DB_MUDAHMY_HOST = os.getenv("DB_MUDAHMY_HOST", "47.236.125.23")

def convert_price(price_str):
    if isinstance(price_str, int):
        return price_str  
    if isinstance(price_str, str) and 'RM' in price_str:
        return int(price_str.replace('RM', '').replace(',', '').strip())
    return None

def convert_millage(millage_str):
    if isinstance(millage_str, int):
        return millage_str  
    if isinstance(millage_str, str):
        numbers = re.findall(r'\d+', millage_str)
        if numbers:
            millage_value = int(numbers[-1])
            
            if millage_value >= 1000:
                return millage_value
            else:
                return millage_value * 1000
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

async def fetch_data_from_remote_db(conn):
    query = "SELECT * FROM public.cars"
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

async def fetch_brands_models_variants_by_source(source: str):
    conn = await get_local_db_connection()
    
    if source == "mudahmy":
        table_name = "cars_mudahmy"
    elif source == "carlistmy":
        table_name = "cars_carlistmy"
    else:
        await conn.close()
        raise HTTPException(status_code=400, detail="Invalid source")
    
    query = f"""
        SELECT DISTINCT brand, model, variant
        FROM {table_name}
        WHERE brand IS NOT NULL AND model IS NOT NULL AND variant IS NOT NULL;
    """
    rows = await conn.fetch(query)
    await conn.close()
    
    return [{"brand": row["brand"], "model": row["model"], "variant": row["variant"]} for row in rows]

async def insert_or_update_data_into_local_db(data, table_name, source):
    conn = await get_local_db_connection()
    try:
        for row in data:
            id_ = row[0]
            listing_url = row[1]
            brand = row[2]
            model = row[3]
            variant = row[4]
            informasi_iklan = row[5]
            lokasi = row[6]
            price = row[7]
            year = row[8]
            millage = row[9]
            transmission = row[10]
            seat_capacity = row[11]
            gambar = row[12]  
            last_scraped_at = parse_datetime(row[13])
            version = row[14]
            created_at = parse_datetime(row[15])
            sold_at = parse_datetime(row[16])
            status = row[17]
            previous_price = row[18]

            price_int = convert_price(price)
            year_int = int(year) if year else None  
            millage_int = convert_millage(millage)

            if isinstance(gambar, str):
                gambar = json.loads(gambar)  

            if not isinstance(gambar, list):
                gambar = []  

            await conn.execute(f"""
                INSERT INTO {table_name} (
                    id, listing_url, brand, model, variant, informasi_iklan,
                    lokasi, price, year, millage, transmission, seat_capacity,
                    gambar, last_scraped_at, version, created_at, sold_at, status,
                    previous_price, source
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
                    $13, $14, $15, $16, $17, $18, $19, $20
                )
                ON CONFLICT (id)
                DO UPDATE SET
                    listing_url = EXCLUDED.listing_url,
                    brand = EXCLUDED.brand,
                    model = EXCLUDED.model,
                    variant = EXCLUDED.variant,
                    informasi_iklan = EXCLUDED.informasi_iklan,
                    lokasi = EXCLUDED.lokasi,
                    price = EXCLUDED.price,
                    year = EXCLUDED.year,
                    millage = EXCLUDED.millage,
                    transmission = EXCLUDED.transmission,
                    seat_capacity = EXCLUDED.seat_capacity,
                    gambar = EXCLUDED.gambar,
                    last_scraped_at = EXCLUDED.last_scraped_at,
                    version = EXCLUDED.version,
                    created_at = EXCLUDED.created_at,
                    sold_at = EXCLUDED.sold_at,
                    status = EXCLUDED.status,
                    previous_price = EXCLUDED.previous_price,
                    source = EXCLUDED.source
            """, 
            id_, listing_url, brand, model, variant, informasi_iklan,
            lokasi, price_int, year_int, millage_int, transmission, seat_capacity,
            gambar, last_scraped_at, version, created_at, sold_at, status,
            previous_price, source)
    finally:
        await conn.close()

async def sync_data_from_remote():
    logger.info("Proses sinkronisasi dimulai...")
    
    remote_conn_carlistmy = await get_remote_db_connection(f'{DB_CARLISTMY}', f'{DB_CARLISTMY_USERNAME}', f'{DB_CARLISTMY_HOST}', f'{DB_CARLISTMY_PASSWORD}')
    logger.info("Koneksi ke CarlistMY berhasil.")
    data_carlistmy = await fetch_data_from_remote_db(remote_conn_carlistmy) 
    await insert_or_update_data_into_local_db(data_carlistmy, 'cars_carlistmy', 'carlistmy')  
    
    remote_conn_mudahmy = await get_remote_db_connection(f'{DB_MUDAHMY}', f'{DB_MUDAHMY_USERNAME}', f'{DB_MUDAHMY_HOST}', f'{DB_MUDAHMY_PASSWORD}')
    logger.info("Koneksi ke MudahMY berhasil.")
    data_mudahmy = await fetch_data_from_remote_db(remote_conn_mudahmy)  
    await insert_or_update_data_into_local_db(data_mudahmy, 'cars_mudahmy', 'mudahmy')  
    
    logger.info("Menyalin data price_history CarlistMY...")
    data_price_history_carlistmy = await fetch_price_history_from_remote_db(remote_conn_carlistmy)
    await insert_or_update_price_history(data_price_history_carlistmy, 'price_history_carlistmy')

    logger.info("Menyalin data price_history MudahMY...")
    data_price_history_mudahmy = await fetch_price_history_from_remote_db(remote_conn_mudahmy)  
    await insert_or_update_price_history(data_price_history_mudahmy, 'price_history_mudahmy')

    logger.info("Proses sinkronisasi selesai.")
    return {"status": "Data synced successfully"}

async def fetch_price_history_from_remote_db(conn):
    query = "SELECT car_id, old_price, new_price, changed_at FROM public.price_history"
    rows = await conn.fetch(query)
    return rows

async def insert_or_update_price_history(data, table_name):
    conn = await get_local_db_connection()
    try:
        for row in data:
            car_id = row['car_id']
            old_price = row['old_price']
            new_price = row['new_price']
            changed_at = row['changed_at']

            await conn.execute(f"""
                INSERT INTO {table_name} (car_id, old_price, new_price, changed_at)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (car_id) 
                DO UPDATE SET 
                    old_price = EXCLUDED.old_price,
                    new_price = EXCLUDED.new_price,
                    changed_at = EXCLUDED.changed_at
            """, car_id, old_price, new_price, changed_at)
    finally:
        await conn.close()

async def get_price_rank_carlistmy(request):
    brand = request.get("brand")
    model = request.get("model")
    variant = request.get("variant")
    price = request.get("price")
    millage = request.get("millage")
    year = request.get("year")
    
    if not (brand and model and variant and price and millage and year):
        raise HTTPException(status_code=400, detail="Brand, model, variant, price, millage, and year must be provided.")

    price = convert_price(price)
    millage = convert_millage(millage)

    if price is None:
        raise HTTPException(status_code=400, detail="Invalid price format. Price must contain 'RM'.")
    if millage is None:
        raise HTTPException(status_code=400, detail="Invalid millage format.")

    conn = await get_local_db_connection()

    try:
        query = f"""
            SELECT id, brand, model, variant, price, millage, year
            FROM cars_carlistmy
            WHERE brand = $1 AND model = $2 AND variant = $3 AND year = $4
        """
        rows = await conn.fetch(query, brand, model, variant, year)

        if not rows:
            raise HTTPException(status_code=404, detail="No cars found for the specified brand, model, variant, and year.")
        
        import pandas as pd
        df = pd.DataFrame(rows, columns=["id", "brand", "model", "variant", "price", "millage", "year"])

        scaler = RobustScaler()
        df[["scaled_price", "scaled_millage"]] = scaler.fit_transform(df[["price", "millage"]])

        weight_price = 0.6
        weight_millage = 0.4

        df["skor"] = (df["scaled_price"] * weight_price) + (df["scaled_millage"] * weight_millage)

        df = df.sort_values(by="skor", ascending=True).reset_index(drop=True)

        df["ranking"] = df["skor"].rank(method="dense", ascending=True).astype(int)

        def calculate_distance(row):
            return np.sqrt((row["price"] - price) ** 2 + (row["millage"] - millage) ** 2)

        df["distance"] = df.apply(calculate_distance, axis=1)

        closest_car = df.loc[df["distance"].idxmin()]

        user_rank = closest_car["ranking"]

        total_listings = len(df)

        total_rank = df["ranking"].nunique()

        message = (
            f"You are in rank position {user_rank} out of {total_rank} ranks, "
            f"with a total of {total_listings} listings."
        )

        if total_listings < 10:
            all_data = [
                RankCarResponse(
                    id=row["id"],
                    brand=row["brand"],
                    model=row["model"],
                    variant=row["variant"],
                    price=row["price"],
                    millage=row["millage"],
                    year=row["year"],
                    ranking=row["ranking"]
                )
                for _, row in df.iterrows()
            ]
            return RankPriceResponse(
                user_rank=user_rank,
                message=message,
                top_5=None,
                bottom_5=None,
                all_data=all_data  
            )
        
        top_5 = df.head(5)
        bottom_5 = df.tail(5)

        top_5_with_rank = [
            RankCarResponse(
                id=row["id"],
                brand=row["brand"],
                model=row["model"],
                variant=row["variant"],
                price=row["price"],
                millage=row["millage"],
                year=row["year"],
                ranking=row["ranking"]
            )
            for _, row in top_5.iterrows()
        ]
        
        bottom_5_with_rank = [
            RankCarResponse(
                id=row["id"],
                brand=row["brand"],
                model=row["model"],
                variant=row["variant"],
                price=row["price"],
                millage=row["millage"],
                year=row["year"],
                ranking=row["ranking"]
            )
            for _, row in bottom_5.iterrows()
        ]

        return RankPriceResponse(
            user_rank=user_rank,
            top_5=top_5_with_rank,
            bottom_5=bottom_5_with_rank,
            message=message
        )
    finally:
        await conn.close()

async def get_price_rank_mudahmy(request: dict) -> RankPriceResponse:
    brand = request.get("brand")
    model = request.get("model")
    variant = request.get("variant")
    price = request.get("price")
    millage = request.get("millage")
    year = request.get("year")
    
    if not (brand and model and variant and price and millage and year):
        raise HTTPException(
            status_code=400,
            detail="Brand, model, variant, price, millage, and year must be provided."
        )

    price = convert_price(price)
    millage = convert_millage(millage)

    if price is None:
        raise HTTPException(status_code=400, detail="Invalid price format. Price must contain 'RM'.")
    if millage is None:
        raise HTTPException(status_code=400, detail="Invalid millage format.")

    conn = await get_local_db_connection()

    try:
        query = """
            SELECT id, brand, model, variant, price, millage, year
            FROM cars_mudahmy
            WHERE brand = $1 AND model = $2 AND variant = $3 AND year = $4
        """
        rows = await conn.fetch(query, brand, model, variant, year)

        if not rows:
            raise HTTPException(status_code=404, detail="No cars found for the specified brand, model, variant, and year.")
        
        df = pd.DataFrame(rows, columns=["id", "brand", "model", "variant", "price", "millage", "year"])

        # Scaling price & millage
        scaler = RobustScaler()
        df[["scaled_price", "scaled_millage"]] = scaler.fit_transform(df[["price", "millage"]])

        weight_price = 0.6
        weight_millage = 0.4
        df["skor"] = (df["scaled_price"] * weight_price) + (df["scaled_millage"] * weight_millage)

        # Urutkan berdasar skor
        df = df.sort_values(by="skor", ascending=True).reset_index(drop=True)

        # Ranking
        df["ranking"] = df["skor"].rank(method="dense", ascending=True).astype(int)

        # Cari mobil yang paling dekat dengan input user (price & millage)
        def calculate_distance(row):
            return np.sqrt((row["price"] - price) ** 2 + (row["millage"] - millage) ** 2)

        df["distance"] = df.apply(calculate_distance, axis=1)
        closest_car = df.loc[df["distance"].idxmin()]

        user_rank = closest_car["ranking"]
        total_listings = len(df)
        total_rank = df["ranking"].nunique()

        message = (
            f"You are in rank position {user_rank} out of {total_rank} ranks, "
            f"with a total of {total_listings} listings."
        )

        # Jika data < 10, langsung kembalikan semua data
        if total_listings < 10:
            all_data = [
                RankCarResponse(
                    id=row["id"],
                    brand=row["brand"],
                    model=row["model"],
                    variant=row["variant"],
                    price=row["price"],
                    millage=row["millage"],
                    year=row["year"],
                    ranking=row["ranking"]
                )
                for _, row in df.iterrows()
            ]
            return RankPriceResponse(
                user_rank=user_rank,
                message=message,
                top_5=None,
                bottom_5=None,
                all_data=all_data  
            )

        # top_5 dan bottom_5
        top_5 = df.head(5)
        bottom_5 = df.tail(5)

        top_5_with_rank = [
            RankCarResponse(
                id=row["id"],
                brand=row["brand"],
                model=row["model"],
                variant=row["variant"],
                price=row["price"],
                millage=row["millage"],
                year=row["year"],
                ranking=row["ranking"]
            )
            for _, row in top_5.iterrows()
        ]
        
        bottom_5_with_rank = [
            RankCarResponse(
                id=row["id"],
                brand=row["brand"],
                model=row["model"],
                variant=row["variant"],
                price=row["price"],
                millage=row["millage"],
                year=row["year"],
                ranking=row["ranking"]
            )
            for _, row in bottom_5.iterrows()
        ]

        return RankPriceResponse(
            user_rank=user_rank,
            top_5=top_5_with_rank,
            bottom_5=bottom_5_with_rank,
            message=message
        )
    finally:
        await conn.close()

async def search_cars_mudahmy(
    brand: Optional[str] = None,
    model: Optional[str] = None,
    variant: Optional[str] = None,
    min_price: Optional[int] = None,
    max_price: Optional[int] = None,
    year: Optional[int] = None,
    location: Optional[str] = None,
    page: int = 1,
    size: int = 10
) -> SearchCarsResponse:
    """
    Mencari mobil di tabel cars_mudahmy dengan filter dinamis.
    Menggunakan pagination (page, size).
    """

    conn = await get_local_db_connection()
    try:
        base_query = """
            SELECT id, brand, model, variant, price, millage, year, lokasi
            FROM cars_mudahmy
        """
        conditions = []
        values = []

        if brand:
            conditions.append(f"brand ILIKE ${len(values)+1}")
            values.append(brand)
        if model:
            conditions.append(f"model = ${len(values)+1}")
            values.append(model)
        if variant:
            conditions.append(f"variant = ${len(values)+1}")
            values.append(variant)
        if min_price is not None:
            conditions.append(f"price >= ${len(values)+1}")
            values.append(min_price)
        if max_price is not None:
            conditions.append(f"price <= ${len(values)+1}")
            values.append(max_price)
        if year is not None:
            conditions.append(f"year = ${len(values)+1}")
            values.append(year)
        if location:
            conditions.append(f"lokasi ILIKE ${len(values)+1}")
            values.append(f"%{location}%")  

        if conditions:
            base_query += " WHERE " + " AND ".join(conditions)

        count_query = f"SELECT COUNT(*) FROM ({base_query}) AS sub"
        total_items = await conn.fetchval(count_query, *values)

        if total_items == 0:
            return SearchCarsResponse(
                page=page,
                size=size,
                total_pages=0,
                total_items=0,
                data=[]
            )

        offset = (page - 1) * size
        base_query += f" ORDER BY id ASC LIMIT {size} OFFSET {offset}"

        rows = await conn.fetch(base_query, *values)

        cars = []
        for row in rows:
            cars.append(
                CarMudahMy(
                    id=row["id"],
                    brand=row["brand"],
                    model=row["model"],
                    variant=row["variant"],
                    price=row["price"],
                    millage=row["millage"],
                    year=row["year"],
                    lokasi=row["lokasi"]
                )
            )

        total_pages = math.ceil(total_items / size)
        return SearchCarsResponse(
            page=page,
            size=size,
            total_pages=total_pages,
            total_items=total_items,
            data=cars
        )
    finally:
        await conn.close()

async def search_cars_carlistmy(
    brand: Optional[str] = None,
    model: Optional[str] = None,
    variant: Optional[str] = None,
    min_price: Optional[int] = None,
    max_price: Optional[int] = None,
    year: Optional[int] = None,
    location: Optional[str] = None,
    page: int = 1,
    size: int = 10
) -> SearchCarsCarlistMyResponse:
    """
    Mencari mobil di tabel cars_carlistmy dengan filter dinamis.
    Menggunakan pagination (page, size).
    """
    conn = await get_local_db_connection()
    try:
        base_query = """
            SELECT id, brand, model, variant, price, millage, year, lokasi
            FROM cars_carlistmy
        """
        conditions = []
        values = []

        if brand:
            conditions.append(f"brand ILIKE ${len(values)+1}")
            values.append(brand)
        if model:
            conditions.append(f"model = ${len(values)+1}")
            values.append(model)
        if variant:
            conditions.append(f"variant = ${len(values)+1}")
            values.append(variant)
        if min_price is not None:
            conditions.append(f"price >= ${len(values)+1}")
            values.append(min_price)
        if max_price is not None:
            conditions.append(f"price <= ${len(values)+1}")
            values.append(max_price)
        if year is not None:
            conditions.append(f"year = ${len(values)+1}")
            values.append(year)
        if location:
            conditions.append(f"lokasi ILIKE ${len(values)+1}")
            values.append(f"%{location}%")  

        if conditions:
            base_query += " WHERE " + " AND ".join(conditions)

        count_query = f"SELECT COUNT(*) FROM ({base_query}) AS sub"
        total_items = await conn.fetchval(count_query, *values)

        if total_items == 0:
            return SearchCarsCarlistMyResponse(
                page=page,
                size=size,
                total_pages=0,
                total_items=0,
                data=[]
            )

        offset = (page - 1) * size
        base_query += f" ORDER BY id ASC LIMIT {size} OFFSET {offset}"

        rows = await conn.fetch(base_query, *values)

        cars = []
        for row in rows:
            cars.append(
                CarCarlistMy(
                    id=row["id"],
                    brand=row["brand"],
                    model=row["model"],
                    variant=row["variant"],
                    price=row["price"],
                    millage=row["millage"],
                    year=row["year"],
                    lokasi=row["lokasi"]  
                )
            )

        total_pages = math.ceil(total_items / size)
        return SearchCarsCarlistMyResponse(
            page=page,
            size=size,
            total_pages=total_pages,
            total_items=total_items,
            data=cars
        )
    finally:
        await conn.close()

async def get_brand_distribution_mudahmy() -> List[BrandCount]:
    """
    Mengembalikan jumlah listing untuk setiap brand di tabel cars_mudahmy.
    Urutkan dari brand paling banyak listing ke yang paling sedikit.
    """
    conn = await get_local_db_connection()
    try:
        query = """
            SELECT brand, COUNT(*) AS total
            FROM cars_mudahmy
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

async def get_price_summary_mudahmy(
    brand: Optional[str] = None,
    model: Optional[str] = None,
    variant: Optional[str] = None,
    year: Optional[int] = None
) -> PriceSummary:
    """
    Mengembalikan ringkasan statistik harga (min, max, avg, median) dari tabel cars_mudahmy,
    dengan filter opsional: brand, model, variant, year.
    Menambahkan total_listing juga.
    """
    conn = await get_local_db_connection()
    try:
        base_query_stats = """
            SELECT 
                MIN(price) AS min_price,
                MAX(price) AS max_price,
                AVG(price) AS avg_price
            FROM cars_mudahmy
        """

        base_query_median = """
            SELECT 
                percentile_cont(0.5) WITHIN GROUP (ORDER BY price) AS median_price
            FROM cars_mudahmy
        """

        base_query_count = """
            SELECT 
                COUNT(*) AS total_listing
            FROM cars_mudahmy
        """

        conditions = ["price IS NOT NULL"]
        values = []

        # Filter brand, model, variant, year (exact match)
        if brand:
            conditions.append(f"brand = ${len(values)+1}")
            values.append(brand)
        if model:
            conditions.append(f"model = ${len(values)+1}")
            values.append(model)
        if variant:
            conditions.append(f"variant = ${len(values)+1}")
            values.append(variant)
        if year:
            conditions.append(f"year = ${len(values)+1}")
            values.append(year)

        if conditions:
            where_clause = " AND ".join(conditions)
            base_query_stats += f" WHERE {where_clause}"
            base_query_median += f" WHERE {where_clause}"
            base_query_count += f" WHERE {where_clause}"

        row_stats = await conn.fetchrow(base_query_stats, *values)
        row_median = await conn.fetchrow(base_query_median, *values)
        row_count = await conn.fetchrow(base_query_count, *values)

        if not row_stats or row_stats["min_price"] is None:
            return PriceSummary(
                total_listing=0,
                min_price=None,
                max_price=None,
                avg_price=None,
                median_price=None
            )

        return PriceSummary(
            total_listing=row_count["total_listing"],
            min_price=row_stats["min_price"],
            max_price=row_stats["max_price"],
            avg_price=row_stats["avg_price"],
            median_price=row_median["median_price"]
        )
    finally:
        await conn.close()

async def get_top_locations_mudahmy(limit: int = 10) -> List[LocationCount]:
    """
    Mengembalikan daftar lokasi teratas (paling banyak listing) di tabel cars_mudahmy.
    """
    conn = await get_local_db_connection()
    try:
        query = f"""
            SELECT lokasi, COUNT(*) AS total
            FROM cars_mudahmy
            WHERE lokasi IS NOT NULL
            GROUP BY lokasi
            ORDER BY total DESC
            LIMIT {limit}
        """
        rows = await conn.fetch(query)

        results = []
        for row in rows:
            results.append(
                LocationCount(
                    location=row["lokasi"],
                    count=row["total"]
                )
            )
        return results
    finally:
        await conn.close()

async def get_brand_distribution_carlistmy() -> List[BrandCount]:
    """
    Mengembalikan jumlah listing untuk setiap brand di tabel cars_carlistmy.
    Urutkan dari brand dengan listing terbanyak ke paling sedikit.
    """
    conn = await get_local_db_connection()
    try:
        query = """
            SELECT brand, COUNT(*) AS total
            FROM cars_carlistmy
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

async def get_price_summary_carlistmy(
    brand: Optional[str] = None,
    model: Optional[str] = None,
    variant: Optional[str] = None,
    year: Optional[int] = None
) -> PriceSummary:
    """
    Mengembalikan ringkasan statistik harga (min, max, avg, median) dari tabel cars_carlistmy,
    dengan filter opsional: brand, model, variant, year. Termasuk total_listing.
    """
    conn = await get_local_db_connection()
    try:
        base_query_stats = """
            SELECT 
                MIN(price) AS min_price,
                MAX(price) AS max_price,
                AVG(price) AS avg_price
            FROM cars_carlistmy
        """

        base_query_median = """
            SELECT 
                percentile_cont(0.5) WITHIN GROUP (ORDER BY price) AS median_price
            FROM cars_carlistmy
        """

        base_query_count = """
            SELECT 
                COUNT(*) AS total_listing
            FROM cars_carlistmy
        """
        conditions = ["price IS NOT NULL"]
        values = []

        if brand:
            conditions.append(f"brand = ${len(values)+1}")
            values.append(brand)
        if model:
            conditions.append(f"model = ${len(values)+1}")
            values.append(model)
        if variant:
            conditions.append(f"variant = ${len(values)+1}")
            values.append(variant)
        if year:
            conditions.append(f"year = ${len(values)+1}")
            values.append(year)

        if conditions:
            where_clause = " AND ".join(conditions)
            base_query_stats += f" WHERE {where_clause}"
            base_query_median += f" WHERE {where_clause}"
            base_query_count += f" WHERE {where_clause}"

        row_stats = await conn.fetchrow(base_query_stats, *values)
        row_median = await conn.fetchrow(base_query_median, *values)
        row_count = await conn.fetchrow(base_query_count, *values)

        if not row_stats or row_stats["min_price"] is None:
            return PriceSummary(
                total_listing=0,
                min_price=None,
                max_price=None,
                avg_price=None,
                median_price=None
            )

        return PriceSummary(
            total_listing=row_count["total_listing"],
            min_price=row_stats["min_price"],
            max_price=row_stats["max_price"],
            avg_price=row_stats["avg_price"],
            median_price=row_median["median_price"]
        )
    finally:
        await conn.close()

async def get_top_locations_carlistmy(limit: int = 10) -> List[LocationCount]:
    """
    Mengembalikan daftar lokasi teratas di tabel cars_carlistmy.
    """
    conn = await get_local_db_connection()
    try:
        query = f"""
            SELECT lokasi, COUNT(*) AS total
            FROM cars_carlistmy
            WHERE lokasi IS NOT NULL
            GROUP BY lokasi
            ORDER BY total DESC
            LIMIT {limit}
        """
        rows = await conn.fetch(query)

        results = []
        for row in rows:
            results.append(
                LocationCount(
                    location=row["lokasi"],
                    count=row["total"]
                )
            )
        return results
    finally:
        await conn.close()
