import logging
import asyncpg
import os
import re
import json
import numpy as np 
import pandas as pd
from datetime import datetime
from typing import Optional, List, Literal
from fastapi import HTTPException
from sklearn.preprocessing import RobustScaler
from app.database import get_local_db_connection
from app.models import (
    RankPriceResponse, RankCarResponse, BrandCount, LocationCount, PriceDropItem, OptimalPriceItem
)
logger = logging.getLogger(__name__)

DB_CARLISTMY = os.getenv("DB_CARLISTNY", "scrap_carlistmy")
DB_CARLISTMY_USERNAME = os.getenv("DB_CARLISTNY_USERNAME", "fanfan")
DB_CARLISTMY_PASSWORD = os.getenv("DB_CARLISTNY_PASSWORD", "cenanun")
DB_CARLISTMY_HOST = os.getenv("DB_CARLISTMY_HOST", "192.168.1.207")
DB_MUDAHMY = os.getenv("DB_MUDAHMY", "scrap_mudahmy")
DB_MUDAHMY_USERNAME = os.getenv("DB_MUDAHMY_USERNAME", "fanfan")
DB_MUDAHMY_PASSWORD = os.getenv("DB_MUDAHMY_PASSWORD", "cenanun")
DB_MUDAHMY_HOST = os.getenv("DB_MUDAHMY_HOST", "192.168.1.207")

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
        table_name = "cars_mudahmy_1"
    elif source == "carlistmy":
        table_name = "cars_carlistmy_1"
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
            brand = row[2].upper() if row[2] else None
            model = row[3].upper() if row[3] else None
            variant = row[4].upper() if row[4] else None
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
                    source
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
                    $13, $14, $15, $16, $17, $18, $19
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
                    source = EXCLUDED.source
            """,
            id_, listing_url, brand, model, variant, informasi_iklan,
            lokasi, price_int, year_int, millage_int, transmission, seat_capacity,
            gambar, last_scraped_at, version, created_at, sold_at, status,
            source)
    finally:
        await conn.close()

async def sync_data_from_remote():
    logger.info("Proses sinkronisasi dimulai...")

    # Sinkronisasi dengan CarlistMY
    remote_conn_carlistmy = await get_remote_db_connection(f'{DB_CARLISTMY}', f'{DB_CARLISTMY_USERNAME}',
                                                           f'{DB_CARLISTMY_HOST}', f'{DB_CARLISTMY_PASSWORD}')
    logger.info("Koneksi ke CarlistMY berhasil.")
    data_carlistmy = await fetch_data_from_remote_db(remote_conn_carlistmy)
    await insert_or_update_data_into_local_db(data_carlistmy, 'cars_carlistmy_1', 'carlistmy')

    logger.info("Menyalin data price_history dari CarlistMY...")
    data_price_history_carlistmy = await fetch_price_history_from_remote_db(remote_conn_carlistmy, 'carlistmy')
    await insert_or_update_price_history(data_price_history_carlistmy, 'price_history_carlistmy')

    # Sinkronisasi dengan MudahMY
    remote_conn_mudahmy = await get_remote_db_connection(f'{DB_MUDAHMY}', f'{DB_MUDAHMY_USERNAME}',
                                                         f'{DB_MUDAHMY_HOST}', f'{DB_MUDAHMY_PASSWORD}')
    logger.info("Koneksi ke MudahMY berhasil.")
    data_mudahmy = await fetch_data_from_remote_db(remote_conn_mudahmy)
    await insert_or_update_data_into_local_db(data_mudahmy, 'cars_mudahmy_1', 'mudahmy')

    logger.info("Menyalin data price_history dari MudahMY...")
    data_price_history_mudahmy = await fetch_price_history_from_remote_db(remote_conn_mudahmy, 'mudahmy')
    await insert_or_update_price_history(data_price_history_mudahmy, 'price_history_mudahmy')

    logger.info("Proses sinkronisasi selesai.")
    return {"status": "Data synced successfully"}

async def fetch_price_history_from_remote_db(conn, source):
    """
    Mengambil data price history berdasarkan sumbernya, apakah carlistmy atau mudahmy.
    """
    if source == 'carlistmy':
        query = "SELECT car_id, old_price, new_price, changed_at FROM public.price_history_combined"
    elif source == 'mudahmy':
        query = "SELECT car_id, old_price, new_price, changed_at FROM public.price_history_combined"
    else:
        raise HTTPException(status_code=400, detail="Invalid source for price history.")

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
                ON CONFLICT (car_id, changed_at) 
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
            FROM cars_carlistmy_1
            WHERE brand = $1 AND model = $2 AND variant = $3 AND year = $4
        """
        rows = await conn.fetch(query, brand, model, variant, year)

        if not rows:
            raise HTTPException(status_code=404, detail="No cars found for the specified brand, model, variant, and year.")

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
            FROM cars_mudahmy_1
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

async def get_brand_distribution_mudahmy() -> List[BrandCount]:
    """
    Mengembalikan jumlah listing untuk setiap brand di tabel cars_mudahmy.
    Urutkan dari brand paling banyak listing ke yang paling sedikit.
    """
    conn = await get_local_db_connection()
    try:
        query = """
            SELECT brand, COUNT(*) AS total
            FROM cars_mudahmy_1
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

async def get_brand_distribution_carlistmy() -> List[BrandCount]:
    """
    Mengembalikan jumlah listing untuk setiap brand di tabel cars_carlistmy.
    Urutkan dari brand dengan listing terbanyak ke paling sedikit.
    """
    conn = await get_local_db_connection()
    try:
        query = """
            SELECT brand, COUNT(*) AS total
            FROM cars_carlistmy_1
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

async def get_top_locations_carlistmy(limit: int = 10) -> List[LocationCount]:
    """
    Mengembalikan daftar lokasi teratas di tabel cars_carlistmy.
    """
    conn = await get_local_db_connection()
    try:
        query = f"""
            SELECT lokasi, COUNT(*) AS total
            FROM cars_carlistmy_1
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

async def get_cars_for_datatables(source: str, start: int, length: int, search: str, order_column: int, order_dir: str):
    conn = await get_local_db_connection()

    try:
        table_name = "cars_mudahmy_1" if source == "mudahmy" else "cars_carlistmy_1"

        # Validasi kolom yang bisa disortir
        sortable_columns = [
            "gambar", "informasi_iklan", "brand", "model", "variant",
            "price", "millage", "year", "transmission", "source"
        ]
        sort_column = sortable_columns[order_column] if order_column < len(sortable_columns) else "id"
        sort_direction = "ASC" if order_dir == "asc" else "DESC"

        # Total records
        total_query = f"SELECT COUNT(*) FROM {table_name}"
        total = await conn.fetchval(total_query)

        # Filtering
        filter_query = f"FROM {table_name} WHERE brand ILIKE $1 OR model ILIKE $1 OR variant ILIKE $1 OR informasi_iklan ILIKE $1"
        filtered_query = f"SELECT * {filter_query} ORDER BY {sort_column} {sort_direction} OFFSET {start} LIMIT {length}"
        count_query = f"SELECT COUNT(*) {filter_query}"

        rows = await conn.fetch(filtered_query, f"%{search}%")
        filtered_count = await conn.fetchval(count_query, f"%{search}%")

        # Format untuk DataTables
        data = []
        for row in rows:
            data.append([
                row["gambar"][0] if row["gambar"] else "",
                row["informasi_iklan"],
                row["brand"],
                row["model"],
                row["variant"],
                row["price"],
                row["millage"],
                row["year"],
                row["transmission"],
                row["source"],
                f'<a href="{row["listing_url"]}" target="_blank" class="text-blue-600 underline">Details</a>'
            ])

        return total, filtered_count, data
    finally:
        await conn.close()

async def get_price_drop_top(source: str, limit: int = 10) -> List[RankCarResponse]:
    conn = await get_local_db_connection()
    try:
        if source == "mudahmy":
            table_name = "price_history_mudahmy"
            car_table = "cars_mudahmy_1"
        elif source == "carlistmy":
            table_name = "price_history_carlistmy"
            car_table = "cars_carlistmy_1"
        else:
            raise HTTPException(status_code=400, detail="Invalid source")

        query = f"""
            SELECT ph.car_id, ph.old_price, ph.new_price, ph.changed_at,
                   c.brand, c.model, c.variant, c.price, c.millage, c.year
            FROM {table_name} ph
            JOIN {car_table} c ON ph.car_id = c.id
            WHERE ph.old_price IS NOT NULL AND ph.new_price IS NOT NULL AND ph.old_price > ph.new_price
            ORDER BY (ph.old_price - ph.new_price) DESC
            LIMIT $1
        """

        rows = await conn.fetch(query, limit)

        result = []
        for row in rows:
            result.append(RankCarResponse(
                id=row["car_id"],
                brand=row["brand"],
                model=row["model"],
                variant=row["variant"],
                price=row["price"],
                millage=row["millage"],
                year=row["year"],
                ranking=0  # opsional jika ingin tetap sesuai schema
            ))

        return result
    finally:
        await conn.close()


async def get_price_drop_top(source: str, limit: int = 10) -> List[PriceDropItem]:
    table_name = "price_history_mudahmy" if source == "mudahmy" else "price_history_carlistmy"
    car_table = "cars_mudahmy_1" if source == "mudahmy" else "cars_carlistmy_1"

    conn = await get_local_db_connection()
    try:
        # Ambil perubahan harga terbesar (selisih harga)
        query = f"""
            SELECT ph.car_id, c.brand, c.model, c.variant, c.price, c.year, c.millage,
                   MAX(ph.old_price - ph.new_price) AS drop_amount,
                   MAX(ph.changed_at) AS last_changed_at
            FROM {table_name} ph
            JOIN {car_table} c ON ph.car_id = c.id
            WHERE ph.old_price > ph.new_price
            GROUP BY ph.car_id, c.brand, c.model, c.variant, c.price, c.year, c.millage
            ORDER BY drop_amount DESC
            LIMIT $1
        """
        rows = await conn.fetch(query, limit)

        return [
            PriceDropItem(
                car_id=row["car_id"],
                brand=row["brand"],
                model=row["model"],
                variant=row["variant"],
                price=row["price"],
                millage=row["millage"],
                year=row["year"],
                drop_amount=row["drop_amount"],
                last_changed_at=row["last_changed_at"].isoformat()
            )
            for row in rows
        ]
    finally:
        await conn.close()

async def get_top_price_drops(source: str, limit: int = 10) -> List[PriceDropItem]:
    if source not in ["mudahmy", "carlistmy"]:
        raise HTTPException(status_code=400, detail="Invalid source")

    table_price_history = f"price_history_{source}"
    table_cars = f"cars_{source}"

    conn = await get_local_db_connection()
    try:
        query = f"""
            SELECT 
                h.car_id,
                c.brand,
                c.model,
                c.variant,
                h.old_price,
                h.new_price,
                (h.old_price - h.new_price) AS drop_amount,
                h.changed_at
            FROM {table_price_history} h
            JOIN {table_cars} c ON h.car_id = c.id
            WHERE h.old_price > h.new_price
            ORDER BY drop_amount DESC
            LIMIT $1
        """

        rows = await conn.fetch(query, limit)
        return [
            PriceDropItem(
                car_id=row["car_id"],
                brand=row["brand"],
                model=row["model"],
                variant=row["variant"],
                old_price=row["old_price"],
                new_price=row["new_price"],
                drop_amount=row["drop_amount"],
                changed_at=row["changed_at"].strftime("%Y-%m-%d %H:%M:%S")
            )
            for row in rows
        ]
    finally:
        await conn.close()

async def get_brand_model_distribution(source: str) -> List[BrandCount]:
    table_name = "cars_mudahmy_1" if source == "mudahmy" else "cars_carlistmy_1"
    conn = await get_local_db_connection()
    try:
        query = f"""
            SELECT CONCAT(brand, ' ', model) AS brand_model, COUNT(*) AS total
            FROM {table_name}
            WHERE brand IS NOT NULL AND model IS NOT NULL
            GROUP BY brand_model
            ORDER BY total DESC
        """
        rows = await conn.fetch(query)
        return [
            BrandCount(
                brand=row["brand_model"],
                count=row["total"]
            ) for row in rows
        ]
    finally:
        await conn.close()


async def get_top_locations_by_brand(source: str, brand: str, model: Optional[str] = None, limit: int = 10) -> List[LocationCount]:
    table_name = "cars_mudahmy_1" if source == "mudahmy" else "cars_carlistmy_1"
    conn = await get_local_db_connection()
    try:
        conditions = ["UPPER(brand) = UPPER($1)"]
        values = [brand]

        if model:
            conditions.append(f"UPPER(model) = UPPER(${len(values) + 1})")
            values.append(model)

        where_clause = " AND ".join(conditions)

        query = f"""
            SELECT lokasi, COUNT(*) AS total
            FROM {table_name}
            WHERE {where_clause} AND lokasi IS NOT NULL
            GROUP BY lokasi
            ORDER BY total DESC
            LIMIT ${len(values) + 1}
        """
        values.append(limit)

        rows = await conn.fetch(query, *values)

        return [
            LocationCount(
                location=row["lokasi"],
                count=row["total"]
            ) for row in rows
        ]
    finally:
        await conn.close()

async def get_available_brands_models(source: str) -> List[dict]:
    table_name = "cars_mudahmy_1" if source == "mudahmy" else "cars_carlistmy_1"
    conn = await get_local_db_connection()
    try:
        query = f"""
            SELECT DISTINCT brand, model
            FROM {table_name}
            WHERE brand IS NOT NULL AND model IS NOT NULL
            ORDER BY brand ASC, model ASC
        """
        rows = await conn.fetch(query)

        return [{"brand": row["brand"], "model": row["model"]} for row in rows]
    finally:
        await conn.close()

async def get_optimal_price_recommendations(source: Literal["carlistmy", "mudahmy"]):
    table_name = f"cars_{source}"
    query = f"""
        SELECT brand, model, variant,
               COUNT(*) AS jumlah_iklan,
               AVG(price)::FLOAT AS rata_rata_harga,
               PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price)::FLOAT AS median_harga,
               MIN(price) AS harga_terendah,
               MAX(price) AS harga_tertinggi
        FROM {table_name}
        WHERE price > 0 AND brand IS NOT NULL AND model IS NOT NULL
        GROUP BY brand, model, variant
        ORDER BY jumlah_iklan DESC, rata_rata_harga DESC
    """
    conn = await get_local_db_connection()
    try:
        rows = await conn.fetch(query)
        return [
            OptimalPriceItem(
                brand=row["brand"],
                model=row["model"],
                variant=row["variant"],
                jumlah_iklan=row["jumlah_iklan"],
                rata_rata_harga=row["rata_rata_harga"],
                median_harga=row["median_harga"],
                harga_terendah=row["harga_terendah"],
                harga_tertinggi=row["harga_tertinggi"]
            )
            for row in rows
        ]
    finally:
        await conn.close()

async def get_price_vs_millage_filtered(
    source: str,
    brand: str,
    model: str,
    variant: Optional[str] = None,
    year: Optional[int] = None
) -> List[dict]:
    if source not in ["mudahmy", "carlistmy"]:
        raise HTTPException(status_code=400, detail="Invalid source")

    table = f"cars_{source}"
    conditions = ["brand = $1", "model = $2"]
    values = [brand, model]

    if variant:
        conditions.append(f"variant ILIKE ${len(values) + 1}")
        values.append(f"%{variant}%")

    if year:
        conditions.append(f"year = ${len(values) + 1}")
        values.append(year)

    where_clause = " AND ".join(conditions)
    query = f"""
        SELECT price, millage
        FROM {table}
        WHERE price IS NOT NULL AND millage IS NOT NULL AND {where_clause}
    """

    conn = await get_local_db_connection()
    try:
        rows = await conn.fetch(query, *values)
        return [{"price": row["price"], "millage": row["millage"]} for row in rows]
    finally:
        await conn.close()

# untuk price_vs_millage
async def get_all_dropdown_options(
    source: str,
    brand: Optional[str] = None,
    model: Optional[str] = None,
    variant: Optional[str] = None
) -> dict:
    table = f"cars_{source}"
    conn = await get_local_db_connection()
    try:
        result = {}

        # Ambil semua brand
        brand_query = f"SELECT DISTINCT brand FROM {table} WHERE brand IS NOT NULL ORDER BY brand ASC"
        brands = await conn.fetch(brand_query)
        result["brands"] = [row["brand"] for row in brands]

        # Ambil model kalau brand disediakan
        if brand:
            model_query = f"""
                SELECT DISTINCT model FROM {table}
                WHERE brand = $1 AND model IS NOT NULL
                ORDER BY model ASC
            """
            models = await conn.fetch(model_query, brand)
            result["models"] = [row["model"] for row in models]
        else:
            result["models"] = []

        # Ambil variant kalau brand & model disediakan
        if brand and model:
            variant_query = f"""
                SELECT DISTINCT variant FROM {table}
                WHERE brand = $1 AND model = $2 AND variant IS NOT NULL
                ORDER BY variant ASC
            """
            variants = await conn.fetch(variant_query, brand, model)
            result["variants"] = [row["variant"] for row in variants]
        else:
            result["variants"] = []

        # Ambil year kalau brand & model disediakan (variant opsional)
        if brand and model:
            if variant:
                year_query = f"""
                    SELECT DISTINCT year FROM {table}
                    WHERE brand = $1 AND model = $2 AND variant ILIKE $3 AND year IS NOT NULL
                    ORDER BY year DESC
                """
                years = await conn.fetch(year_query, brand, model, f"%{variant}%")
            else:
                year_query = f"""
                    SELECT DISTINCT year FROM {table}
                    WHERE brand = $1 AND model = $2 AND year IS NOT NULL
                    ORDER BY year DESC
                """
                years = await conn.fetch(year_query, brand, model)
            result["years"] = [int(row["year"]) for row in years if row["year"] is not None]
        else:
            result["years"] = []

        return result

    finally:
        await conn.close()

async def normalize_data_to_cars_normalize():
    import time
    conn = await get_local_db_connection()
    try:
        sources = [
            ("cars_mudahmy_1", "mudahmy"),
            ("cars_carlistmy_1", "carlistmy"),
        ]

        for table_name, source in sources:
            print(f"\n🚀 Memulai normalisasi data dari {table_name.upper()}...")

            # Mulai waktu
            start_time = time.time()

            # Ambil data yang semua kolom pentingnya tidak null
            query = f"""
                SELECT id FROM {table_name}
                WHERE listing_url IS NOT NULL
                  AND brand IS NOT NULL
                  AND model IS NOT NULL
                  AND variant IS NOT NULL
                  AND price IS NOT NULL
                  AND year IS NOT NULL
                  AND millage IS NOT NULL
                  AND brand_norm IS NOT NULL
                  AND model_group_norm IS NOT NULL
                  AND model_norm IS NOT NULL
                  AND variant_norm IS NOT NULL
                  AND cleaned IS NOT NULL
            """

            rows = await conn.fetch(query)
            total_valid = len(rows)
            print(f"✅ Ditemukan {total_valid} data valid dari {table_name}")

            total_inserted = 0
            for idx, row in enumerate(rows, 1):
                try:
                    await conn.execute("""
                        INSERT INTO cars_normalize (cars_id, source)
                        VALUES ($1, $2)
                        ON CONFLICT (cars_id, source) DO NOTHING
                    """, row["id"], source)
                    total_inserted += 1

                    if idx % 500 == 0:
                        print(f"   ⏳ Progress: {idx}/{total_valid} data diproses...")

                except Exception as e:
                    print(f"⚠️ Gagal insert ID {row['id']} dari {source}: {e}")

            elapsed = time.time() - start_time
            print(f"✅ Selesai normalisasi dari {table_name}: {total_inserted} data dimasukkan. ⏱️ Waktu: {elapsed:.2f} detik")

        return {"status": "success", "message": "Normalization complete"}

    finally:
        await conn.close()

async def get_price_vs_millage_normalized(
    source: Optional[str] = None,
    brand: Optional[str] = None,
    model_group: Optional[str] = None,
    model: Optional[str] = None,
    variant: Optional[str] = None,
    year: Optional[int] = None
) -> List[dict]:
    query = """
        SELECT
            COALESCE(m.brand_norm, c.brand_norm) AS brand,
            COALESCE(m.model_group_norm, c.model_group_norm) AS model_group,
            COALESCE(m.model_norm, c.model_norm) AS model,
            COALESCE(m.variant_norm, c.variant_norm) AS variant,
            COALESCE(m.price, c.price) AS price,
            COALESCE(m.millage, c.millage) AS millage,
            COALESCE(m.year, c.year) AS year,
            n.source
        FROM cars_normalize n
        LEFT JOIN cars_mudahmy_1 m ON n.source = 'mudahmy' AND m.id = n.cars_id
        LEFT JOIN cars_carlistmy_1 c ON n.source = 'carlistmy' AND c.id = n.cars_id
        WHERE
            ($1::VARCHAR IS NULL OR n.source = $1) AND
            ($2::VARCHAR IS NULL OR UPPER(COALESCE(m.brand_norm, c.brand_norm)) = UPPER($2)) AND
            ($3::VARCHAR IS NULL OR UPPER(COALESCE(m.model_group_norm, c.model_group_norm)) = UPPER($3)) AND
            ($4::VARCHAR IS NULL OR UPPER(COALESCE(m.model_norm, c.model_norm)) = UPPER($4)) AND
            ($5::VARCHAR IS NULL OR UPPER(COALESCE(m.variant_norm, c.variant_norm)) = UPPER($5)) AND
            ($6::INTEGER IS NULL OR COALESCE(m.year, c.year) = $6)
    """

    conn = await get_local_db_connection()
    try:
        rows = await conn.fetch(query, source, brand, model_group, model, variant, year)
        return [dict(row) for row in rows]
    finally:
        await conn.close()

