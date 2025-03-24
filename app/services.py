import logging
import asyncpg
from app.database import get_local_db_connection
from fastapi import HTTPException
import json
from app.config import DATABASE_URL
import re
from app.models import RankPriceResponse, RankCarResponse
from sklearn.preprocessing import RobustScaler
import numpy as np 
from datetime import datetime

logger = logging.getLogger(__name__)

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
            gambar_json = json.dumps(gambar)  
            last_scraped_at = parse_datetime(row[13])
            version = row[14]
            created_at = parse_datetime(row[15])
            sold_at = parse_datetime(row[16])
            status = row[17]
            previous_price = row[18]

            logger.info(f"id_: {id_}, type: {type(id_)}")
            logger.info(f"status: {status}, type: {type(status)}")
            logger.info(f"previous_price: {previous_price}, type: {type(previous_price)}")

            brand = brand.upper() if brand else None
            millage_int = convert_millage(millage)
            price_int = convert_price(price)
            
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
            lokasi, price_int, year, millage_int, transmission, seat_capacity,
            gambar_json, last_scraped_at, version, created_at, sold_at, status,
            previous_price, source)
    finally:
        await conn.close()

async def sync_data_from_remote():
    logger.info("Proses sinkronisasi dimulai...")
    
    remote_conn_carlistmy = await get_remote_db_connection('scrap_carlistmy', 'fanfan', '192.168.1.207', 'cenanun')
    logger.info("Koneksi ke CarlistMY berhasil.")
    data_carlistmy = await fetch_data_from_remote_db(remote_conn_carlistmy) 
    await insert_or_update_data_into_local_db(data_carlistmy, 'cars_carlistmy', 'carlistmy')  
    
    remote_conn_mudahmy = await get_remote_db_connection('scrap_mudahmy', 'funfun', '47.236.125.23', 'cenanun')
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
                ON CONFLICT (car_id)  -- Gunakan constraint unik pada car_id
                DO UPDATE SET 
                    old_price = EXCLUDED.old_price,
                    new_price = EXCLUDED.new_price,
                    changed_at = EXCLUDED.changed_at
            """, car_id, old_price, new_price, changed_at)
    finally:
        await conn.close()

async def get_price_rank(request):
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