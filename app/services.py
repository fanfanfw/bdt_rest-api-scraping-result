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

logger = logging.getLogger(__name__)

def convert_price(price_str):
    if isinstance(price_str, int):
        return price_str  
    if isinstance(price_str, str) and 'RM' in price_str:
        return int(price_str.replace('RM', '').replace(',', '').strip())
    return None

def convert_millage(millage_str):
    if isinstance(millage_str, int):
        return millage_str  # Langsung kembalikan angka jika input adalah integer
    if isinstance(millage_str, str):
        numbers = re.findall(r'\d+', millage_str)
        if numbers:
            # Ambil angka terakhir yang ditemukan
            millage_value = int(numbers[-1])
            
            # Jika nilai millage sudah lebih dari 1000, langsung kembalikan tanpa dikali 1000
            if millage_value >= 1000:
                return millage_value
            else:
                # Jika nilainya kurang dari 1000 (misalnya "15K"), kalikan dengan 1000
                return millage_value * 1000
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

async def insert_or_update_data_into_local_db(data, table_name):
    conn = await get_local_db_connection()
    try:
        for row in data:
            listing_url, brand, model, variant, informasi_iklan, lokasi, price, year, millage, transmission, seat_capacity, gambar = row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12]
            
            brand = brand.upper() if brand else None
            model = model.upper() if model else None
            variant = variant.upper() if variant else None

            price_int = convert_price(price)
            millage_int = convert_millage(millage)
            
            gambar_json = json.dumps(gambar) 
            await conn.execute(f"""
                INSERT INTO {table_name} (listing_url, brand, model, variant, informasi_iklan, lokasi, price, year, millage, transmission, seat_capacity, gambar)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                ON CONFLICT (listing_url) 
                DO UPDATE SET 
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
                    gambar = EXCLUDED.gambar
            """, listing_url, brand, model, variant, informasi_iklan, lokasi, price_int, year, millage_int, transmission, seat_capacity, gambar_json)
    finally:
        await conn.close()

async def sync_data_from_remote():
    logger.info("Proses sinkronisasi dimulai...")
    
    remote_conn_carlistmy = await get_remote_db_connection('scrap_carlistmy', 'fanfan', '192.168.1.207', 'cenanun')
    logger.info("Koneksi ke CarlistMY berhasil.")
    data_carlistmy = await fetch_data_from_remote_db(remote_conn_carlistmy) 
    await insert_or_update_data_into_local_db(data_carlistmy, 'cars_carlistmy')
    
    remote_conn_mudahmy = await get_remote_db_connection('scrap_mudahmy', 'funfun', '47.236.125.23', 'cenanun')
    logger.info("Koneksi ke MudahMY berhasil.")
    data_mudahmy = await fetch_data_from_remote_db(remote_conn_mudahmy)  
    await insert_or_update_data_into_local_db(data_mudahmy, 'cars_mudahmy')
    
    logger.info("Proses sinkronisasi selesai.")
    return {"status": "Data synced successfully"}

async def get_price_rank(request):
    brand = request.get("brand")
    model = request.get("model")
    variant = request.get("variant")
    price = request.get("price")
    millage = request.get("millage")
    year = request.get("year")
    
    if not (brand and model and variant and price and millage and year):
        raise HTTPException(status_code=400, detail="Brand, model, variant, price, millage, and year must be provided.")

    # Konversi price dan millage
    price = convert_price(price)
    millage = convert_millage(millage)

    # Pastikan price dan millage tidak None setelah konversi
    if price is None:
        raise HTTPException(status_code=400, detail="Invalid price format. Price must contain 'RM'.")
    if millage is None:
        raise HTTPException(status_code=400, detail="Invalid millage format.")

    conn = await get_local_db_connection()

    try:
        # Query untuk mengambil mobil dengan brand, model, variant, dan tahun yang sama
        query = f"""
            SELECT id, brand, model, variant, price, millage, year
            FROM cars_carlistmy
            WHERE brand = $1 AND model = $2 AND variant = $3 AND year = $4
        """
        rows = await conn.fetch(query, brand, model, variant, year)

        if not rows:
            raise HTTPException(status_code=404, detail="No cars found for the specified brand, model, variant, and year.")
        
        # Konversi data ke DataFrame
        import pandas as pd
        df = pd.DataFrame(rows, columns=["id", "brand", "model", "variant", "price", "millage", "year"])

        # Robust scaling untuk price dan millage
        scaler = RobustScaler()
        df[["scaled_price", "scaled_millage"]] = scaler.fit_transform(df[["price", "millage"]])

        # Bobot (weight) untuk price dan millage
        weight_price = 0.6
        weight_millage = 0.4

        # Menghitung skor
        df["skor"] = (df["scaled_price"] * weight_price) + (df["scaled_millage"] * weight_millage)

        # Mengurutkan data berdasarkan skor (dari terbaik ke terburuk)
        df = df.sort_values(by="skor", ascending=True).reset_index(drop=True)

        # Memberikan ranking kelompok dengan method="dense"
        df["ranking"] = df["skor"].rank(method="dense", ascending=True).astype(int)

        # Menghitung jarak antara mobil pengguna dan mobil di database
        def calculate_distance(row):
            return np.sqrt((row["price"] - price) ** 2 + (row["millage"] - millage) ** 2)

        df["distance"] = df.apply(calculate_distance, axis=1)

        # Menemukan mobil dengan jarak terdekat
        closest_car = df.loc[df["distance"].idxmin()]

        # Menentukan peringkat pengguna berdasarkan mobil terdekat
        user_rank = closest_car["ranking"]

        # Menghitung jumlah total listing yang relevan
        total_listings = len(df)

        # Menghitung total rank (jumlah peringkat unik)
        total_rank = df["ranking"].nunique()

        # Membuat pesan
        message = (
            f"You are in rank position {user_rank} out of {total_rank} ranks, "
            f"with a total of {total_listings} listings."
        )

        # Jika jumlah data kurang dari 10, tampilkan semua data
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
                all_data=all_data  # Tambahkan field all_data untuk menampilkan semua data
            )
        
        # Jika jumlah data lebih dari 10, tampilkan top 5 dan bottom 5
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