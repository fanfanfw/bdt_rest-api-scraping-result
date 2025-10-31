#!/usr/bin/env python3
"""
Script untuk mengimport data Carsome dari CSV ke tabel carsome
"""

import os
import csv
import asyncio
import asyncpg
import logging
from datetime import datetime
from typing import List, Dict, Any
import argparse

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database configuration
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_USER = os.getenv('DB_USER', 'fanfan')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'cenanun')
DB_NAME = os.getenv('DB_NAME', 'db_test')

async def get_db_connection():
    """Create database connection"""
    return await asyncpg.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )

def parse_mileage(mileage_str: str) -> int:
    """Parse mileage from string to integer"""
    if mileage_str is None or mileage_str == '':
        return None

    # Remove any non-digit characters
    mileage = ''.join(filter(str.isdigit, str(mileage_str)))

    if mileage == '':
        return None

    return int(mileage)

def parse_price(price_str: str) -> int:
    """Parse price from string to integer"""
    if price_str is None or price_str == '':
        return None

    # Remove any non-digit characters
    price = ''.join(filter(str.isdigit, str(price_str)))

    if price == '':
        return None

    return int(price)

def parse_year(year_str: str) -> int:
    """Parse year from string to integer"""
    if year_str is None or year_str == '':
        return None

    try:
        return int(year_str)
    except ValueError:
        return None

def parse_datetime(datetime_str: str) -> datetime:
    """Parse datetime from string"""
    if datetime_str is None or datetime_str == '':
        return datetime.now()

    try:
        return datetime.fromisoformat(datetime_str.replace('Z', '+00:00'))
    except ValueError:
        return datetime.now()

async def create_table(conn):
    """Create carsome table if not exists"""
    logger.info("Creating carsome table...")

    create_table_sql = """
    CREATE TABLE IF NOT EXISTS carsome (
        id SERIAL PRIMARY KEY,
        image TEXT,
        brand VARCHAR(100),
        model VARCHAR(100),
        model_group VARCHAR(100) DEFAULT 'NO MODEL GROUP',
        variant VARCHAR(100),
        year INTEGER,
        mileage INTEGER,
        price INTEGER,
        created_at TIMESTAMP,
        cars_standard_id INTEGER REFERENCES cars_standard(id),

        -- Additional metadata fields
        last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        status VARCHAR(20) DEFAULT 'active',
        is_deleted BOOLEAN DEFAULT FALSE,

        -- Source tracking
        source VARCHAR(50) DEFAULT 'carsome'
    );
    """

    await conn.execute(create_table_sql)
    logger.info("✅ Carsome table created successfully")

async def import_csv_data(conn, csv_file_path: str):
    """Import data from CSV to carsome table"""
    logger.info(f"Importing data from {csv_file_path}...")

    # Read CSV file
    try:
        with open(csv_file_path, 'r', encoding='utf-8') as file:
            csv_reader = csv.DictReader(file)

            # Prepare batch insert
            insert_data = []

            for row in csv_reader:
                try:
                    # Parse and clean data
                    data = {
                        'image': row.get('image', '').strip() if row.get('image') else None,
                        'brand': row.get('brand', '').strip().upper() if row.get('brand') else None,
                        'model': row.get('model', '').strip().upper() if row.get('model') else None,
                        'model_group': 'NO MODEL GROUP',  # Default value (already uppercase)
                        'variant': row.get('variant', '').strip().upper() if row.get('variant') else None,
                        'year': parse_year(row.get('year')),
                        'mileage': parse_mileage(row.get('mileage')),
                        'price': parse_price(row.get('price')),
                        'created_at': parse_datetime(row.get('created_at')),
                        'cars_standard_id': None  # Will be filled later
                    }

                    # Only add if we have essential data
                    if data['brand'] and data['model'] and data['price']:
                        insert_data.append(data)

                except Exception as e:
                    logger.warning(f"Skipping row due to error: {e} - Row: {row}")
                    continue

            if not insert_data:
                logger.warning("No valid data found in CSV file")
                return

            logger.info(f"Found {len(insert_data)} valid records to import")

            # Insert data in batches
            batch_size = 100
            total_imported = 0

            for i in range(0, len(insert_data), batch_size):
                batch = insert_data[i:i + batch_size]

                # Prepare insert query
                insert_query = """
                    INSERT INTO carsome (
                        image, brand, model, model_group, variant, year, mileage,
                        price, created_at, cars_standard_id, status, is_deleted, source
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13
                    )
                """

                for data in batch:
                    await conn.execute(
                        insert_query,
                        data['image'],
                        data['brand'],
                        data['model'],
                        data['model_group'],
                        data['variant'],
                        data['year'],
                        data['mileage'],
                        data['price'],
                        data['created_at'],
                        data['cars_standard_id'],
                        'active',
                        False,
                        'carsome'
                    )
                    total_imported += 1

                logger.info(f"Imported {total_imported}/{len(insert_data)} records...")

            logger.info(f"✅ Successfully imported {total_imported} records to carsome table")

    except FileNotFoundError:
        logger.error(f"CSV file not found: {csv_file_path}")
        raise
    except Exception as e:
        logger.error(f"Error reading CSV file: {e}")
        raise

async def map_cars_standard_id(conn):
    """Map carsome records to cars_standard table"""
    logger.info("Mapping carsome records to cars_standard...")

    update_query = """
        UPDATE carsome
        SET cars_standard_id = cs.id
        FROM cars_standard cs
        WHERE UPPER(TRIM(carsome.brand)) = UPPER(TRIM(cs.brand_norm))
          AND UPPER(TRIM(carsome.model)) = UPPER(TRIM(cs.model_norm))
          AND UPPER(TRIM(carsome.variant)) = UPPER(TRIM(cs.variant_norm))
          AND carsome.cars_standard_id IS NULL
    """

    result = await conn.execute(update_query)

    # Extract count from result (format: "UPDATE count")
    updated_count = int(result.split()[-1]) if result and ' ' in result else 0

    logger.info(f"✅ Mapped {updated_count} records to cars_standard")

async def get_import_stats(conn):
    """Get statistics after import"""
    stats = await conn.fetchrow("""
        SELECT
            COUNT(*) as total_records,
            COUNT(CASE WHEN cars_standard_id IS NOT NULL THEN 1 END) as mapped_records,
            COUNT(CASE WHEN cars_standard_id IS NULL THEN 1 END) as unmapped_records,
            AVG(price) as avg_price,
            MIN(price) as min_price,
            MAX(price) as max_price
        FROM carsome
    """)

    logger.info("📊 Import Statistics:")
    logger.info(f"   Total records: {stats['total_records']}")
    logger.info(f"   Mapped to cars_standard: {stats['mapped_records']}")
    logger.info(f"   Unmapped: {stats['unmapped_records']}")
    logger.info(f"   Price range: {stats['min_price']} - {stats['max_price']}")
    logger.info(f"   Average price: {stats['avg_price']:.0f}" if stats['avg_price'] else "   Average price: N/A")

async def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Import Carsome CSV data to database')
    parser.add_argument('--csv', default='scrapes_export.csv', help='Path to CSV file')
    parser.add_argument('--create-table', action='store_true', help='Create carsome table')
    parser.add_argument('--map-standard', action='store_true', help='Map to cars_standard table')

    args = parser.parse_args()

    csv_file_path = args.csv

    if not os.path.exists(csv_file_path):
        logger.error(f"CSV file not found: {csv_file_path}")
        return

    logger.info(f"🚀 Starting Carsome import process...")
    logger.info(f"📁 CSV file: {csv_file_path}")
    logger.info(f"🗄️  Database: {DB_NAME}")

    conn = None
    try:
        conn = await get_db_connection()

        # Create table if requested
        if args.create_table:
            await create_table(conn)

        # Import CSV data
        await import_csv_data(conn, csv_file_path)

        # Map to cars_standard if requested
        if args.map_standard:
            await map_cars_standard_id(conn)

        # Show statistics
        await get_import_stats(conn)

        logger.info("✅ Import process completed successfully!")

    except Exception as e:
        logger.error(f"❌ Error during import: {e}")
        raise
    finally:
        if conn:
            await conn.close()

if __name__ == "__main__":
    asyncio.run(main())