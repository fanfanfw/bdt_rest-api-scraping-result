"""
Standalone Car Data Sync Script
===============================

Sync car data from remote scraping database to unified local database.
Can be run independently without Django management commands.

Usage:
    python sync_cars.py                     # Today's data (by last_scraped_at)
    python sync_cars.py --days 7            # Last 7 days
    python sync_cars.py --hours 6           # Last 6 hours
    python sync_cars.py --since 2026-02-06T00:00:00  # Since timestamp (local time)
    python sync_cars.py --days 30           # Last 30 days
    python sync_cars.py --all               # All data
    python sync_cars.py today               # Quick today sync
    python sync_cars.py week                # Quick weekly sync
    python sync_cars.py month               # Quick monthly sync
"""

import asyncio
import asyncpg
import psycopg2
from psycopg2.extras import RealDictCursor
import logging
import csv
import sys
import argparse
import os
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Tuple
import json
from dotenv import load_dotenv

# Load environment variables
load_dotenv(override=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Import functions from fill scripts
fill_all_category_id = None
fill_all_cars_standard_id = None

try:
    from fill_cars_category_id import fill_all_category_id
    logger.info("✅ fill_cars_category_id imported successfully")
except ImportError:
    logger.warning("⚠️ fill_cars_category_id not available (optional)")

try:
    from fill_cars_standard_id import fill_all_cars_standard_id
    logger.info("✅ fill_cars_standard_id imported successfully")
except ImportError:
    logger.warning("⚠️ fill_cars_standard_id not available (optional)")


class DatabaseConfig:
    """Database configuration"""
    
    def __init__(self):
        # Source database (scraping data) - from SOURCE_DB_* env vars
        self.SOURCE_DB = {
            'host': os.getenv('SOURCE_DB_HOST', '127.0.0.1'),
            'port': int(os.getenv('SOURCE_DB_PORT', 5432)),
            'database': os.getenv('SOURCE_DB_NAME', 'db_cars_scrap'),
            'user': os.getenv('SOURCE_DB_USER', 'fanfan'),
            'password': os.getenv('SOURCE_DB_PASSWORD', 'cenanun')
        }
        
        # Target database (car market price) - from DB_* env vars
        self.TARGET_DB = {
            'host': os.getenv('DB_HOST', '127.0.0.1'),
            'port': int(os.getenv('DB_PORT', 5432)),
            'database': os.getenv('DB_NAME', 'db_test'),
            'user': os.getenv('DB_USER', 'fanfan'),
            'password': os.getenv('DB_PASSWORD', 'cenanun')
        }
    
    def log_config(self):
        """Log database configuration (without passwords)"""
        logger.info("🔧 Database Configuration:")
        logger.info(f"   Source DB: {self.SOURCE_DB['user']}@{self.SOURCE_DB['host']}:{self.SOURCE_DB['port']}/{self.SOURCE_DB['database']}")
        logger.info(f"   Target DB: {self.TARGET_DB['user']}@{self.TARGET_DB['host']}:{self.TARGET_DB['port']}/{self.TARGET_DB['database']}")


class CarDataSyncService:
    """Standalone car data sync service"""

    def __init__(self, config: DatabaseConfig):
        self.config = config

    def normalize_field(self, text, default_value=None):
        """
        Normalize field text with improved flow:
        1. Handle empty/null values first
        2. Clean symbols and replace with spaces
        3. Remove extra spaces
        4. Convert to uppercase for consistency
        """
        if not text or str(text).strip() in ["-", "N/A", "", "null", "NULL"]:
            return default_value

        try:
            text_str = str(text).strip()

            # Step 1: Replace symbols with spaces (but not multiple consecutive symbols)
            import re
            cleaned = re.sub(r'[-_()]+', ' ', text_str)  # Replace consecutive symbols with single space

            # Step 2: Remove other unwanted characters (keep only alphanumeric and spaces)
            cleaned = re.sub(r'[^\w\s]', '', cleaned)

            # Step 3: Remove multiple spaces and normalize
            cleaned = ' '.join(cleaned.split())

            # Step 4: Convert to uppercase for consistency
            cleaned = cleaned.upper()

            return cleaned if cleaned else default_value

        except Exception as e:
            logger.warning(f"Error normalizing field '{text}': {e}")
            return default_value

    def normalize_brand_name(self, brand_str):
        """Normalize brand name using improved normalize_field"""
        if not brand_str or brand_str == "N/A":
            return brand_str

        try:
            normalized = self.normalize_field(brand_str, brand_str)
            logger.info(f"Brand normalized: '{brand_str}' -> '{normalized}'")
            return normalized

        except Exception as e:
            logger.warning(f"Error normalizing brand '{brand_str}': {e}")
            return brand_str
    
    
    async def fetch_source_data(
        self,
        table_name: str,
        days_back: Optional[int] = None,
        hours_back: Optional[int] = None,
        since: Optional[datetime] = None,
        fetch_all: bool = False,
        use_ads_date_for_today: bool = False,
    ) -> List[Dict[str, Any]]:
        """Fetch data from source database"""
        conn = None
        try:
            conn = await asyncpg.connect(**self.config.SOURCE_DB)
            
            base_query = f"SELECT * FROM public.{table_name}"

            query_args: List[Any] = []
            if fetch_all:
                query = f"{base_query} ORDER BY last_scraped_at DESC"
            elif since:
                query = f"""
                    {base_query}
                    WHERE last_scraped_at >= $1
                    ORDER BY last_scraped_at DESC
                """
                query_args = [since]
            elif hours_back:
                query = f"""
                    {base_query}
                    WHERE last_scraped_at >= (NOW() - ($1 * INTERVAL '1 hour'))
                    ORDER BY last_scraped_at DESC
                """
                query_args = [hours_back]
            elif days_back:
                query = f"""
                    {base_query}
                    WHERE last_scraped_at >= (NOW() - ($1 * INTERVAL '1 day'))
                    ORDER BY last_scraped_at DESC
                """
                query_args = [days_back]
            else:
                if use_ads_date_for_today:
                    query = f"""
                        {base_query}
                        WHERE information_ads_date = CURRENT_DATE
                    """
                else:
                    query = f"""
                        {base_query}
                        WHERE last_scraped_at >= date_trunc('day', NOW())
                        ORDER BY last_scraped_at DESC
                    """
            
            logger.info(f"🔍 Fetching {table_name} data...")
            result = await conn.fetch(query, *query_args)
            return [dict(row) for row in result]
            
        except Exception as e:
            logger.error(f"❌ Error fetching {table_name} data: {e}")
            return []
        finally:
            if conn:
                await conn.close()
    
    async def fetch_price_history_data(
        self,
        table_name: str,
        days_back: Optional[int] = None,
        hours_back: Optional[int] = None,
        since: Optional[datetime] = None,
        fetch_all: bool = False,
    ) -> List[Dict[str, Any]]:
        """Fetch price history data from source database"""
        conn = None
        try:
            conn = await asyncpg.connect(**self.config.SOURCE_DB)
            
            base_query = f"SELECT * FROM public.{table_name}"

            query_args: List[Any] = []
            if fetch_all:
                query = f"{base_query} ORDER BY changed_at DESC"
            elif since:
                query = f"""
                    {base_query}
                    WHERE changed_at >= $1
                    ORDER BY changed_at DESC
                """
                query_args = [since]
            elif hours_back:
                query = f"""
                    {base_query}
                    WHERE changed_at >= (NOW() - ($1 * INTERVAL '1 hour'))
                    ORDER BY changed_at DESC
                """
                query_args = [hours_back]
            elif days_back:
                query = f"""
                    {base_query}
                    WHERE changed_at >= (NOW() - ($1 * INTERVAL '1 day'))
                    ORDER BY changed_at DESC
                """
                query_args = [days_back]
            else:
                query = f"""
                    {base_query}
                    WHERE changed_at >= (NOW() - INTERVAL '30 days')
                    ORDER BY changed_at DESC
                """
            
            logger.info(f"📈 Fetching {table_name} price history...")
            result = await conn.fetch(query, *query_args)
            return [dict(row) for row in result]
            
        except Exception as e:
            logger.error(f"❌ Error fetching {table_name} price history: {e}")
            return []
        finally:
            if conn:
                await conn.close()
    
    def normalize_car_data(self, data: Dict[str, Any], source: str) -> Dict[str, Any]:
        """Normalize car data from different sources with improved field cleaning"""
        images = data.get("images")
        if images is not None and isinstance(images, str):
            images = [images]

        normalized = {
            'source': source,
            'listing_id': str(data.get('listing_id')) if data.get('listing_id') is not None else None,
            'listing_url': data['listing_url'],
            'condition': self.normalize_field(data.get('condition')),
            'brand': self.normalize_field(data.get('brand')),
            'model': self.normalize_field(data.get('model')),
            'variant': self.normalize_field(data.get('variant')),
            'series': self.normalize_field(data.get('series')),
            'type': self.normalize_field(data.get('type')),
            'year': data.get('year'),
            'mileage': data.get('mileage'),
            'transmission': self.normalize_field(data.get('transmission')),
            'seat_capacity': data.get('seat_capacity'),
            'engine_cc': data.get('engine_cc'),
            'fuel_type': self.normalize_field(data.get('fuel_type')),
            'price': data.get('price'),
            'location': self.normalize_field(data.get('location')),
            'information_ads': data.get('information_ads'),
            'images': images,
            'status': data.get('status', 'active'),
            'is_deleted': data.get('is_deleted', False),
            'created_at': data.get('created_at'),
            'last_scraped_at': data.get('last_scraped_at'),
            'version': data.get('version', 1),
            'sold_at': data.get('sold_at'),
            'last_status_check': data.get('last_status_check'),
            'information_ads_date': data.get('information_ads_date'),
        }

        return normalized
    
    def sync_to_target_database(self, normalized_data: List[Dict[str, Any]]) -> Tuple[int, int, int]:
        """Sync normalized data to target database using efficient UPSERT approach"""
        if not normalized_data:
            return 0, 0, 0
            
        inserted = 0
        updated = 0
        
        # STEP 0: Validate and filter data
        valid_data = []
        invalid_count = 0
        
        for data in normalized_data:
            # Helper function to check if field is empty/invalid
            def is_empty_field(value):
                if value is None:
                    return True
                if isinstance(value, str):
                    return value.strip() in ["", "-", "N/A", "null", "NULL"]
                if isinstance(value, (int, float)):
                    return value == 0
                return not value

            # Check REQUIRED fields - skip record if any is empty
            # Required: brand, model, variant, price, year, mileage
            required_fields = ['brand', 'model', 'variant', 'price', 'year', 'mileage']
            skip_record = False

            for field in required_fields:
                if is_empty_field(data.get(field)):
                    invalid_count += 1
                    logger.warning(f"⚠️ Skipped record - missing/empty {field}: {data.get('source', 'unknown')} - {data.get('listing_url', 'no url')}")
                    skip_record = True
                    break

            if skip_record:
                continue

            valid_data.append(data)
        
        if invalid_count > 0:
            logger.warning(f"❌ {invalid_count} records skipped due to missing required fields (brand/model/variant/price/year/mileage)")
        
        if not valid_data:
            logger.warning("❌ No valid records to process")
            return 0, 0, invalid_count
        
        logger.info(f"✅ {len(valid_data)} valid records will be processed")
        
        conn = None
        try:
            conn = psycopg2.connect(**self.config.TARGET_DB)
            cur = conn.cursor(cursor_factory=RealDictCursor)
            
            logger.info(f"💾 Performing bulk UPSERT for {len(valid_data)} valid records...")
            
            # Debug: Check sample data
            if valid_data:
                sample = valid_data[0]
                logger.info(f"🔍 Sample record: {sample['source']} - {sample['brand']} {sample.get('model', 'NO MODEL')} - {sample['listing_url']}")
            
            # STEP 1: Bulk UPSERT using PostgreSQL ON CONFLICT (without created_at, updated_at)
            upsert_query = """
                INSERT INTO cars_unified (
                    source, listing_url, listing_id, condition, brand, model, variant, series, type,
                    year, mileage, transmission, seat_capacity, engine_cc, fuel_type, price,
                    location, information_ads, images, status, is_deleted,
                    created_at, last_scraped_at, version, sold_at, last_status_check, information_ads_date
                ) VALUES %s
                ON CONFLICT (source, listing_url)
                DO UPDATE SET
                    listing_url = EXCLUDED.listing_url,
                    listing_id = EXCLUDED.listing_id,
                    condition = EXCLUDED.condition,
                    brand = EXCLUDED.brand,
                    model = EXCLUDED.model,
                    variant = EXCLUDED.variant,
                    series = EXCLUDED.series,
                    type = EXCLUDED.type,
                    year = EXCLUDED.year,
                    mileage = EXCLUDED.mileage,
                    transmission = EXCLUDED.transmission,
                    seat_capacity = EXCLUDED.seat_capacity,
                    engine_cc = EXCLUDED.engine_cc,
                    fuel_type = EXCLUDED.fuel_type,
                    price = EXCLUDED.price,
                    location = EXCLUDED.location,
                    information_ads = EXCLUDED.information_ads,
                    images = EXCLUDED.images,
                    status = EXCLUDED.status,
                    is_deleted = EXCLUDED.is_deleted,
                    created_at = COALESCE(cars_unified.created_at, EXCLUDED.created_at),
                    last_scraped_at = EXCLUDED.last_scraped_at,
                    version = EXCLUDED.version,
                    sold_at = EXCLUDED.sold_at,
                    last_status_check = EXCLUDED.last_status_check,
                    information_ads_date = EXCLUDED.information_ads_date
                RETURNING (xmax = 0) AS inserted
            """
            
            values = []
            for data in valid_data:
                values.append((
                    data['source'], data['listing_url'], data.get('listing_id'), data['condition'],
                    data['brand'], data['model'], data['variant'], data.get('series'), data.get('type'),
                    data['year'], data['mileage'], data['transmission'], data['seat_capacity'],
                    data['engine_cc'], data['fuel_type'], data['price'], data['location'],
                    data['information_ads'], data['images'], data['status'], data['is_deleted'],
                    data.get('created_at'), data['last_scraped_at'], data['version'],
                    data['sold_at'], data['last_status_check'], data['information_ads_date']
                ))
            
            # Execute bulk upsert using psycopg2.extras.execute_values
            from psycopg2.extras import execute_values
            
            try:
                # Execute bulk upsert directly
                result = execute_values(
                    cur, upsert_query, values,
                    template=None, page_size=1000, fetch=True
                )
                
                # Count results  
                inserted = sum(1 for row in result if row['inserted'])
                updated = len(result) - inserted
                
                logger.info(f"✅ UPSERT completed: {inserted} inserted, {updated} updated")
                
            except Exception as upsert_error:
                logger.error(f"❌ UPSERT failed: {upsert_error}")
                logger.error(f"❌ UPSERT error type: {type(upsert_error)}")
                import traceback
                logger.error(f"❌ Full traceback: {traceback.format_exc()}")
                
                # Fallback: tidak ada fallback, raise error
                logger.error("❌ UPSERT failed and no fallback available")
                raise
            
            # Commit the UPSERT first
            conn.commit()

        except Exception as e:
            logger.error(f"❌ Database sync error: {e}")
            if conn:
                conn.rollback()
            return 0, 0, invalid_count
        finally:
            if conn:
                conn.close()

        return inserted, updated, invalid_count
    
    def sync_price_history_direct(
        self,
        price_data: List[Dict[str, Any]],
        source: str,
    ) -> Tuple[int, int, int]:
        """Direct UPSERT price history without matching"""
        if not price_data:
            return 0, 0, 0
            
        inserted = 0
        updated = 0
        skipped = 0
        
        # Filter valid data - ensure required fields exist
        valid_data = []

        def is_empty_field(value):
            if value is None:
                return True
            if isinstance(value, str):
                return value.strip() in ["", "-", "N/A", "null", "NULL"]
            if isinstance(value, (int, float)):
                return value == 0
            return not value

        for data in price_data:
            # Required fields for price_history: listing_id, listing_url, old_price, new_price, changed_at
            if (is_empty_field(data.get('listing_url')) or
                is_empty_field(data.get('listing_id')) or
                is_empty_field(data.get('old_price')) or
                is_empty_field(data.get('new_price')) or
                is_empty_field(data.get('changed_at'))):
                skipped += 1
                continue

            valid_data.append(data)
        
        if not valid_data:
            logger.warning(f"❌ No valid {source} price history records to process")
            return 0, 0, skipped
        
        conn = None
        try:
            conn = psycopg2.connect(**self.config.TARGET_DB)
            cur = conn.cursor(cursor_factory=RealDictCursor)
            
            # Filter out price history for listings that do not exist in cars_unified (in the target DB),
            # so incremental sync remains correct even when the current run only syncs a subset of cars.
            listing_urls = sorted({row["listing_url"] for row in valid_data})
            existing_urls = set()
            chunk_size = 10000
            for i in range(0, len(listing_urls), chunk_size):
                chunk = listing_urls[i : i + chunk_size]
                cur.execute(
                    "SELECT listing_url FROM cars_unified WHERE source = %s AND listing_url = ANY(%s::text[])",
                    (source, chunk),
                )
                existing_urls.update(row["listing_url"] for row in cur.fetchall())

            filtered_data = [row for row in valid_data if row["listing_url"] in existing_urls]
            skipped += len(valid_data) - len(filtered_data)

            if not filtered_data:
                logger.warning(f"❌ No valid {source} price history records to process after cars_unified filter")
                return 0, 0, skipped

            logger.info(f"📈 Direct UPSERT for {len(filtered_data)} {source} price history records...")
            
            # Bulk UPSERT using PostgreSQL ON CONFLICT (without created_at)
            upsert_query = """
                INSERT INTO price_history_unified (
                    source, listing_id, listing_url, old_price, new_price, changed_at
                ) VALUES %s
                ON CONFLICT (listing_url, changed_at)
                DO UPDATE SET
                    source = EXCLUDED.source,
                    listing_id = EXCLUDED.listing_id,
                    old_price = EXCLUDED.old_price,
                    new_price = EXCLUDED.new_price
                RETURNING (xmax = 0) AS inserted
            """
            
            values = []
            for data in filtered_data:
                values.append((
                    source,
                    str(data["listing_id"]) if data.get("listing_id") is not None else None,
                    data['listing_url'],
                    data['old_price'],
                    data['new_price'],
                    data['changed_at']
                ))
            
            # Execute bulk upsert
            from psycopg2.extras import execute_values
            
            try:
                result = execute_values(
                    cur, upsert_query, values,
                    template=None, page_size=1000, fetch=True
                )
                
                # Count results  
                inserted = sum(1 for row in result if row['inserted'])
                updated = len(result) - inserted
                
                logger.info(f"✅ {source} price history UPSERT: {inserted} inserted, {updated} updated")
                
            except Exception as upsert_error:
                logger.error(f"❌ Price history UPSERT failed: {upsert_error}")
                # Could add fallback here if needed
                raise
            
            conn.commit()
            
        except Exception as e:
            logger.error(f"❌ Price history sync error: {e}")
            if conn:
                conn.rollback()
            return 0, 0, skipped
        finally:
            if conn:
                conn.close()
        
        return inserted, updated, skipped
    
    async def sync_all_data(
        self,
        days_back: Optional[int] = None,
        hours_back: Optional[int] = None,
        since: Optional[datetime] = None,
        fetch_all: bool = False,
        use_ads_date_for_today: bool = False,
    ) -> Dict[str, Any]:
        """Main sync method with proper sequence"""
        
        logger.info("🚀 Starting unified car data synchronization...")
        if fetch_all:
            mode_label = "All data"
        elif since:
            mode_label = f"Since {since.isoformat()}"
        elif hours_back:
            mode_label = f"Last {hours_back} hours"
        elif days_back:
            mode_label = f"Last {days_back} days"
        else:
            mode_label = "Today only"
            if use_ads_date_for_today:
                mode_label += " (information_ads_date)"
            else:
                mode_label += " (last_scraped_at)"

        logger.info(f"📅 Mode: {mode_label}")
        
        try:
            # STEP 1: No need to load mappings - fill scripts will handle it
            logger.info("📋 Skipping mapping loads - will use fill scripts after UPSERT")
            
            # STEP 2: Fetch car data from both sources
            logger.info("📡 Fetching car data from both sources...")
            carlistmy_data, mudahmy_data = await asyncio.gather(
                self.fetch_source_data(
                    'cars_scrap_carlistmy',
                    days_back=days_back,
                    hours_back=hours_back,
                    since=since,
                    fetch_all=fetch_all,
                    use_ads_date_for_today=use_ads_date_for_today,
                ),
                self.fetch_source_data(
                    'cars_scrap_mudahmy',
                    days_back=days_back,
                    hours_back=hours_back,
                    since=since,
                    fetch_all=fetch_all,
                    use_ads_date_for_today=use_ads_date_for_today,
                ),
            )
            
            logger.info(f"📊 CarlistMY data: {len(carlistmy_data)} records")
            logger.info(f"📊 MudahMY data: {len(mudahmy_data)} records")
            
            # STEP 3: Normalize data
            logger.info("🔄 Normalizing car data...")
            normalized_carlistmy = [self.normalize_car_data(data, 'carlistmy') for data in carlistmy_data]
            normalized_mudahmy = [self.normalize_car_data(data, 'mudahmy') for data in mudahmy_data]
            all_normalized_data = normalized_carlistmy + normalized_mudahmy
            
            # STEP 4: Sync car data using efficient UPSERT (without cars_standard_id and category_id)
            logger.info(f"💾 Syncing {len(all_normalized_data)} car records to target database...")
            car_inserted, car_updated, car_skipped = self.sync_to_target_database(all_normalized_data)
            logger.info(f"✅ Car data UPSERT completed: {car_inserted} inserted, {car_updated} updated, {car_skipped} skipped")
            
            # STEP 5: Sync price history FIRST (before fill scripts)
            price_carlistmy_inserted = 0
            price_carlistmy_updated = 0
            price_carlistmy_not_found = 0
            price_mudahmy_inserted = 0
            price_mudahmy_updated = 0
            price_mudahmy_not_found = 0

            logger.info("📈 Fetching price history data...")
            carlistmy_prices, mudahmy_prices = await asyncio.gather(
                self.fetch_price_history_data(
                    'price_history_scrap_carlistmy',
                    days_back=days_back,
                    hours_back=hours_back,
                    since=since,
                    fetch_all=fetch_all,
                ),
                self.fetch_price_history_data(
                    'price_history_scrap_mudahmy',
                    days_back=days_back,
                    hours_back=hours_back,
                    since=since,
                    fetch_all=fetch_all,
                ),
            )

            logger.info(f"📈 CarlistMY price history: {len(carlistmy_prices)} records")
            logger.info(f"📈 MudahMY price history: {len(mudahmy_prices)} records")

            if len(carlistmy_prices) > 0:
                price_carlistmy_inserted, price_carlistmy_updated, price_carlistmy_not_found = self.sync_price_history_direct(
                    carlistmy_prices,
                    'carlistmy',
                )
                logger.info(f"✅ CarlistMY price history: {price_carlistmy_inserted} inserted, {price_carlistmy_updated} updated, {price_carlistmy_not_found} skipped")

            if len(mudahmy_prices) > 0:
                price_mudahmy_inserted, price_mudahmy_updated, price_mudahmy_not_found = self.sync_price_history_direct(
                    mudahmy_prices,
                    'mudahmy',
                )
                logger.info(f"✅ MudahMY price history: {price_mudahmy_inserted} inserted, {price_mudahmy_updated} updated, {price_mudahmy_not_found} skipped")
            
            # STEP 6: Fill cars_standard_id only (skip category_id since it will be removed)
            cars_standard_updated = 0
            
            if car_inserted > 0 or car_updated > 0:
                logger.info("🔄 Running fill script for cars_standard_id only...")
                
                # Fill cars_standard_id using existing script (run in thread to avoid async issues)
                if fill_all_cars_standard_id:
                    logger.info("📋 Running fill_cars_standard_id script...")
                    try:
                        standard_result = await asyncio.to_thread(fill_all_cars_standard_id)
                        if standard_result and standard_result.get('status') == 'success':
                            cars_standard_updated = standard_result.get('total_updated', 0)
                            logger.info(f"✅ Cars standard ID filled: {cars_standard_updated} records")
                        else:
                            logger.warning(f"⚠️ Cars standard ID fill had issues: {standard_result}")
                    except Exception as e:
                        logger.error(f"❌ Error running fill_cars_standard_id: {e}")
                else:
                    logger.error("❌ fill_all_cars_standard_id function not available")
                
                logger.info(f"✅ Fill script completed: {cars_standard_updated} cars_standard_id updated")
            else:
                logger.info("⏭️ No car data changes, skipping fill script")
            
            # STEP 7: All done, prepare summary
            
            # STEP 8: Summary
            summary = {
                'cars': {
                    'total_fetched': len(all_normalized_data),
                    'inserted': car_inserted,
                    'updated': car_updated,
                    'skipped': car_skipped,
                    'carlistmy_records': len(normalized_carlistmy),
                    'mudahmy_records': len(normalized_mudahmy),
                },
                'fill_results': {
                    'cars_standard_updated': cars_standard_updated,
                },
                'price_history': {
                    'carlistmy': {
                        'inserted': price_carlistmy_inserted,
                        'updated': price_carlistmy_updated,
                        'skipped': price_carlistmy_not_found
                    },
                    'mudahmy': {
                        'inserted': price_mudahmy_inserted,
                        'updated': price_mudahmy_updated,
                        'skipped': price_mudahmy_not_found
                    }
                }
            }
            
            logger.info("✅ Unified car data synchronization completed!")
            return summary
            
        except Exception as e:
            logger.error(f"❌ Sync failed: {e}")
            raise


def display_summary(summary: Dict[str, Any]):
    """Display sync summary"""
    print("\n" + "="*60)
    print("✅ SYNCHRONIZATION COMPLETED")
    print("="*60)
    
    # Car data
    cars = summary['cars']
    print(f"🚗 CAR DATA:")
    print(f"   Total fetched: {cars['total_fetched']}")
    print(f"   - CarlistMY: {cars['carlistmy_records']}")
    print(f"   - MudahMY: {cars['mudahmy_records']}")
    print(f"   Inserted: {cars['inserted']}")
    print(f"   Updated: {cars['updated']}")
    if cars.get('skipped', 0) > 0:
        print(f"   Skipped: {cars['skipped']} (missing brand/model/condition/price/mileage/year)")
    
    # Fill results
    fill = summary.get('fill_results', {})
    if fill.get('cars_standard_updated', 0) > 0:
        print(f"\n🔄 FILL RESULTS:")
        print(f"   Cars Standard ID: {fill.get('cars_standard_updated', 0)} updated")
    
    # Price history
    ph = summary['price_history']
    print(f"\n📈 PRICE HISTORY:")
    print(f"   CarlistMY - Inserted: {ph['carlistmy']['inserted']}, Updated: {ph['carlistmy']['updated']}")
    if ph['carlistmy']['skipped'] > 0:
        print(f"   CarlistMY - Skipped: {ph['carlistmy']['skipped']} (invalid data)")
    
    print(f"   MudahMY - Inserted: {ph['mudahmy']['inserted']}, Updated: {ph['mudahmy']['updated']}")
    if ph['mudahmy']['skipped'] > 0:
        print(f"   MudahMY - Skipped: {ph['mudahmy']['skipped']} (invalid data)")
    
    # Overall stats
    total_processed = (ph['carlistmy']['inserted'] + ph['carlistmy']['updated'] + 
                      ph['mudahmy']['inserted'] + ph['mudahmy']['updated'])
    total_skipped = ph['carlistmy']['skipped'] + ph['mudahmy']['skipped']
    
    if total_skipped > 0:
        success_rate = (total_processed / (total_processed + total_skipped)) * 100
        print(f"   Overall success rate: {success_rate:.1f}% ({total_processed}/{total_processed + total_skipped})")
    
    print(f"\n🎉 Sync completed successfully!")


async def main():
    """Main function with command line argument parsing"""
    parser = argparse.ArgumentParser(description='Car Data Sync Script')
    parser.add_argument('mode', nargs='?', choices=['today', 'week', 'month', 'all-data'], 
                       help='Quick sync mode')
    parser.add_argument('--days', type=int, help='Number of days back to sync')
    parser.add_argument('--hours', type=int, help='Number of hours back to sync')
    parser.add_argument(
        '--since',
        type=str,
        help="Sync records since this ISO timestamp (e.g. 2026-02-06T00:00:00). Uses last_scraped_at/changed_at.",
    )
    parser.add_argument('--all', action='store_true', help='Sync all data')
    parser.add_argument(
        '--use-ads-date',
        action='store_true',
        help="For the default 'today' mode, filter by information_ads_date instead of last_scraped_at.",
    )
    parser.add_argument('--verbose', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Determine sync parameters
    days_back = None
    hours_back = None
    since = None
    fetch_all = False
    
    if args.mode:
        if args.mode == 'today':
            days_back = None
        elif args.mode == 'week':
            days_back = 7
        elif args.mode == 'month':
            days_back = 30
        elif args.mode == 'all-data':
            fetch_all = True
    elif args.all:
        fetch_all = True
    elif args.since:
        try:
            since_raw = args.since.strip().replace("Z", "+00:00")
            since = datetime.fromisoformat(since_raw)
        except Exception as exc:
            print(f"❌ Invalid --since value: {args.since} ({exc})", file=sys.stderr)
            sys.exit(2)
    elif args.hours:
        hours_back = args.hours
    elif args.days:
        days_back = args.days
    
    # Validation
    selected = sum(
        1
        for value in (
            fetch_all,
            days_back is not None,
            hours_back is not None,
            since is not None,
        )
        if value
    )
    if selected > 1:
        print("❌ Cannot combine --all/--days/--hours/--since together", file=sys.stderr)
        sys.exit(1)
    
    # Display configuration
    print("🚀 Car Data Synchronization Script")
    print("-" * 40)
    if fetch_all:
        print("📅 Mode: Sync ALL data")
    elif since:
        print(f"📅 Mode: Sync since {since.isoformat()}")
    elif hours_back:
        print(f"📅 Mode: Sync last {hours_back} hours")
    elif days_back:
        print(f"📅 Mode: Sync last {days_back} days")
    else:
        if args.use_ads_date:
            print("📅 Mode: Sync today only (information_ads_date)")
        else:
            print("📅 Mode: Sync today only (last_scraped_at)")
    
    # Initialize config and display database settings
    config = DatabaseConfig()
    config.log_config()
    print()
    
    # Run sync
    try:
        service = CarDataSyncService(config)
        
        summary = await service.sync_all_data(
            days_back=days_back,
            hours_back=hours_back,
            since=since,
            fetch_all=fetch_all,
            use_ads_date_for_today=args.use_ads_date,
        )
        
        display_summary(summary)
        
    except KeyboardInterrupt:
        print("\n❌ Sync interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Sync failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
