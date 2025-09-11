"""
Standalone Car Data Sync Script
===============================

Sync car data from remote scraping database to unified local database.
Can be run independently without Django management commands.

Usage:
    python sync_cars.py                     # Today's data only
    python sync_cars.py --days 7            # Last 7 days
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
try:
    from fill_cars_category_id import fill_all_category_id
    from fill_cars_standard_id import fill_all_cars_standard_id
except ImportError as e:
    logger.warning(f"⚠️ Could not import fill functions: {e}")
    fill_all_category_id = None
    fill_all_cars_standard_id = None


class DatabaseConfig:
    """Database configuration"""
    
    def __init__(self):
        # Source database (scraping data) - from SOURCE_DB_* env vars
        self.SOURCE_DB = {
            'host': os.getenv('SOURCE_DB_HOST', '127.0.0.1'),
            'port': int(os.getenv('SOURCE_DB_PORT', 5432)),
            'database': os.getenv('SOURCE_DB_NAME', 'db_scrap_new'),
            'user': os.getenv('SOURCE_DB_USER', 'fanfan'),
            'password': os.getenv('SOURCE_DB_PASSWORD', 'cenanun')
        }
        
        # Target database (car market price) - from DB_* env vars
        self.TARGET_DB = {
            'host': os.getenv('DB_HOST', '127.0.0.1'),
            'port': int(os.getenv('DB_PORT', 5432)),
            'database': os.getenv('DB_NAME', 'db_carmarketprice_new_2'),
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
    
    
    async def fetch_source_data(self, table_name: str, days_back: Optional[int] = None, 
                               fetch_all: bool = False) -> List[Dict[str, Any]]:
        """Fetch data from source database"""
        conn = None
        try:
            conn = await asyncpg.connect(**self.config.SOURCE_DB)
            
            base_query = f"SELECT * FROM public.{table_name}"
            
            if fetch_all:
                query = f"{base_query} ORDER BY last_scraped_at DESC"
            elif days_back:
                query = f"""
                    {base_query} 
                    WHERE last_scraped_at >= (NOW() - INTERVAL '{days_back} days')
                    ORDER BY last_scraped_at DESC
                """
            else:
                # Default: today's data
                today = datetime.now().strftime('%Y-%m-%d')
                query = f"{base_query} WHERE information_ads_date = '{today}'"
            
            logger.info(f"🔍 Fetching {table_name} data...")
            result = await conn.fetch(query)
            return [dict(row) for row in result]
            
        except Exception as e:
            logger.error(f"❌ Error fetching {table_name} data: {e}")
            return []
        finally:
            if conn:
                await conn.close()
    
    async def fetch_price_history_data(self, table_name: str, days_back: Optional[int] = None,
                                     fetch_all: bool = False) -> List[Dict[str, Any]]:
        """Fetch price history data from source database"""
        conn = None
        try:
            conn = await asyncpg.connect(**self.config.SOURCE_DB)
            
            base_query = f"SELECT * FROM public.{table_name}"
            
            if fetch_all:
                query = f"{base_query} ORDER BY changed_at DESC"
            elif days_back:
                query = f"""
                    {base_query} 
                    WHERE changed_at >= (NOW() - INTERVAL '{days_back} days')
                    ORDER BY changed_at DESC
                """
            else:
                # Default: 30 days
                query = f"""
                    {base_query} 
                    WHERE changed_at >= (NOW() - INTERVAL '30 days')
                    ORDER BY changed_at DESC
                """
            
            logger.info(f"📈 Fetching {table_name} price history...")
            result = await conn.fetch(query)
            return [dict(row) for row in result]
            
        except Exception as e:
            logger.error(f"❌ Error fetching {table_name} price history: {e}")
            return []
        finally:
            if conn:
                await conn.close()
    
    def normalize_car_data(self, data: Dict[str, Any], source: str) -> Dict[str, Any]:
        """Normalize car data from different sources"""
        normalized = {
            'source': source,
            'listing_url': data['listing_url'],
            'condition': data.get('condition'),
            'brand': data.get('brand'),
            'model': data.get('model'),
            'variant': data.get('variant'),
            'year': data.get('year'),
            'mileage': data.get('mileage'),
            'transmission': data.get('transmission'),
            'seat_capacity': data.get('seat_capacity'),
            'engine_cc': data.get('engine_cc'),
            'fuel_type': data.get('fuel_type'),
            'price': data.get('price'),
            'location': data.get('location'),
            'information_ads': data.get('information_ads'),
            'images': data.get('images'),
            'status': data.get('status', 'active'),
            'ads_tag': data.get('ads_tag'),
            'is_deleted': data.get('is_deleted', False),
            'last_scraped_at': data.get('last_scraped_at'),
            'version': data.get('version', 1),
            'sold_at': data.get('sold_at'),
            'last_status_check': data.get('last_status_check'),
            'information_ads_date': data.get('information_ads_date'),
        }
        
        # Handle model_group differences
        if source == 'carlistmy':
            normalized['model_group'] = data.get('model_group')
        elif source == 'mudahmy':
            normalized['model_group'] = data.get('model_group', 'NO MODEL GROUP')
        
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
            # Check required fields
            if not data.get('brand') or not data.get('brand').strip():
                invalid_count += 1
                logger.warning(f"⚠️ Skipped record - missing brand: {data.get('source', 'unknown')} - {data.get('listing_url', 'no url')}")
                continue
                
            if not data.get('model') or not data.get('model').strip():
                invalid_count += 1
                logger.warning(f"⚠️ Skipped record - missing model: {data.get('source', 'unknown')} - {data.get('listing_url', 'no url')}")
                continue
                
            if not data.get('condition') or not data.get('condition').strip():
                invalid_count += 1
                logger.warning(f"⚠️ Skipped record - missing condition: {data.get('source', 'unknown')} - {data.get('listing_url', 'no url')}")
                continue
                
            valid_data.append(data)
        
        if invalid_count > 0:
            logger.warning(f"❌ {invalid_count} records skipped due to missing required fields (brand/model/condition)")
        
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
                    source, listing_url, condition, brand, model_group, model, variant,
                    year, mileage, transmission, seat_capacity, engine_cc, fuel_type, price,
                    location, information_ads, images, status, ads_tag, is_deleted,
                    last_scraped_at, version, sold_at, last_status_check, information_ads_date
                ) VALUES %s
                ON CONFLICT (source, listing_url)
                DO UPDATE SET
                    listing_url = EXCLUDED.listing_url,
                    condition = EXCLUDED.condition,
                    brand = EXCLUDED.brand,
                    model_group = EXCLUDED.model_group,
                    model = EXCLUDED.model,
                    variant = EXCLUDED.variant,
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
                    ads_tag = EXCLUDED.ads_tag,
                    is_deleted = EXCLUDED.is_deleted,
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
                    data['source'], data['listing_url'], data['condition'],
                    data['brand'], data['model_group'], data['model'], data['variant'],
                    data['year'], data['mileage'], data['transmission'], data['seat_capacity'],
                    data['engine_cc'], data['fuel_type'], data['price'], data['location'],
                    data['information_ads'], data['images'], data['status'], data['ads_tag'],
                    data['is_deleted'], data['last_scraped_at'], data['version'],
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
    
    def sync_price_history_direct(self, price_data: List[Dict[str, Any]], source: str) -> Tuple[int, int, int]:
        """Direct UPSERT price history without matching"""
        if not price_data:
            return 0, 0, 0
            
        inserted = 0
        updated = 0
        skipped = 0
        
        # Filter valid data - ensure listing_url exists
        valid_data = []
        for data in price_data:
            if not data.get('listing_url') or not data.get('listing_url').strip():
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
            
            logger.info(f"📈 Direct UPSERT for {len(valid_data)} {source} price history records...")
            
            # Bulk UPSERT using PostgreSQL ON CONFLICT (without created_at)
            upsert_query = """
                INSERT INTO price_history_unified (
                    source, listing_url, old_price, new_price, changed_at
                ) VALUES %s
                ON CONFLICT (listing_url, changed_at)
                DO UPDATE SET
                    source = EXCLUDED.source,
                    old_price = EXCLUDED.old_price,
                    new_price = EXCLUDED.new_price
                RETURNING (xmax = 0) AS inserted
            """
            
            values = []
            for data in valid_data:
                values.append((
                    source, data['listing_url'], data['old_price'], data['new_price'],
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
    
    async def sync_all_data(self, days_back: Optional[int] = None, 
                          fetch_all: bool = False) -> Dict[str, Any]:
        """Main sync method with proper sequence"""
        
        logger.info("🚀 Starting unified car data synchronization...")
        logger.info(f"📅 Mode: {'All data' if fetch_all else f'{days_back} days' if days_back else 'Today only'}")
        
        try:
            # STEP 1: No need to load mappings - fill scripts will handle it
            logger.info("📋 Skipping mapping loads - will use fill scripts after UPSERT")
            
            # STEP 2: Fetch car data from both sources
            logger.info("📡 Fetching car data from both sources...")
            carlistmy_data, mudahmy_data = await asyncio.gather(
                self.fetch_source_data('cars_scrap_carlistmy', days_back, fetch_all),
                self.fetch_source_data('cars_scrap_mudahmy', days_back, fetch_all)
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
            
            if car_inserted > 0 or car_updated > 0:
                logger.info("📈 Fetching price history data...")
                carlistmy_prices, mudahmy_prices = await asyncio.gather(
                    self.fetch_price_history_data('price_history_scrap_carlistmy', days_back, fetch_all),
                    self.fetch_price_history_data('price_history_scrap_mudahmy', days_back, fetch_all)
                )
                
                logger.info(f"📈 CarlistMY price history: {len(carlistmy_prices)} records")
                logger.info(f"📈 MudahMY price history: {len(mudahmy_prices)} records")
                
                # Sync price history using direct UPSERT
                if len(carlistmy_prices) > 0:
                    price_carlistmy_inserted, price_carlistmy_updated, price_carlistmy_not_found = self.sync_price_history_direct(carlistmy_prices, 'carlistmy')
                    logger.info(f"✅ CarlistMY price history: {price_carlistmy_inserted} inserted, {price_carlistmy_updated} updated")

                if len(mudahmy_prices) > 0:
                    price_mudahmy_inserted, price_mudahmy_updated, price_mudahmy_not_found = self.sync_price_history_direct(mudahmy_prices, 'mudahmy')
                    logger.info(f"✅ MudahMY price history: {price_mudahmy_inserted} inserted, {price_mudahmy_updated} updated")
            else:
                logger.info("⏭️ No car data changes, skipping price history sync")
            
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
        print(f"   Skipped: {cars['skipped']} (missing brand/model/condition)")
    
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
    parser.add_argument('--all', action='store_true', help='Sync all data')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Determine sync parameters
    days_back = None
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
    elif args.days:
        days_back = args.days
    
    # Validation
    if fetch_all and days_back:
        print("❌ Cannot use both --all and --days together")
        sys.exit(1)
    
    # Display configuration
    print("🚀 Car Data Synchronization Script")
    print("-" * 40)
    if fetch_all:
        print("📅 Mode: Sync ALL data")
    elif days_back:
        print(f"📅 Mode: Sync last {days_back} days")
    else:
        print("📅 Mode: Sync today's data only")
    
    # Initialize config and display database settings
    config = DatabaseConfig()
    config.log_config()
    print()
    
    # Run sync
    try:
        service = CarDataSyncService(config)
        
        summary = await service.sync_all_data(
            days_back=days_back,
            fetch_all=fetch_all
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