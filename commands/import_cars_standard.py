#!/usr/bin/env python3
"""
Import Cars Standard Script
==========================

Import cars_standard data from CSV file to database.
This should be run once before using sync_cars.py.

Usage:
    python import_cars_standard.py                          # Use cars_standard.csv
    python import_cars_standard.py --csv-path custom.csv    # Use custom CSV file
    python import_cars_standard.py --clear-only             # Only clear existing data
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import logging
import csv
import sys
import argparse
import os
from typing import Dict, Any
from dotenv import load_dotenv

# Load environment variables
load_dotenv(override=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DatabaseConfig:
    """Database configuration"""
    
    def __init__(self):
        # Target database (car market price) - from DB_* env vars
        self.TARGET_DB = {
            'host': os.getenv('DB_HOST', '127.0.0.1'),
            'port': int(os.getenv('DB_PORT', 5432)),
            'database': os.getenv('DB_NAME', 'db_carmarketprice_new_2'),
            'user': os.getenv('DB_USER', 'fanfan'),
            'password': os.getenv('DB_PASSWORD', 'cenanun')
        }
    
    def log_config(self):
        """Log database configuration (without password)"""
        logger.info("🔧 Database Configuration:")
        logger.info(f"   Target DB: {self.TARGET_DB['user']}@{self.TARGET_DB['host']}:{self.TARGET_DB['port']}/{self.TARGET_DB['database']}")


class CarsStandardImporter:
    """Import cars_standard data from CSV to database"""
    
    def __init__(self, config: DatabaseConfig):
        self.config = config
    
    def clear_existing_data(self):
        """Clear existing cars_standard data with foreign key check"""
        conn = None
        try:
            conn = psycopg2.connect(**self.config.TARGET_DB)
            cur = conn.cursor()
            
            # Get count before deletion
            cur.execute("SELECT COUNT(*) FROM cars_standard")
            existing_count = cur.fetchone()[0]
            
            if existing_count == 0:
                logger.info("🗑️ No existing cars_standard records to clear")
                return 0
            
            # Check for foreign key references
            try:
                cur.execute("""
                    SELECT COUNT(*) FROM cars_unified 
                    WHERE cars_standard_id IS NOT NULL
                """)
                referenced_count = cur.fetchone()[0]
                
                if referenced_count > 0:
                    logger.error(f"❌ Cannot clear cars_standard: {referenced_count} cars_unified records reference this data")
                    logger.info("💡 Solutions:")
                    logger.info("   1. Use UPSERT mode instead: python import_cars_standard.py --mode upsert")
                    logger.info("   2. Clear cars_unified first: UPDATE cars_unified SET cars_standard_id = NULL")
                    raise Exception(f"Foreign key constraint: {referenced_count} records in cars_unified reference cars_standard")
                    
            except psycopg2.ProgrammingError:
                # Table might not exist yet, ignore
                logger.warning("⚠️ Table cars_unified not found, proceeding with clear")
            
            # Clear data
            cur.execute("DELETE FROM cars_standard")
            conn.commit()
            
            logger.info(f"🗑️ Cleared {existing_count} existing cars_standard records")
            return existing_count
            
        except Exception as e:
            logger.error(f"❌ Failed to clear existing data: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                conn.close()
    
    def import_from_csv(self, csv_file_path: str = 'cars_standard.csv') -> int:
        """Import cars_standard data from CSV file"""
        conn = None
        try:
            conn = psycopg2.connect(**self.config.TARGET_DB)
            cur = conn.cursor(cursor_factory=RealDictCursor)
            
            imported_count = 0
            skipped_count = 0
            
            logger.info(f"📋 Reading CSV file: {csv_file_path}")
            
            with open(csv_file_path, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                
                for row_num, row in enumerate(reader, start=1):
                    try:
                        # Validate required fields
                        if not row.get('id') or not row.get('brand_norm'):
                            logger.warning(f"⚠️ Row {row_num}: Missing required fields, skipping")
                            skipped_count += 1
                            continue
                        
                        insert_query = """
                            INSERT INTO cars_standard (
                                id, brand_norm, model_group_norm, model_norm, variant_norm,
                                model_group_raw, model_raw, variant_raw, variant_raw2
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """
                        
                        # Helper function to convert CSV NULL string to actual None
                        def parse_csv_value(value):
                            if not value or value.strip().upper() == 'NULL':
                                return None
                            return value.strip()
                        
                        cur.execute(insert_query, (
                            int(row['id']),
                            parse_csv_value(row['brand_norm']),
                            parse_csv_value(row['model_group_norm']), 
                            parse_csv_value(row['model_norm']),
                            parse_csv_value(row['variant_norm']),
                            parse_csv_value(row.get('model_group_raw')),
                            parse_csv_value(row.get('model_raw')),
                            parse_csv_value(row.get('variant_raw')),
                            parse_csv_value(row.get('variant_raw2'))
                        ))
                        imported_count += 1
                        
                        # Progress indicator for large files
                        if imported_count % 500 == 0:
                            logger.info(f"📊 Imported {imported_count} records...")
                            
                    except Exception as e:
                        logger.error(f"❌ Error importing row {row_num}: {e}")
                        logger.error(f"Row data: {row}")
                        skipped_count += 1
                        continue
                
                conn.commit()
                logger.info(f"✅ Import completed: {imported_count} imported, {skipped_count} skipped")
                
                return imported_count
                
        except FileNotFoundError:
            logger.error(f"❌ CSV file not found: {csv_file_path}")
            raise
        except Exception as e:
            logger.error(f"❌ Import failed: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                conn.close()
    
    def upsert_from_csv(self, csv_file_path: str = 'cars_standard.csv') -> Dict[str, int]:
        """UPSERT cars_standard data from CSV file (safe for existing data)"""
        conn = None
        try:
            conn = psycopg2.connect(**self.config.TARGET_DB)
            cur = conn.cursor(cursor_factory=RealDictCursor)
            
            inserted_count = 0
            updated_count = 0
            skipped_count = 0
            
            logger.info(f"🔄 UPSERT mode: Reading CSV file: {csv_file_path}")
            
            with open(csv_file_path, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                
                for row_num, row in enumerate(reader, start=1):
                    try:
                        # Validate required fields
                        if not row.get('id') or not row.get('brand_norm'):
                            logger.warning(f"⚠️ Row {row_num}: Missing required fields, skipping")
                            skipped_count += 1
                            continue
                        
                        # Helper function to convert CSV NULL string to actual None
                        def parse_csv_value(value):
                            if not value or value.strip().upper() == 'NULL':
                                return None
                            return value.strip()
                        
                        # UPSERT query using PostgreSQL ON CONFLICT (without created_at, updated_at)
                        upsert_query = """
                            INSERT INTO cars_standard (
                                id, brand_norm, model_group_norm, model_norm, variant_norm,
                                model_group_raw, model_raw, variant_raw, variant_raw2
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (id) 
                            DO UPDATE SET
                                brand_norm = EXCLUDED.brand_norm,
                                model_group_norm = EXCLUDED.model_group_norm,
                                model_norm = EXCLUDED.model_norm,
                                variant_norm = EXCLUDED.variant_norm,
                                model_group_raw = EXCLUDED.model_group_raw,
                                model_raw = EXCLUDED.model_raw,
                                variant_raw = EXCLUDED.variant_raw,
                                variant_raw2 = EXCLUDED.variant_raw2
                            RETURNING (xmax = 0) AS inserted
                        """
                        
                        cur.execute(upsert_query, (
                            int(row['id']),
                            parse_csv_value(row['brand_norm']),
                            parse_csv_value(row['model_group_norm']), 
                            parse_csv_value(row['model_norm']),
                            parse_csv_value(row['variant_norm']),
                            parse_csv_value(row.get('model_group_raw')),
                            parse_csv_value(row.get('model_raw')),
                            parse_csv_value(row.get('variant_raw')),
                            parse_csv_value(row.get('variant_raw2'))
                        ))
                        
                        result = cur.fetchone()
                        if result and result['inserted']:
                            inserted_count += 1
                        else:
                            updated_count += 1
                        
                        # Progress indicator for large files
                        total_processed = inserted_count + updated_count
                        if total_processed % 500 == 0:
                            logger.info(f"📊 Processed {total_processed} records ({inserted_count} new, {updated_count} updated)...")
                            
                    except Exception as e:
                        logger.error(f"❌ Error processing row {row_num}: {e}")
                        logger.error(f"Row data: {row}")
                        skipped_count += 1
                        continue
                
                conn.commit()
                logger.info(f"✅ UPSERT completed: {inserted_count} inserted, {updated_count} updated, {skipped_count} skipped")
                
                return {
                    'inserted': inserted_count,
                    'updated': updated_count,
                    'skipped': skipped_count
                }
                
        except FileNotFoundError:
            logger.error(f"❌ CSV file not found: {csv_file_path}")
            raise
        except Exception as e:
            logger.error(f"❌ UPSERT failed: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                conn.close()
    
    def verify_import(self) -> Dict[str, Any]:
        """Verify imported data"""
        conn = None
        try:
            conn = psycopg2.connect(**self.config.TARGET_DB)
            cur = conn.cursor(cursor_factory=RealDictCursor)
            
            # Get total count
            cur.execute("SELECT COUNT(*) as total FROM cars_standard")
            total_count = cur.fetchone()['total']
            
            # Get brand statistics
            cur.execute("""
                SELECT brand_norm, COUNT(*) as count 
                FROM cars_standard 
                GROUP BY brand_norm 
                ORDER BY count DESC 
                LIMIT 10
            """)
            top_brands = cur.fetchall()
            
            # Get sample data
            cur.execute("""
                SELECT id, brand_norm, model_norm, variant_norm 
                FROM cars_standard 
                ORDER BY id 
                LIMIT 5
            """)
            sample_data = cur.fetchall()
            
            verification_data = {
                'total_count': total_count,
                'top_brands': list(top_brands),
                'sample_data': list(sample_data)
            }
            
            logger.info(f"📊 Verification: {total_count} total records imported")
            
            return verification_data
            
        except Exception as e:
            logger.error(f"❌ Verification failed: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def full_import(self, csv_file_path: str = 'cars_standard.csv', clear_first: bool = True) -> Dict[str, Any]:
        """Full import process with verification"""
        
        logger.info("🚀 Starting cars_standard import process...")
        
        try:
            # Step 1: Clear existing data (if requested)
            cleared_count = 0
            if clear_first:
                cleared_count = self.clear_existing_data()
            
            # Step 2: Import from CSV
            imported_count = self.import_from_csv(csv_file_path)
            
            # Step 3: Verify import
            verification = self.verify_import()
            
            # Summary
            summary = {
                'cleared_count': cleared_count,
                'imported_count': imported_count,
                'final_total': verification['total_count'],
                'top_brands': verification['top_brands'][:5],  # Top 5 only
                'sample_data': verification['sample_data']
            }
            
            logger.info("✅ Cars standard import completed successfully!")
            
            return summary
            
        except Exception as e:
            logger.error(f"❌ Import process failed: {e}")
            raise


def display_summary(summary: Dict[str, Any]):
    """Display import summary"""
    print("\n" + "="*60)
    print("✅ CARS STANDARD IMPORT COMPLETED")
    print("="*60)
    
    print(f"🗑️ Cleared: {summary['cleared_count']} existing records")
    print(f"📥 Imported: {summary['imported_count']} new records")
    print(f"📊 Final Total: {summary['final_total']} records in database")
    
    print(f"\n🏷️ TOP BRANDS:")
    for brand in summary['top_brands']:
        print(f"   {brand['brand_norm']}: {brand['count']} models")
    
    print(f"\n📋 SAMPLE DATA:")
    for item in summary['sample_data']:
        print(f"   ID {item['id']}: {item['brand_norm']} {item['model_norm']} {item['variant_norm']}")
    
    print(f"\n🎉 Import completed successfully!")


def main():
    """Main function with command line argument parsing"""
    parser = argparse.ArgumentParser(description='Import Cars Standard Data from CSV')
    parser.add_argument('--csv-path', default='commands/cars_standard.csv', 
                       help='Path to cars_standard.csv file (default: commands/cars_standard.csv)')
    parser.add_argument('--mode', choices=['upsert', 'clear-import'], default='upsert',
                       help='Import mode: upsert (safe, default) or clear-import (may fail with foreign keys)')
    parser.add_argument('--clear-only', action='store_true', 
                       help='Only clear existing data, do not import')
    parser.add_argument('--verify-only', action='store_true', 
                       help='Only verify existing data, do not import')
    parser.add_argument('--verbose', action='store_true', 
                       help='Enable verbose logging')
    
    args = parser.parse_args()
    
    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Display configuration
    print("📋 Cars Standard Data Import")
    print("-" * 40)
    
    if args.clear_only:
        print("🗑️ Mode: Clear existing data only")
    elif args.verify_only:
        print("🔍 Mode: Verify existing data only")
    else:
        print(f"📥 Mode: {args.mode.upper()}")
        print(f"📄 CSV File: {args.csv_path}")
    
    print()
    
    # Initialize config and display database settings
    config = DatabaseConfig()
    config.log_config()
    
    # Run import
    try:
        importer = CarsStandardImporter(config)
        
        if args.clear_only:
            # Only clear data
            cleared_count = importer.clear_existing_data()
            print(f"✅ Cleared {cleared_count} records from database")
            
        elif args.verify_only:
            # Only verify data
            verification = importer.verify_import()
            summary = {
                'cleared_count': 0,
                'imported_count': 0,
                'final_total': verification['total_count'],
                'top_brands': verification['top_brands'][:5],
                'sample_data': verification['sample_data']
            }
            display_summary(summary)
            
        elif args.mode == 'upsert':
            # Safe UPSERT mode (recommended)
            logger.info("🔄 Using UPSERT mode (safe for existing data)")
            result = importer.upsert_from_csv(args.csv_path)
            
            # Verify after upsert
            verification = importer.verify_import()
            
            # Display UPSERT summary
            print("\n" + "="*60)
            print("✅ CARS STANDARD UPSERT COMPLETED")
            print("="*60)
            
            print(f"📥 Inserted: {result['inserted']} new records")
            print(f"🔄 Updated: {result['updated']} existing records")
            if result['skipped'] > 0:
                print(f"⚠️ Skipped: {result['skipped']} invalid records")
            print(f"📊 Final Total: {verification['total_count']} records in database")
            
            print(f"\n🏷️ TOP BRANDS:")
            for brand in verification['top_brands'][:5]:
                print(f"   {brand['brand_norm']}: {brand['count']} models")
            
            print(f"\n🎉 UPSERT completed successfully!")
            
        else:
            # Clear and import mode (may fail with foreign keys)
            logger.warning("⚠️ Using CLEAR-IMPORT mode (may fail if foreign keys exist)")
            clear_first = True
            summary = importer.full_import(
                csv_file_path=args.csv_path,
                clear_first=clear_first
            )
            
            display_summary(summary)
        
    except KeyboardInterrupt:
        print("\n❌ Import interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Import failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()