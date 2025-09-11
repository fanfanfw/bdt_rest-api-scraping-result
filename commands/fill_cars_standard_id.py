#!/usr/bin/env python3
"""
Fill Cars Standard ID Script
============================

Script untuk mengisi field cars_standard_id yang masih NULL
di database cars_unified berdasarkan matching dengan cars_standard.

Usage:
    python fill_cars_standard_id.py
"""

import os
import django
import sys
from datetime import datetime
import logging
from tqdm import tqdm
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables
load_dotenv(override=True)

# Add project root to Python path
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

# Setup Django environment using environment variable
django_settings = os.getenv('DJANGO_SETTINGS_MODULE', 'carmarket.settings')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', django_settings)
django.setup()

from django.db import connection
from main.models import CarStandard, CarUnified

logger = logging.getLogger(__name__)


def find_cars_standard_id(brand, model_group, model, variant):
    """
    Mencari cars_standard_id berdasarkan brand, model_group, model, variant
    dengan logika matching yang sesuai dengan project ini
    """
    cars_standard_id = None
    
    if not brand or not model or not variant:
        return None
    
    try:
        # Step 1: Cari berdasarkan brand_norm (case insensitive)
        brand_matches = CarStandard.objects.filter(
            brand_norm__iexact=brand.strip()
        )
        
        for candidate in brand_matches:
            # Step 2: Cek model_group - prioritas model_group_norm dulu, lalu model_group_raw
            model_group_match = False
            if model_group and model_group.strip():  # Jika ada model_group dari input
                if candidate.model_group_norm and candidate.model_group_norm.strip().upper() == model_group.strip().upper():
                    model_group_match = True
                elif candidate.model_group_raw and candidate.model_group_raw.strip().upper() == model_group.strip().upper():
                    model_group_match = True
            else:  # Jika tidak ada model_group dari input, skip pengecekan model_group
                model_group_match = True
            
            if not model_group_match:
                continue
            
            # Step 3: Cek model - prioritas model_norm dulu, lalu model_raw
            model_match = False
            if candidate.model_norm and candidate.model_norm.strip().upper() == model.strip().upper():
                model_match = True
            elif candidate.model_raw and candidate.model_raw.strip().upper() == model.strip().upper():
                model_match = True
            
            if not model_match:
                continue
            
            # Step 4: Cek variant - prioritas variant_norm, lalu variant_raw, lalu variant_raw2
            variant_match = False
            if candidate.variant_norm and candidate.variant_norm.strip().upper() == variant.strip().upper():
                variant_match = True
            elif candidate.variant_raw and candidate.variant_raw.strip().upper() == variant.strip().upper():
                variant_match = True
            elif candidate.variant_raw2 and candidate.variant_raw2.strip().upper() == variant.strip().upper():
                variant_match = True
            
            if variant_match:
                cars_standard_id = candidate.id
                break  # Keluar dari loop jika sudah ditemukan match
                
    except Exception as e:
        logger.error(f"Error dalam pencarian cars_standard_id: {e}")
        return None
    
    return cars_standard_id


def fill_cars_standard_id_for_source(source):
    """
    Mengisi cars_standard_id yang NULL untuk source tertentu
    """
    updated_count = 0
    failed_count = 0
    failed_records = []
    
    try:
        logger.info(f"🔍 Mencari record dengan cars_standard_id NULL untuk source {source}...")
        
        # Ambil semua record yang cars_standard_id nya NULL
        null_records = CarUnified.objects.filter(
            source=source,
            cars_standard_id__isnull=True,
            brand__isnull=False,
            model__isnull=False,
            variant__isnull=False
        ).values('id', 'listing_url', 'brand', 'model_group', 'model', 'variant')
        
        logger.info(f"📊 Ditemukan {len(null_records)} record dengan cars_standard_id NULL untuk {source}")
        
        if len(null_records) == 0:
            logger.info(f"✅ Tidak ada record yang perlu diupdate untuk {source}")
            return updated_count, failed_count, failed_records
        
        # Progress bar untuk pemrosesan record
        with tqdm(total=len(null_records), desc=f"🔄 {source}", 
                  unit="record", ncols=100, colour='green') as pbar:
            for record in null_records:
                record_id = record['id']
                listing_url = record['listing_url']
                brand = record['brand']
                model_group = record['model_group']
                model = record['model']
                variant = record['variant']
                
                # Cari cars_standard_id
                cars_standard_id = find_cars_standard_id(brand, model_group, model, variant)
                
                if cars_standard_id:
                    # Update cars_standard_id
                    CarUnified.objects.filter(id=record_id).update(
                        cars_standard_id=cars_standard_id
                    )
                    updated_count += 1
                    pbar.set_postfix({"✅ Updated": updated_count, "❌ Failed": failed_count})
                else:
                    failed_count += 1
                    failed_records.append({
                        'id': record_id,
                        'listing_url': listing_url,
                        'brand': brand,
                        'model_group': model_group,
                        'model': model,
                        'variant': variant,
                        'source': source
                    })
                    pbar.set_postfix({"✅ Updated": updated_count, "❌ Failed": failed_count})
                
                pbar.update(1)
                
        logger.info(f"✅ {source}: Selesai memproses {len(null_records)} record")
        logger.info(f"   📈 Berhasil update: {updated_count}")
        logger.info(f"   ❌ Gagal match: {failed_count}")
        
        return updated_count, failed_count, failed_records
        
    except Exception as e:
        logger.error(f"❌ Error saat memproses {source}: {str(e)}")
        raise


def fill_all_cars_standard_id():
    """
    Mengisi cars_standard_id untuk semua source (carlistmy dan mudahmy)
    """
    print("📋 Fill Cars Standard ID")
    print("-" * 50)
    print(f"🔧 Django Settings: {os.getenv('DJANGO_SETTINGS_MODULE', 'carmarket.settings')}")
    print(f"🗄️ Database: {os.getenv('DB_NAME', 'default')}")
    print("")
    
    logger.info("🚀 Memulai proses pengisian cars_standard_id untuk record yang NULL...")
    
    start_time = datetime.now()
    total_updated = 0
    total_failed = 0
    all_failed_records = []
    
    try:
        # Check if cars_standard table has data
        cars_standard_count = CarStandard.objects.count()
        if cars_standard_count == 0:
            logger.error("❌ No cars_standard data found in database!")
            logger.error("💡 Please run: python import_cars_standard.py")
            return {
                'status': 'error',
                'error': 'Cars standard data not found. Run import_cars_standard.py first.',
                'total_updated': 0,
                'total_failed': 0
            }
        
        logger.info(f"📖 Found {cars_standard_count} cars_standard records")
        
        # Process CarlistMY
        logger.info("=" * 60)
        logger.info("📋 Memproses source CarlistMY...")
        updated_carlistmy, failed_carlistmy, failed_records_carlistmy = fill_cars_standard_id_for_source('carlistmy')
        total_updated += updated_carlistmy
        total_failed += failed_carlistmy
        all_failed_records.extend(failed_records_carlistmy)
        
        # Process MudahMY
        logger.info("=" * 60)
        logger.info("📋 Memproses source MudahMY...")
        updated_mudahmy, failed_mudahmy, failed_records_mudahmy = fill_cars_standard_id_for_source('mudahmy')
        total_updated += updated_mudahmy
        total_failed += failed_mudahmy
        all_failed_records.extend(failed_records_mudahmy)
        
        # Summary report
        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.info("=" * 60)
        logger.info("📊 SUMMARY REPORT")
        logger.info("=" * 60)
        logger.info(f"⏱️  Waktu eksekusi: {duration}")
        logger.info(f"📈 Total record berhasil diupdate: {total_updated}")
        logger.info(f"❌ Total record gagal match: {total_failed}")
        logger.info("")
        logger.info("📋 Detail per source:")
        logger.info(f"   CarlistMY - Updated: {updated_carlistmy}, Failed: {failed_carlistmy}")
        logger.info(f"   MudahMY   - Updated: {updated_mudahmy}, Failed: {failed_mudahmy}")
        
        # Simpan record yang gagal ke CSV untuk analisis
        failed_filename = None
        if all_failed_records:
            try:
                import pandas as pd
                failed_df = pd.DataFrame(all_failed_records)
                failed_filename = f"failed_cars_standard_id_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                failed_df.to_csv(failed_filename, index=False)
                logger.info(f"💾 Record yang gagal match disimpan di: {failed_filename}")
            except ImportError:
                logger.warning("⚠️  pandas tidak tersedia, tidak dapat menyimpan failed records ke CSV")
        
        logger.info("=" * 60)
        if total_updated > 0:
            logger.info("🎉 Proses pengisian cars_standard_id BERHASIL!")
        else:
            logger.info("ℹ️  Tidak ada record yang perlu diupdate")
        logger.info("=" * 60)
        
        return {
            'status': 'success',
            'total_updated': total_updated,
            'total_failed': total_failed,
            'duration': str(duration),
            'carlistmy': {'updated': updated_carlistmy, 'failed': failed_carlistmy},
            'mudahmy': {'updated': updated_mudahmy, 'failed': failed_mudahmy},
            'failed_records_file': failed_filename if all_failed_records else None
        }
        
    except Exception as e:
        logger.error(f"❌ Error dalam proses pengisian cars_standard_id: {str(e)}")
        return {
            'status': 'error',
            'error': str(e),
            'total_updated': total_updated,
            'total_failed': total_failed
        }


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s:%(name)s:%(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    try:
        result = fill_all_cars_standard_id()
        print("\n" + "=" * 60)
        print("HASIL AKHIR:")
        print("=" * 60)
        for key, value in result.items():
            print(f"{key}: {value}")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)