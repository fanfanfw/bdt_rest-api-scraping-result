"""
Fill Cars Standard ID Script
============================

Script untuk mengisi field cars_standard_id yang masih NULL
di database cars_unified berdasarkan matching dengan cars_standard.

Usage:
    python fill_cars_standard_id.py
"""

import os
import sys
import argparse
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

import psycopg2
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)

DEFAULT_TABLE_NAME = "cars_unified"
DEFAULT_SOURCES = ["carlistmy", "mudahmy"]


def validate_table_name(table_name):
    """Allow only simple SQL identifiers for dynamic table names."""
    if not table_name or not str(table_name).replace("_", "").isalnum():
        raise ValueError(f"Invalid table name: {table_name!r}")
    return table_name


def normalize_value(value):
    """Konversi string ke uppercase tanpa spasi ekstra; kosong -> None."""
    if value is None:
        return None
    cleaned = str(value).strip().upper()
    if cleaned in {"", "-", "N/A", "NULL"}:
        return None
    return cleaned


def candidate_matches(candidate, key, target):
    """Bandingkan kandidat cars_standard berdasarkan prioritas kolom untuk tiap field."""
    if target is None:
        return False

    priority_columns = {
        "brand": ["brand_norm", "brand_raw", "brand_raw2"],
        "model_group": ["model_group_norm", "model_group_raw"],
        "model": ["model_norm", "model_raw", "model_raw2"],
        "variant": ["variant_norm", "variant_raw", "variant_raw2", "variant_raw3", "variant_raw4"],
    }

    for column in priority_columns.get(key, []):
        value = candidate.get(column)
        if value and str(value).strip().upper() == target:
            return True
    return False


def get_table_columns(cur, table_name):
    """Return available columns for a public table."""
    cur.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = %s
    """, (table_name,))
    return {row["column_name"] for row in cur.fetchall()}


def find_cars_standard_id(cur, brand, model_group, model, variant):
    """
    Mencari cars_standard_id berdasarkan brand, model_group, model, variant
    dengan logika matching yang sesuai dengan project ini
    Menggunakan cursor yang sudah ada untuk menghindari multiple connections
    """
    cars_standard_id = None

    brand_norm = normalize_value(brand)
    model_group_norm = normalize_value(model_group)
    model_norm = normalize_value(model)
    variant_norm = normalize_value(variant)

    if brand_norm is None or model_norm is None or variant_norm is None:
        return None

    try:
        standard_columns = get_table_columns(cur, "cars_standard")
        candidate_columns = [
            "id",
            "brand_norm",
            "brand_raw",
            "brand_raw2",
            "model_group_norm",
            "model_norm",
            "variant_norm",
            "model_group_raw",
            "model_raw",
            "model_raw2",
            "variant_raw",
            "variant_raw2",
            "variant_raw3",
            "variant_raw4",
        ]
        select_columns = [
            column if column in standard_columns else f"NULL AS {column}"
            for column in candidate_columns
        ]
        brand_lookup_columns = [
            column
            for column in ("brand_norm", "brand_raw", "brand_raw2")
            if column in standard_columns
        ]
        if not brand_lookup_columns:
            logger.error("cars_standard has no brand lookup columns")
            return None
        brand_conditions = " OR ".join(
            f"UPPER(TRIM({column})) = %s"
            for column in brand_lookup_columns
        )

        # Step 1: Cari kandidat berdasarkan brand dengan fallback norm/raw/raw2
        cur.execute(f"""
            SELECT {', '.join(select_columns)}
            FROM cars_standard
            WHERE {brand_conditions}
        """, tuple(brand_norm for _ in brand_lookup_columns))

        brand_matches = cur.fetchall()

        # model_group opsional untuk cars_unified terbaru.
        # Jika None/"NO MODEL GROUP", jangan blokir matching.
        ignore_model_group = model_group_norm in {None, "NO MODEL GROUP"}

        for candidate in brand_matches:
            if not candidate_matches(candidate, "brand", brand_norm):
                continue

            # Step 2: Cek model_group hanya jika ada nilai valid
            if not ignore_model_group and not candidate_matches(candidate, "model_group", model_group_norm):
                continue

            if not candidate_matches(candidate, "model", model_norm):
                continue

            if not candidate_matches(candidate, "variant", variant_norm):
                continue

            cars_standard_id = candidate['id']
            break  # Keluar dari loop jika sudah ditemukan match

    except Exception as e:
        logger.error(f"Error dalam pencarian cars_standard_id: {e}")
        return None

    return cars_standard_id


def fill_cars_standard_id_for_source(source, batch_size=500, table_name=DEFAULT_TABLE_NAME):
    """
    Mengisi cars_standard_id yang NULL untuk source tertentu
    dengan batch commit untuk menyimpan progress secara berkala
    Optimized version: menggunakan satu koneksi dan cursor yang persistent
    """
    updated_count = 0
    failed_count = 0
    failed_records = []
    batch_count = 0

    table_name = validate_table_name(table_name)

    # Database configuration
    db_config = {
        'host': os.getenv('DB_HOST', '127.0.0.1'),
        'port': int(os.getenv('DB_PORT', 5432)),
        'database': os.getenv('DB_NAME', 'db_test'),
        'user': os.getenv('DB_USER', 'fanfan'),
        'password': os.getenv('DB_PASSWORD', 'cenanun')
    }

    conn = None
    try:
        logger.info(f"🔍 Mencari record dengan cars_standard_id NULL untuk source {source} di {table_name}...")

        conn = psycopg2.connect(**db_config)
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Set connection settings untuk optimasi
        conn.autocommit = False  # Explicit transaction control
        cur.execute("SET work_mem = '256MB'")  # Increase working memory for sorting

        # Create index untuk mempercepat pencarian di cars_standard jika belum ada
        try:
            cur.execute("""
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_cars_standard_brand_norm_upper
                ON cars_standard (UPPER(brand_norm))
            """)
            conn.commit()
        except Exception as e:
            logger.debug(f"Index mungkin sudah ada: {e}")
            conn.rollback()

        for index_name, column_name in (
            ("idx_cars_standard_brand_raw_upper", "brand_raw"),
            ("idx_cars_standard_brand_raw2_upper", "brand_raw2"),
        ):
            try:
                cur.execute(f"""
                    CREATE INDEX CONCURRENTLY IF NOT EXISTS {index_name}
                    ON cars_standard (UPPER({column_name}))
                """)
                conn.commit()
            except Exception as e:
                logger.debug(f"Index mungkin sudah ada: {e}")
                conn.rollback()

        # Ambil semua record yang cars_standard_id nya NULL dengan LIMIT untuk processing batch
        cur.execute(f"""
            SELECT id, listing_url, brand, model, variant
            FROM {table_name}
            WHERE source = %s
            AND cars_standard_id IS NULL
            AND brand IS NOT NULL
            AND model IS NOT NULL
            AND variant IS NOT NULL
            ORDER BY id
        """, (source,))

        null_records = cur.fetchall()

        logger.info(f"📊 Ditemukan {len(null_records)} record dengan cars_standard_id NULL untuk {source}")
        logger.info(f"🔄 Batch size: {batch_size} (data akan di-commit setiap {batch_size} record)")

        if len(null_records) == 0:
            logger.info(f"✅ Tidak ada record yang perlu diupdate untuk {source}")
            return updated_count, failed_count, failed_records

        # Prepare update statement untuk optimasi
        update_stmt = f"""
            UPDATE {table_name}
            SET cars_standard_id = %s
            WHERE id = %s
        """

        # Progress bar untuk pemrosesan record
        with tqdm(total=len(null_records), desc=f"🔄 {source}",
                  unit="record", ncols=100, colour='green') as pbar:

            update_batch = []  # Batch updates

            for record in null_records:
                record_id = record['id']
                listing_url = record['listing_url']
                brand = record['brand']
                model_group = None
                model = record['model']
                variant = record['variant']

                # Cari cars_standard_id menggunakan cursor yang sudah ada
                cars_standard_id = find_cars_standard_id(cur, brand, model_group, model, variant)

                if cars_standard_id:
                    # Tambahkan ke batch update
                    update_batch.append((cars_standard_id, record_id))
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

                batch_count += 1
                pbar.update(1)

                # Execute batch updates dan commit setiap batch_size record
                if batch_count >= batch_size:
                    if update_batch:
                        cur.executemany(update_stmt, update_batch)
                        update_batch = []
                    conn.commit()
                    logger.debug(f"💾 Batch commit: {batch_count} record telah diproses dan disimpan")
                    batch_count = 0

        # Execute sisa batch updates dan commit
        if update_batch:
            cur.executemany(update_stmt, update_batch)
        if batch_count > 0:
            conn.commit()
            logger.debug(f"💾 Final commit: {batch_count} record terakhir telah disimpan")

        logger.info(f"✅ {source}: Selesai memproses {len(null_records)} record")
        logger.info(f"   📈 Berhasil update: {updated_count}")
        logger.info(f"   ❌ Gagal match: {failed_count}")

        return updated_count, failed_count, failed_records

    except Exception as e:
        logger.error(f"❌ Error saat memproses {source}: {str(e)}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()


def fill_all_cars_standard_id(sources=None, table_name=DEFAULT_TABLE_NAME, batch_size=500):
    """
    Mengisi cars_standard_id untuk semua source yang diberikan.
    Default tetap source Malaysia pada tabel cars_unified.
    """
    table_name = validate_table_name(table_name)
    sources = list(sources or DEFAULT_SOURCES)

    print("📋 Fill Cars Standard ID")
    print("-" * 50)
    print(f"🗄️ Database: {os.getenv('DB_NAME', 'db_test')}")
    print(f"🔧 Host: {os.getenv('DB_HOST', '127.0.0.1')}:{os.getenv('DB_PORT', 5432)}")
    print(f"📦 Table: {table_name}")
    print(f"📌 Sources: {', '.join(sources)}")
    print("")
    
    logger.info("🚀 Memulai proses pengisian cars_standard_id untuk record yang NULL...")
    
    start_time = datetime.now()
    total_updated = 0
    total_failed = 0
    all_failed_records = []
    
    try:
        # Check if cars_standard table has data using direct query
        db_config = {
            'host': os.getenv('DB_HOST', '127.0.0.1'),
            'port': int(os.getenv('DB_PORT', 5432)),
            'database': os.getenv('DB_NAME', 'db_test'),
            'user': os.getenv('DB_USER', 'fanfan'),
            'password': os.getenv('DB_PASSWORD', 'cenanun')
        }
        
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM cars_standard")
        cars_standard_count = cur.fetchone()[0]
        conn.close()
        
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
        
        source_results = {}
        for source in sources:
            logger.info("=" * 60)
            logger.info(f"📋 Memproses source {source}...")
            updated, failed, failed_records = fill_cars_standard_id_for_source(
                source,
                batch_size=batch_size,
                table_name=table_name,
            )
            total_updated += updated
            total_failed += failed
            all_failed_records.extend(failed_records)
            source_results[source] = {'updated': updated, 'failed': failed}
        
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
        for source, stats in source_results.items():
            logger.info(f"   {source} - Updated: {stats['updated']}, Failed: {stats['failed']}")
        
        # Simpan record yang gagal ke CSV untuk analisis
        failed_filename = None
        if all_failed_records:
            try:
                import pandas as pd
                failed_df = pd.DataFrame(all_failed_records)
                failed_filename = f"failed_cars_standard_id_{table_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
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
            'table_name': table_name,
            'sources': source_results,
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
    
    parser = argparse.ArgumentParser(description="Fill cars_standard_id for unified car tables")
    parser.add_argument("--table", default=DEFAULT_TABLE_NAME, help="Target table name, default: cars_unified")
    parser.add_argument(
        "--sources",
        default=",".join(DEFAULT_SOURCES),
        help="Comma-separated sources, default: carlistmy,mudahmy",
    )
    parser.add_argument("--batch-size", type=int, default=500, help="Commit batch size")
    args = parser.parse_args()

    try:
        selected_sources = [source.strip() for source in args.sources.split(",") if source.strip()]
        result = fill_all_cars_standard_id(
            sources=selected_sources,
            table_name=args.table,
            batch_size=args.batch_size,
        )
        print("\n" + "=" * 60)
        print("HASIL AKHIR:")
        print("=" * 60)
        for key, value in result.items():
            print(f"{key}: {value}")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)
