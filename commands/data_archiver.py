#!/usr/bin/env python3
"""
Data Archiver for DB_TEST
==========================

Script untuk mengarsipkan data lama dari tabel cars_unified dan price_history_unified
di database db_test berdasarkan information_ads_date.

Usage:
    python data_archiver.py
"""

import os
import sys
import argparse
import psycopg2
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv(override=True)

# Add project root to Python path
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

class DataArchiver:
    def __init__(self):
        self.conn = None
        self.cursor = None
        self.setup_logging()

    def setup_logging(self):
        # Ensure logs directory exists
        logs_dir = Path("logs")
        logs_dir.mkdir(exist_ok=True)

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                logging.FileHandler(f"logs/data_archiver_{datetime.now().strftime('%Y%m%d')}.log"),
                logging.StreamHandler()
            ]
        )

    def get_connection(self):
        try:
            self.conn = psycopg2.connect(
                dbname=os.getenv("DB_NAME", "db_test"),
                user=os.getenv("DB_USER", "fanfan"),
                password=os.getenv("DB_PASSWORD", "cenanun"),
                host=os.getenv("DB_HOST", "127.0.0.1"),
                port=os.getenv("DB_PORT", 5432)
            )
            self.cursor = self.conn.cursor()
            logging.info("✅ Koneksi ke database berhasil")
        except Exception as e:
            logging.error(f"❌ Error koneksi ke database: {e}")
            raise e

    def close_connection(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    def create_archive_tables(self):
        """Membuat tabel arsip untuk cars_unified dan price_history_unified"""
        archive_tables = {
            'cars_unified': 'cars_unified_archive',
            'price_history_unified': 'price_history_unified_archive'
        }

        for original_table, archive_table in archive_tables.items():
            try:
                # Membuat tabel arsip dengan struktur yang sama
                self.cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {archive_table}
                    (LIKE {original_table} INCLUDING ALL)
                """)

                # Menambahkan kolom archived_at jika belum ada
                self.cursor.execute(f"""
                    ALTER TABLE {archive_table}
                    ADD COLUMN IF NOT EXISTS archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                """)

                self.conn.commit()
                logging.info(f"✅ Tabel arsip {archive_table} berhasil dibuat/diupdate")

            except Exception as e:
                self.conn.rollback()
                logging.error(f"❌ Error membuat tabel arsip {archive_table}: {e}")

    def get_old_car_records(self, months=6):
        """Mengambil records mobil yang information_ads_date > X bulan"""
        cutoff_date = datetime.now() - timedelta(days=months * 30)

        self.cursor.execute("""
            SELECT * FROM cars_unified
            WHERE information_ads_date < %s
        """, (cutoff_date,))

        return self.cursor.fetchall()

    def get_table_columns(self, table_name):
        """Mengambil nama kolom dari tabel"""
        self.cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position
        """, (table_name,))

        return [row[0] for row in self.cursor.fetchall()]

    def archive_cars_data(self, months=6):
        """Archive data mobil dari cars_unified"""
        try:
            # Ambil data lama yang akan diarsipkan
            old_records = self.get_old_car_records(months)

            if not old_records:
                logging.info(f"ℹ️  Tidak ada data lama di tabel cars_unified")
                return []

            # Ambil listing_urls untuk archiving price history
            original_columns = self.get_table_columns('cars_unified')
            listing_url_index = original_columns.index('listing_url')
            listing_urls = [record[listing_url_index] for record in old_records]

            # Insert ke archive menggunakan INSERT...SELECT
            columns_str = ', '.join(original_columns)
            cutoff_date = datetime.now() - timedelta(days=months * 30)

            self.cursor.execute(f"""
                INSERT INTO cars_unified_archive ({columns_str})
                SELECT * FROM cars_unified
                WHERE information_ads_date < %s
            """, (cutoff_date,))

            inserted_count = self.cursor.rowcount

            # Hapus dari tabel asli
            self.cursor.execute("""
                DELETE FROM cars_unified
                WHERE information_ads_date < %s
            """, (cutoff_date,))

            deleted_count = self.cursor.rowcount

            self.conn.commit()
            logging.info(f"✅ {inserted_count} record berhasil diarsipkan dari cars_unified")
            logging.info(f"   - Inserted: {inserted_count}, Deleted: {deleted_count}")

            return listing_urls  # Return listing_urls for price history archiving

        except Exception as e:
            self.conn.rollback()
            logging.error(f"❌ Error archiving cars_unified: {e}")
            return []

    def archive_price_history_data(self, listing_urls):
        """Archive data price history berdasarkan listing_urls yang sudah diarsipkan"""
        if not listing_urls:
            return

        try:
            # Cek dulu apakah ada data yang perlu diarsipkan
            urls_str = ', '.join(['%s'] * len(listing_urls))
            self.cursor.execute(f"""
                SELECT COUNT(*) FROM price_history_unified
                WHERE listing_url IN ({urls_str})
            """, listing_urls)

            price_count = self.cursor.fetchone()[0]

            if price_count == 0:
                logging.info(f"ℹ️  Tidak ada data price history untuk listing_urls yang diarsipkan")
                return

            # Insert ke tabel arsip menggunakan INSERT...SELECT
            self.cursor.execute(f"""
                INSERT INTO price_history_unified_archive
                SELECT ph.*, CURRENT_TIMESTAMP as archived_at
                FROM price_history_unified ph
                WHERE ph.listing_url IN ({urls_str})
            """, listing_urls)

            inserted_count = self.cursor.rowcount

            # Hapus dari tabel asli
            self.cursor.execute(f"""
                DELETE FROM price_history_unified
                WHERE listing_url IN ({urls_str})
            """, listing_urls)

            deleted_count = self.cursor.rowcount

            self.conn.commit()
            logging.info(f"✅ {inserted_count} price history record berhasil diarsipkan")
            logging.info(f"   - Inserted: {inserted_count}, Deleted: {deleted_count}")

        except Exception as e:
            self.conn.rollback()
            logging.error(f"❌ Error archiving price history: {e}")

    def run_archive_process(self, months=6):
        """Menjalankan proses archiving lengkap"""
        try:
            self.get_connection()

            logging.info(f"🚀 Memulai proses archiving data yang lebih lama dari {months} bulan...")
            logging.info(f"🗄️  Database: {os.getenv('DB_NAME', 'db_test')}")
            logging.info(f"🔧 Host: {os.getenv('DB_HOST', '127.0.0.1')}:{os.getenv('DB_PORT', 5432)}")

            # Buat tabel arsip
            self.create_archive_tables()

            # PERTAMA: Archive price history dulu sebelum cars (untuk data yang akan diarsip)
            logging.info("📋 Archiving price history unified terlebih dahulu...")
            cutoff_date = datetime.now() - timedelta(days=months * 30)

            self.cursor.execute("""
                INSERT INTO price_history_unified_archive
                SELECT ph.*, CURRENT_TIMESTAMP as archived_at
                FROM price_history_unified ph
                WHERE EXISTS (
                    SELECT 1 FROM cars_unified c
                    WHERE c.listing_url = ph.listing_url
                    AND c.information_ads_date < %s
                )
            """, (cutoff_date,))

            price_inserted = self.cursor.rowcount

            if price_inserted > 0:
                # Delete price history yang sudah diarsip
                self.cursor.execute("""
                    DELETE FROM price_history_unified ph
                    WHERE EXISTS (
                        SELECT 1 FROM cars_unified c
                        WHERE c.listing_url = ph.listing_url
                        AND c.information_ads_date < %s
                    )
                """, (cutoff_date,))
                price_deleted = self.cursor.rowcount
                logging.info(f"✅ {price_inserted} price history record diarsipkan (inserted: {price_inserted}, deleted: {price_deleted})")
            else:
                logging.info("ℹ️  Tidak ada price history yang perlu diarsipkan")

            # KEDUA: Archive cars data
            logging.info("📦 Archiving cars unified...")
            self.archive_cars_data(months)

            logging.info("✅ Proses archiving selesai!")

        except Exception as e:
            logging.error(f"❌ Error dalam proses archiving: {e}")
        finally:
            self.close_connection()

    def dry_run_archive(self, months=6):
        """Simulasi archiving tanpa benar-benar memindahkan data"""
        try:
            self.get_connection()

            logging.info(f"🔍 Simulasi archiving data yang lebih lama dari {months} bulan...")
            logging.info(f"🗄️  Database: {os.getenv('DB_NAME', 'db_test')}")

            # Hitung jumlah mobil yang akan diarsipkan
            old_records = self.get_old_car_records(months)
            cars_count = len(old_records)

            total_price_history_to_archive = 0

            if cars_count > 0:
                # Ambil kolom untuk mendapat listing_url
                original_columns = self.get_table_columns('cars_unified')
                listing_url_index = original_columns.index('listing_url')
                listing_urls = [record[listing_url_index] for record in old_records]

                # Hitung jumlah price history yang akan diarsipkan
                urls_str = ', '.join(['%s'] * len(listing_urls))
                self.cursor.execute(f"""
                    SELECT COUNT(*) FROM price_history_unified
                    WHERE listing_url IN ({urls_str})
                """, listing_urls)
                price_count = self.cursor.fetchone()[0]

                logging.info(f"  cars_unified: {cars_count} records akan diarsipkan")
                logging.info(f"  price_history_unified: {price_count} records akan diarsipkan")

                total_price_history_to_archive = price_count
            else:
                logging.info(f"  cars_unified: Tidak ada data yang perlu diarsipkan")

            logging.info(f"\n📊 Total yang akan diarsipkan:")
            logging.info(f"  Total mobil: {cars_count} records")
            logging.info(f"  Total price history: {total_price_history_to_archive} records")

        except Exception as e:
            logging.error(f"❌ Error dalam dry run: {e}")
        finally:
            self.close_connection()

    def get_archive_statistics(self):
        """Menampilkan statistik data arsip"""
        try:
            self.get_connection()

            tables = [
                'cars_unified_archive',
                'price_history_unified_archive'
            ]

            logging.info("📊 Statistik data arsip:")

            for table in tables:
                try:
                    self.cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count = self.cursor.fetchone()[0]

                    # Ambil tanggal arsip terbaru dan terlama
                    self.cursor.execute(f"""
                        SELECT MIN(archived_at), MAX(archived_at)
                        FROM {table}
                        WHERE archived_at IS NOT NULL
                    """)
                    date_range = self.cursor.fetchone()

                    logging.info(f"  {table}: {count} records")
                    if date_range[0]:
                        logging.info(f"    Periode arsip: {date_range[0]} - {date_range[1]}")

                except Exception as e:
                    logging.warning(f"  {table}: Tabel belum ada atau error - {e}")

        except Exception as e:
            logging.error(f"❌ Error mendapatkan statistik: {e}")
        finally:
            self.close_connection()

    def get_current_statistics(self):
        """Menampilkan statistik data saat ini"""
        try:
            self.get_connection()

            tables = [
                'cars_unified',
                'price_history_unified'
            ]

            logging.info("📊 Statistik data saat ini:")

            for table in tables:
                try:
                    self.cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count = self.cursor.fetchone()[0]
                    logging.info(f"  {table}: {count} records")

                except Exception as e:
                    logging.warning(f"  {table}: Error - {e}")

        except Exception as e:
            logging.error(f"❌ Error mendapatkan statistik: {e}")
        finally:
            self.close_connection()

def main():
    parser = argparse.ArgumentParser(description='Data Archiver for DB_TEST')
    parser.add_argument('--months', type=int, default=6, help='Jumlah bulan data untuk diarsipkan (default: 6)')
    parser.add_argument('--auto', action='store_true', help='Jalankan otomatis tanpa interaksi user')
    parser.add_argument('--dry-run', action='store_true', help='Hanya simulasi tanpa benar-benar mengarsipkan')
    parser.add_argument('--stats-only', action='store_true', help='Tampilkan statistik saja')

    args = parser.parse_args()

    archiver = DataArchiver()

    # Tampilkan statistik
    logging.info("📊 Statistik sebelum archiving:")
    archiver.get_current_statistics()
    archiver.get_archive_statistics()

    if args.stats_only:
        return

    # Dry run untuk melihat berapa yang akan diarsip
    logging.info("\n" + "="*60)
    archiver.dry_run_archive(months=args.months)

    if args.dry_run:
        return

    # Proses archiving
    if args.auto:
        logging.info("\n" + "="*60)
        logging.info("🚀 Mode otomatis - menjalankan proses archiving...")
        archiver.run_archive_process(months=args.months)

        # Tampilkan statistik setelah archiving
        logging.info("\n" + "="*60)
        logging.info("📊 Statistik setelah archiving:")
        archiver.get_current_statistics()
        archiver.get_archive_statistics()
    else:
        # Konfirmasi dari user
        logging.info("\n" + "="*60)
        proceed = input("Apakah Anda ingin melanjutkan proses archiving? (y/N): ").lower().strip()

        if proceed in ['y', 'yes']:
            logging.info("\n" + "="*60)
            archiver.run_archive_process(months=args.months)

            # Tampilkan statistik setelah archiving
            logging.info("\n" + "="*60)
            logging.info("📊 Statistik setelah archiving:")
            archiver.get_current_statistics()
            archiver.get_archive_statistics()
        else:
            logging.info("❌ Proses archiving dibatalkan")

if __name__ == "__main__":
    main()