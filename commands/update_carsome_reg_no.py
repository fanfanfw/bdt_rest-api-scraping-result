#!/usr/bin/env python3
"""
Tambahkan kolom reg_no pada tabel carsome dan isi nilainya
berdasarkan mapping image→reg_no dari CSV.

Usage:
    python commands/update_carsome_reg_no.py --csv scrapes_reg_no.csv
"""

import argparse
import csv
import logging
import os
from pathlib import Path
from typing import List, Tuple

from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values

# Muat variabel lingkungan (.env) bila tersedia
load_dotenv(override=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def get_db_config() -> dict:
    """Ambil konfigurasi koneksi database dari environment."""
    return {
        "host": os.getenv("DB_HOST", "127.0.0.1"),
        "port": int(os.getenv("DB_PORT", 5432)),
        "database": os.getenv("DB_NAME", "db_test"),
        "user": os.getenv("DB_USER", "fanfan"),
        "password": os.getenv("DB_PASSWORD", "cenanun"),
    }


def read_mapping(csv_path: Path) -> List[Tuple[int, str, str]]:
    """Baca CSV dan kembalikan list tuple (row_num, reg_no, image)."""
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV tidak ditemukan: {csv_path}")

    rows: List[Tuple[int, str, str]] = []
    with csv_path.open("r", encoding="utf-8", newline="") as fp:
        reader = csv.DictReader(fp)
        required_columns = {"reg_no", "image"}
        if not required_columns.issubset(reader.fieldnames or []):
            raise ValueError(f"CSV harus memiliki kolom {required_columns}")

        for idx, record in enumerate(reader, start=1):
            reg_no = (record.get("reg_no") or "").strip()
            image = (record.get("image") or "").strip()
            if not reg_no or not image:
                continue
            rows.append((idx, reg_no, image))

    if not rows:
        raise ValueError("Tidak ada baris valid di CSV (reg_no/image kosong).")

    logger.info("📄 CSV dimuat: %s baris valid", len(rows))
    return rows


def ensure_reg_no_column(cursor) -> None:
    """Tambah kolom reg_no jika belum ada."""
    logger.info("🔧 Memastikan kolom reg_no tersedia di tabel carsome...")
    cursor.execute(
        "ALTER TABLE IF EXISTS carsome "
        "ADD COLUMN IF NOT EXISTS reg_no TEXT"
    )


def prepare_temp_tables(cursor) -> None:
    """Buat tabel sementara untuk menampung CSV dan hasil deduplikasi."""
    cursor.execute("DROP TABLE IF EXISTS tmp_carsome_reg_no_raw")
    cursor.execute(
        """
        CREATE TEMP TABLE tmp_carsome_reg_no_raw (
            row_num INTEGER,
            reg_no TEXT,
            image TEXT
        ) ON COMMIT DROP
        """
    )

    cursor.execute("DROP TABLE IF EXISTS tmp_carsome_reg_no_unique")
    cursor.execute(
        """
        CREATE TEMP TABLE tmp_carsome_reg_no_unique (
            image TEXT PRIMARY KEY,
            reg_no TEXT
        ) ON COMMIT DROP
        """
    )


def load_raw_data(cursor, rows: List[Tuple[int, str, str]]) -> None:
    """Masukkan data CSV ke tabel sementara raw."""
    logger.info("⬆️  Memasukkan data CSV ke tabel sementara...")
    execute_values(
        cursor,
        "INSERT INTO tmp_carsome_reg_no_raw (row_num, reg_no, image) VALUES %s",
        rows,
        page_size=1000,
    )


def deduplicate(cursor) -> Tuple[int, int]:
    """Deduplikasi berdasarkan image sambil mempertahankan urutan CSV."""
    cursor.execute(
        """
        INSERT INTO tmp_carsome_reg_no_unique (image, reg_no)
        SELECT DISTINCT ON (image) image, reg_no
        FROM tmp_carsome_reg_no_raw
        WHERE image IS NOT NULL AND image <> ''
        ORDER BY image, row_num
        """
    )

    cursor.execute("SELECT COUNT(*) FROM tmp_carsome_reg_no_raw")
    total_raw = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM tmp_carsome_reg_no_unique")
    unique_rows = cursor.fetchone()[0]

    duplicates = total_raw - unique_rows
    if duplicates > 0:
        logger.warning("⚠️  Ditemukan %s image duplikat, memakai entry pertama per image.", duplicates)
    else:
        logger.info("✅ Tidak ada duplikasi image di CSV.")

    return unique_rows, duplicates


def update_carsome(cursor) -> int:
    """Update tabel carsome dengan reg_no hasil mapping."""
    logger.info("📝 Mengupdate tabel carsome berdasarkan image...")
    cursor.execute(
        """
        UPDATE carsome AS c
        SET reg_no = u.reg_no
        FROM tmp_carsome_reg_no_unique AS u
        WHERE c.image = u.image
        """
    )
    updated_rows = cursor.rowcount
    logger.info("✅ Berhasil mengisi reg_no untuk %s record.", updated_rows)
    return updated_rows


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Tambahkan kolom reg_no pada tabel carsome dan isi nilainya dari CSV."
    )
    parser.add_argument(
        "--csv",
        default="scrapes_reg_no.csv",
        help="Path CSV yang berisi kolom reg_no,image (default: scrapes_reg_no.csv)",
    )
    args = parser.parse_args()

    csv_path = Path(args.csv)
    rows = read_mapping(csv_path)

    conn = psycopg2.connect(**get_db_config())

    try:
        with conn:
            with conn.cursor() as cursor:
                ensure_reg_no_column(cursor)
                prepare_temp_tables(cursor)
                load_raw_data(cursor, rows)
                unique_rows, duplicates = deduplicate(cursor)
                updated = update_carsome(cursor)

                logger.info(
                    "📊 Ringkasan: %s baris unik (duplikat %s), %s record carsome terupdate.",
                    unique_rows,
                    duplicates,
                    updated,
                )
    except Exception:
        logger.exception("❌ Gagal memperbarui reg_no.")
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
