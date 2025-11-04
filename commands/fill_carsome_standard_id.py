#!/usr/bin/env python3
"""
Fill Carsome Standard ID Script
==============================

Script untuk memetakan record pada tabel carsome ke master cars_standard
dan mengisi kolom cars_standard_id yang masih NULL.

Usage:
    python fill_carsome_standard_id.py
"""

import os
import sys
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple, Dict, Any, List

from dotenv import load_dotenv
from tqdm import tqdm
import psycopg2
from psycopg2.extras import RealDictCursor

# Pastikan environment variable termuat
load_dotenv(override=True)

# Tambahkan project root ke PYTHONPATH (mirroring fill_cars_standard_id.py)
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

logger = logging.getLogger(__name__)


def normalize_value(value: Optional[str]) -> Optional[str]:
    """Konversi nilai string ke uppercase tanpa spasi ekstra; kosong -> None."""
    if value is None:
        return None
    cleaned = value.strip().upper()
    if cleaned in {"", "-", "N/A", "NULL"}:
        return None
    return cleaned


def candidate_matches(candidate: Dict[str, Any], key: str, target: Optional[str]) -> bool:
    """
    Utility untuk membandingkan value kandidat dari tabel cars_standard dengan target.
    Mencoba field norm kemudian raw sesuai urutan prioritas.
    """
    if target is None:
        return False

    # Mapping prioritas kolom berdasarkan key
    priority_columns = {
        "model_group": ["model_group_norm", "model_group_raw"],
        "model": ["model_norm", "model_raw", "model_raw2"],
        "variant": ["variant_norm", "variant_raw", "variant_raw2"],
    }

    for column in priority_columns.get(key, []):
        value = candidate.get(column)
        if value and value.strip().upper() == target:
            return True
    return False


def find_cars_standard_id(
    cursor: RealDictCursor,
    brand: Optional[str],
    model_group: Optional[str],
    model: Optional[str],
    variant: Optional[str],
) -> Optional[int]:
    """
    Cari id dari cars_standard yang cocok dengan kombinasi brand/model_group/model/variant
    milik data Carsome.
    """
    brand_norm = normalize_value(brand)
    model_group_norm = normalize_value(model_group)
    model_norm = normalize_value(model)
    variant_norm = normalize_value(variant)

    # Pastikan field minimal terpenuhi (model_group bisa None/NO MODEL GROUP)
    if brand_norm is None or model_norm is None or variant_norm is None:
        return None

    try:
        cursor.execute(
            """
            SELECT id, brand_norm, model_group_norm, model_group_raw,
                   model_norm, model_raw, model_raw2,
                   variant_norm, variant_raw, variant_raw2
            FROM cars_standard
            WHERE UPPER(brand_norm) = %s
            """,
            (brand_norm,),
        )
        candidates = cursor.fetchall()
    except Exception as exc:
        logger.error("❌ Gagal mengambil kandidat cars_standard untuk brand %s: %s", brand_norm, exc)
        return None

    # Gunakan placeholder khusus yang berarti "model group tidak ada" agar tidak memblokir pencarian
    ignore_model_group = model_group_norm in {None, "NO MODEL GROUP"}

    for candidate in candidates:
        # Brand sudah dipastikan cocok lewat query; cek model_group, model, variant
        if not ignore_model_group and not candidate_matches(candidate, "model_group", model_group_norm):
            continue

        if not candidate_matches(candidate, "model", model_norm):
            continue

        if not candidate_matches(candidate, "variant", variant_norm):
            continue

        return candidate["id"]

    return None


def map_carsome_cars_standard(batch_size: int = 500) -> Dict[str, Any]:
    """Isi kolom cars_standard_id untuk record carsome."""
    db_config = {
        "host": os.getenv("DB_HOST", "127.0.0.1"),
        "port": int(os.getenv("DB_PORT", 5432)),
        "database": os.getenv("DB_NAME", "db_test"),
        "user": os.getenv("DB_USER", "fanfan"),
        "password": os.getenv("DB_PASSWORD", "cenanun"),
    }

    conn = None
    updated_count = 0
    failed_count = 0
    failed_records: List[Dict[str, Any]] = []

    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        conn.autocommit = False

        logger.info("🔍 Mengambil data Carsome yang belum memiliki cars_standard_id ...")
        cursor.execute(
            """
            SELECT id, brand, model_group, model, variant
            FROM carsome
            WHERE cars_standard_id IS NULL
              AND brand IS NOT NULL
              AND model IS NOT NULL
              AND variant IS NOT NULL
            ORDER BY id
            """
        )
        rows = cursor.fetchall()

        total_records = len(rows)
        if total_records == 0:
            logger.info("✅ Tidak ada record Carsome yang perlu dimapping.")
            return {
                "status": "success",
                "total_processed": 0,
                "updated": 0,
                "failed": 0,
                "failed_records_file": None,
            }

        logger.info("📊 Total record tanpa cars_standard_id: %s", total_records)

        update_stmt = """
            UPDATE carsome
            SET cars_standard_id = %s
            WHERE id = %s
        """

        batch: List[Tuple[int, int]] = []

        with tqdm(total=total_records, desc="🔄 Carsome", unit="record", ncols=100, colour="blue") as progress:
            for row in rows:
                cars_standard_id = find_cars_standard_id(
                    cursor,
                    row.get("brand"),
                    row.get("model_group"),
                    row.get("model"),
                    row.get("variant"),
                )

                if cars_standard_id:
                    batch.append((cars_standard_id, row["id"]))
                    updated_count += 1
                else:
                    failed_count += 1
                    failed_records.append(
                        {
                            "id": row["id"],
                            "brand": row.get("brand"),
                            "model_group": row.get("model_group"),
                            "model": row.get("model"),
                            "variant": row.get("variant"),
                        }
                    )

                if len(batch) >= batch_size:
                    cursor.executemany(update_stmt, batch)
                    conn.commit()
                    batch.clear()

                progress.set_postfix({"✅ Updated": updated_count, "❌ Failed": failed_count})
                progress.update(1)

        # Commit sisa batch
        if batch:
            cursor.executemany(update_stmt, batch)
            conn.commit()

        failed_filename = None
        if failed_records:
            try:
                import pandas as pd

                failed_df = pd.DataFrame(failed_records)
                failed_filename = f"failed_carsome_standard_id_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                failed_df.to_csv(failed_filename, index=False)
                logger.info("💾 Detail record gagal disimpan di %s", failed_filename)
            except ImportError:
                logger.warning("⚠️ pandas tidak tersedia; daftar gagal tidak disimpan ke CSV.")

        logger.info("=" * 60)
        logger.info("📊 RINGKASAN")
        logger.info("Total diproses : %s", total_records)
        logger.info("Berhasil update: %s", updated_count)
        logger.info("Gagal match   : %s", failed_count)
        logger.info("=" * 60)

        return {
            "status": "success",
            "total_processed": total_records,
            "updated": updated_count,
            "failed": failed_count,
            "failed_records_file": failed_filename,
        }

    except Exception as exc:
        if conn:
            conn.rollback()
        logger.error("❌ Terjadi error saat mapping Carsome: %s", exc)
        return {"status": "error", "error": str(exc)}
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    try:
        result = map_carsome_cars_standard()
        print("\n" + "=" * 60)
        print("HASIL AKHIR:")
        print("=" * 60)
        for key, value in result.items():
            print(f"{key}: {value}")
    except Exception as err:
        print(f"\n❌ Error: {err}")
        sys.exit(1)
