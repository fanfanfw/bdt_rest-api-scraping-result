#!/usr/bin/env python3
"""
Fill Normalize Predict MudahMY ID Script
=======================================

Script untuk mengisi field normalize_predict_mudahmy_id pada cars_unified
berdasarkan matching brand/model/variant ke tabel normalize_predict_mudahmy.

Usage:
    python commands/fill_normalize_predict_mudahmy_id.py
"""

import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor
from tqdm import tqdm

load_dotenv(override=True)

project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

logger = logging.getLogger(__name__)

TB_UNIFIED = "cars_unified"
TB_NORMALIZE_PREDICT_MUDAHMY = "normalize_predict_mudahmy"
TARGET_COLUMN = "normalize_predict_mudahmy_id"
SOURCE = "mudahmy"


def normalize_value(value: Optional[str]) -> Optional[str]:
    """Konversi string ke uppercase tanpa spasi ekstra; kosong -> None."""
    if value is None:
        return None
    cleaned = str(value).strip().upper()
    if cleaned in {"", "-", "N/A", "NULL"}:
        return None
    return cleaned


def candidate_matches(candidate: Dict[str, Any], key: str, target: Optional[str]) -> bool:
    """Bandingkan kandidat berdasarkan prioritas kolom normalized lalu raw."""
    if target is None:
        return False

    priority_columns = {
        "brand": ["brand_norm", "brand_raw", "brand_raw2"],
        "model": ["model_norm", "model_raw", "model_raw2", "model_raw3"],
        "variant": ["variant_norm", "variant_raw", "variant_raw2", "variant_raw3", "variant_raw4"],
    }

    for column in priority_columns.get(key, []):
        value = candidate.get(column)
        if value and str(value).strip().upper() == target:
            return True
    return False


def find_normalize_predict_mudahmy_id(
    cursor: RealDictCursor,
    model_column: str,
    brand: Optional[str],
    model: Optional[str],
    variant: Optional[str],
) -> Optional[int]:
    """
    Cari id normalize_predict_mudahmy yang cocok dengan brand/model/variant
    dari cars_unified source MudahMY.
    """
    brand_norm = normalize_value(brand)
    model_norm = normalize_value(model)
    variant_norm = normalize_value(variant)

    if brand_norm is None or model_norm is None or variant_norm is None:
        return None

    cursor.execute(
        f"""
        SELECT id,
               brand_norm, brand_raw, brand_raw2,
               {model_column} AS model_norm, model_raw, model_raw2, model_raw3,
               variant_norm, variant_raw, variant_raw2, variant_raw3, variant_raw4
        FROM {TB_NORMALIZE_PREDICT_MUDAHMY}
        WHERE UPPER(TRIM(brand_norm)) = %s
           OR UPPER(TRIM(brand_raw)) = %s
           OR UPPER(TRIM(brand_raw2)) = %s
        ORDER BY id
        """,
        (brand_norm, brand_norm, brand_norm),
    )
    candidates = cursor.fetchall()

    for candidate in candidates:
        if not candidate_matches(candidate, "brand", brand_norm):
            continue
        if not candidate_matches(candidate, "model", model_norm):
            continue
        if not candidate_matches(candidate, "variant", variant_norm):
            continue
        return candidate["id"]

    return None


def _db_config() -> Dict[str, Any]:
    return {
        "host": os.getenv("DB_HOST", "127.0.0.1"),
        "port": int(os.getenv("DB_PORT", 5432)),
        "database": os.getenv("DB_NAME", "db_test"),
        "user": os.getenv("DB_USER", "fanfan"),
        "password": os.getenv("DB_PASSWORD", "cenanun"),
    }


def _validate_schema(cursor: RealDictCursor) -> str:
    cursor.execute("SELECT to_regclass(%s) AS table_name", (f"public.{TB_NORMALIZE_PREDICT_MUDAHMY}",))
    if cursor.fetchone()["table_name"] is None:
        raise RuntimeError(
            f"Table {TB_NORMALIZE_PREDICT_MUDAHMY} belum ada. "
            "Jalankan commands/create_normalize_predict_mudahmy.sql terlebih dahulu."
        )

    cursor.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = %s
        """,
        (TB_UNIFIED,),
    )
    unified_columns = {row["column_name"] for row in cursor.fetchall()}
    if TARGET_COLUMN not in unified_columns:
        raise RuntimeError(
            f"Column {TB_UNIFIED}.{TARGET_COLUMN} belum ada. "
            "Jalankan commands/create_normalize_predict_mudahmy.sql terlebih dahulu."
        )

    cursor.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = %s
        """,
        (TB_NORMALIZE_PREDICT_MUDAHMY,),
    )
    normalize_columns = {row["column_name"] for row in cursor.fetchall()}
    required_columns = {
        "id",
        "brand_norm",
        "brand_raw",
        "brand_raw2",
        "model_raw",
        "model_raw2",
        "model_raw3",
        "variant_norm",
        "variant_raw",
        "variant_raw2",
        "variant_raw3",
        "variant_raw4",
    }
    missing_columns = sorted(required_columns - normalize_columns)
    if missing_columns:
        raise RuntimeError(
            f"Table {TB_NORMALIZE_PREDICT_MUDAHMY} kurang kolom: {', '.join(missing_columns)}"
        )

    if "model_norm" in normalize_columns:
        return "model_norm"
    if "model_nowm" in normalize_columns:
        return "model_nowm"
    raise RuntimeError(
        f"Table {TB_NORMALIZE_PREDICT_MUDAHMY} harus punya kolom model_norm atau model_nowm"
    )


def fill_normalize_predict_mudahmy_id(batch_size: int = 500, include_filled: bool = False) -> Dict[str, Any]:
    """Isi normalize_predict_mudahmy_id untuk cars_unified source MudahMY."""
    conn = None
    updated_count = 0
    failed_count = 0
    failed_records: List[Dict[str, Any]] = []
    start_time = datetime.now()

    try:
        conn = psycopg2.connect(**_db_config())
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        conn.autocommit = False

        model_column = _validate_schema(cursor)
        logger.info("Using %s.%s for model matching", TB_NORMALIZE_PREDICT_MUDAHMY, model_column)

        null_filter = "" if include_filled else f"AND {TARGET_COLUMN} IS NULL"
        cursor.execute(
            f"""
            SELECT id, listing_url, brand, model, variant
            FROM {TB_UNIFIED}
            WHERE source = %s
              {null_filter}
              AND brand IS NOT NULL
              AND model IS NOT NULL
              AND variant IS NOT NULL
            ORDER BY id
            """,
            (SOURCE,),
        )
        rows = cursor.fetchall()
        total_records = len(rows)

        logger.info("Database: %s", os.getenv("DB_NAME", "db_test"))
        logger.info("Total MudahMY records to map: %s", total_records)

        if total_records == 0:
            return {
                "status": "success",
                "total_processed": 0,
                "updated": 0,
                "failed": 0,
                "duration": str(datetime.now() - start_time),
                "failed_records_file": None,
            }

        update_stmt = f"""
            UPDATE {TB_UNIFIED}
            SET {TARGET_COLUMN} = %s
            WHERE id = %s
        """

        batch: List[Tuple[int, int]] = []

        with tqdm(total=total_records, desc="MudahMY normalize", unit="record", ncols=100) as progress:
            for row in rows:
                normalize_predict_id = find_normalize_predict_mudahmy_id(
                    cursor,
                    model_column,
                    row.get("brand"),
                    row.get("model"),
                    row.get("variant"),
                )

                if normalize_predict_id:
                    batch.append((normalize_predict_id, row["id"]))
                    updated_count += 1
                else:
                    failed_count += 1
                    failed_records.append(
                        {
                            "id": row["id"],
                            "listing_url": row.get("listing_url"),
                            "brand": row.get("brand"),
                            "model": row.get("model"),
                            "variant": row.get("variant"),
                            "source": SOURCE,
                        }
                    )

                if len(batch) >= batch_size:
                    cursor.executemany(update_stmt, batch)
                    conn.commit()
                    batch.clear()

                progress.set_postfix({"updated": updated_count, "failed": failed_count})
                progress.update(1)

        if batch:
            cursor.executemany(update_stmt, batch)
            conn.commit()

        failed_filename = None
        if failed_records:
            try:
                import pandas as pd

                failed_filename = f"failed_normalize_predict_mudahmy_id_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                pd.DataFrame(failed_records).to_csv(failed_filename, index=False)
                logger.info("Failed records saved to %s", failed_filename)
            except ImportError:
                logger.warning("pandas tidak tersedia; failed records tidak disimpan ke CSV.")

        duration = datetime.now() - start_time
        logger.info("Summary: processed=%s updated=%s failed=%s duration=%s", total_records, updated_count, failed_count, duration)

        return {
            "status": "success",
            "total_processed": total_records,
            "updated": updated_count,
            "failed": failed_count,
            "duration": str(duration),
            "failed_records_file": failed_filename,
        }

    except Exception as exc:
        if conn:
            conn.rollback()
        logger.error("Error saat mengisi normalize_predict_mudahmy_id: %s", exc)
        return {"status": "error", "error": str(exc)}
    finally:
        if conn:
            conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Fill cars_unified.normalize_predict_mudahmy_id from normalize_predict_mudahmy."
    )
    parser.add_argument("--batch-size", type=int, default=500, help="Commit batch size. Default: 500")
    parser.add_argument(
        "--include-filled",
        action="store_true",
        help="Remap all MudahMY rows, including rows that already have normalize_predict_mudahmy_id.",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    result = fill_normalize_predict_mudahmy_id(
        batch_size=args.batch_size,
        include_filled=args.include_filled,
    )

    print("\n" + "=" * 60)
    print("HASIL AKHIR:")
    print("=" * 60)
    for key, value in result.items():
        print(f"{key}: {value}")

    return 0 if result.get("status") == "success" else 1


if __name__ == "__main__":
    sys.exit(main())
