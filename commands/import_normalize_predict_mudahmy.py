#!/usr/bin/env python3
"""
Import Normalize Predict MudahMY CSV
===================================

One-time importer untuk mengisi tabel normalize_predict_mudahmy dari CSV.

Usage:
    python3 commands/import_normalize_predict_mudahmy.py
    python3 commands/import_normalize_predict_mudahmy.py --clear-first
"""

import argparse
import csv
import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values

load_dotenv(override=True)

project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

logger = logging.getLogger(__name__)

DEFAULT_CSV_PATH = project_root / "commands" / "data-normalize-predict-mudahmy.csv"
TABLE_NAME = "normalize_predict_mudahmy"

CSV_TO_DB_COLUMNS = {
    "id": "id",
    "brand_norm": "brand_norm",
    "brand_raw": "brand_raw",
    "brand_raw2": "brand_raw2",
    "model_norm": "model_norm",
    "model_nowm": "model_norm",
    "model_raw": "model_raw",
    "model_raw2": "model_raw2",
    "model_raw3": "model_raw3",
    "variant_norm": "variant_norm",
    "variant_raw": "variant_raw",
    "variant_raw2": "variant_raw2",
    "variant_raw3": "variant_raw3",
    "variant_raw4": "variant_raw4",
}

BASE_DB_COLUMNS = [
    "id",
    "brand_norm",
    "brand_raw",
    "brand_raw2",
    "model_norm",
    "model_raw",
    "model_raw2",
    "model_raw3",
    "variant_norm",
    "variant_raw",
    "variant_raw2",
    "variant_raw3",
    "variant_raw4",
]


def _db_config() -> Dict[str, Any]:
    return {
        "host": os.getenv("DB_HOST", "127.0.0.1"),
        "port": int(os.getenv("DB_PORT", 5432)),
        "database": os.getenv("DB_NAME", "db_test"),
        "user": os.getenv("DB_USER", "fanfan"),
        "password": os.getenv("DB_PASSWORD", "cenanun"),
    }


def parse_csv_value(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    cleaned = str(value).strip()
    if cleaned.upper() in {"", "NULL", "N/A", "-"}:
        return None
    return cleaned


def parse_id(value: Optional[str]) -> int:
    cleaned = parse_csv_value(value)
    if cleaned is None:
        raise ValueError("id kosong")
    return int(cleaned)


def detect_dialect(csv_path: Path) -> csv.Dialect:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as file:
        sample = file.read(4096)
    return csv.Sniffer().sniff(sample, delimiters=",;\t|")


def read_csv_rows(csv_path: Path, model_column: str = "model_norm") -> List[Dict[str, Any]]:
    dialect = detect_dialect(csv_path)
    rows: List[Dict[str, Any]] = []
    db_columns = [model_column if column == "model_norm" else column for column in BASE_DB_COLUMNS]

    with csv_path.open("r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file, dialect=dialect)
        if not reader.fieldnames:
            raise ValueError("CSV tidak memiliki header")

        unknown_columns = sorted(set(reader.fieldnames) - set(CSV_TO_DB_COLUMNS))
        if unknown_columns:
            raise ValueError(f"Kolom CSV tidak dikenal: {', '.join(unknown_columns)}")

        required_columns = {"id", "brand_norm", "variant_norm"}
        missing_columns = sorted(required_columns - set(reader.fieldnames))
        if "model_norm" not in reader.fieldnames and "model_nowm" not in reader.fieldnames:
            missing_columns.append("model_norm/model_nowm")
        if missing_columns:
            raise ValueError(f"Kolom wajib tidak ada: {', '.join(missing_columns)}")

        for line_number, raw_row in enumerate(reader, start=2):
            try:
                row = {column: None for column in db_columns}
                for csv_column, raw_value in raw_row.items():
                    db_column = CSV_TO_DB_COLUMNS[csv_column]
                    if db_column == "model_norm":
                        db_column = model_column
                    row[db_column] = parse_csv_value(raw_value)

                row["id"] = parse_id(raw_row.get("id"))
                if row["brand_norm"] is None or row[model_column] is None or row["variant_norm"] is None:
                    raise ValueError("brand_norm, model_norm/model_nowm, dan variant_norm wajib terisi")

                rows.append(row)
            except Exception as exc:
                raise ValueError(f"Error di baris {line_number}: {exc}") from exc

    return rows


def get_model_column(cursor) -> str:
    cursor.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = %s
        """,
        (TABLE_NAME,),
    )
    columns = {row[0] for row in cursor.fetchall()}
    if "model_norm" in columns:
        return "model_norm"
    if "model_nowm" in columns:
        return "model_nowm"
    raise RuntimeError(f"Table {TABLE_NAME} harus punya kolom model_norm atau model_nowm")


def import_normalize_predict_mudahmy(csv_path: Path = DEFAULT_CSV_PATH, clear_first: bool = False) -> Dict[str, Any]:
    conn = None
    try:
        conn = psycopg2.connect(**_db_config())
        cursor = conn.cursor()
        conn.autocommit = False

        model_column = get_model_column(cursor)
        db_columns = [model_column if column == "model_norm" else column for column in BASE_DB_COLUMNS]
        rows = read_csv_rows(csv_path, model_column=model_column)
        if not rows:
            return {"status": "success", "inserted_or_updated": 0, "csv_path": str(csv_path)}

        values = [tuple(row[column] for column in db_columns) for row in rows]
        columns_sql = ", ".join(db_columns)
        update_sql = ", ".join(f"{column} = EXCLUDED.{column}" for column in db_columns if column != "id")

        query = f"""
            INSERT INTO {TABLE_NAME} ({columns_sql})
            VALUES %s
            ON CONFLICT (id) DO UPDATE SET
                {update_sql}
        """

        if clear_first:
            logger.info("Clearing %s before import", TABLE_NAME)
            cursor.execute(f"DELETE FROM {TABLE_NAME}")

        execute_values(cursor, query, values, page_size=1000)
        conn.commit()

        logger.info("Imported %s rows into %s", len(rows), TABLE_NAME)
        return {
            "status": "success",
            "inserted_or_updated": len(rows),
            "csv_path": str(csv_path),
            "clear_first": clear_first,
            "model_column": model_column,
        }
    except Exception as exc:
        if conn:
            conn.rollback()
        logger.error("Import failed: %s", exc)
        return {"status": "error", "error": str(exc), "csv_path": str(csv_path)}
    finally:
        if conn:
            conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Import normalize_predict_mudahmy from CSV.")
    parser.add_argument("--csv-path", default=str(DEFAULT_CSV_PATH), help="CSV path to import.")
    parser.add_argument("--clear-first", action="store_true", help="Delete existing rows before importing.")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    result = import_normalize_predict_mudahmy(Path(args.csv_path), clear_first=args.clear_first)

    print("\n" + "=" * 60)
    print("HASIL IMPORT:")
    print("=" * 60)
    for key, value in result.items():
        print(f"{key}: {value}")

    return 0 if result.get("status") == "success" else 1


if __name__ == "__main__":
    sys.exit(main())
