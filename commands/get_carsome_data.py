"""
Fetch latest Carsome data from Scrut API and upsert into the carsome table.

Usage:
    python commands/get_carsome_data.py
    python commands/get_carsome_data.py --limit 100
    python commands/get_carsome_data.py --dry-run
"""

import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor
import requests
from dotenv import load_dotenv

# Ensure environment variables are loaded
load_dotenv(override=True)

# Make sure local commands package import works when executed via `python commands/...`
CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.append(str(CURRENT_DIR))

try:
    from fill_carsome_standard_id import find_cars_standard_id  # type: ignore
except ImportError as exc:
    raise SystemExit("Unable to import fill_carsome_standard_id. Ensure the file exists.") from exc

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Constants
DEFAULT_API_URL = "https://api.scrut.my/scrape/daily-carsome"
DEFAULT_API_KEY = os.getenv("SCRUT_CARSOME_API_KEY", "KJfaCh21tDq0")
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
RUN_LOG_FILE = LOG_DIR / "get_carsome_data.log"


def get_db_config() -> Dict[str, Any]:
    """Read database configuration from environment variables."""
    return {
        "host": os.getenv("DB_HOST", "127.0.0.1"),
        "port": int(os.getenv("DB_PORT", 5432)),
        "database": os.getenv("DB_NAME", "db_test"),
        "user": os.getenv("DB_USER", "fanfan"),
        "password": os.getenv("DB_PASSWORD", "cenanun"),
    }


def normalize_upper(value: Optional[str], fallback: str) -> str:
    """Normalize text values similar to import_carsome.py."""
    if value is None:
        return fallback

    text = value.strip()
    if not text:
        return fallback

    replacements = str.maketrans({"-": " ", "_": " "})
    text = text.translate(replacements)
    text = text.replace("(", " ").replace(")", " ")
    cleaned = " ".join("".join(ch for ch in text if ch.isalnum() or ch.isspace()).split())
    return cleaned.upper() if cleaned else fallback


def normalize_variant(value: Optional[str]) -> str:
    """Normalize variant text."""
    return normalize_upper(value, "NO VARIANT")


def parse_int(value: Any) -> Optional[int]:
    """Parse generic integers; keep None when invalid."""
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    if text.isdigit():
        return int(text)

    digits = "".join(ch for ch in text if ch.isdigit())
    return int(digits) if digits else None


def parse_datetime(value: Optional[str]) -> datetime:
    """Parse ISO datetime string to naive datetime (UTC)."""
    if not value:
        return datetime.utcnow()

    try:
        normalized = value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalized)
        return dt.replace(tzinfo=None)
    except ValueError:
        logger.warning("Unable to parse datetime '%s', using current UTC time.", value)
        return datetime.utcnow()


def fetch_carsome_payload_from_api(api_url: str, api_key: str, timeout: int = 60) -> List[Dict[str, Any]]:
    """Call Scrut API and return payload list."""
    headers = {"x-api-key": api_key}
    logger.info("Fetching Carsome data from %s ...", api_url)
    response = requests.get(api_url, headers=headers, timeout=timeout)
    response.raise_for_status()
    payload = response.json()

    if not isinstance(payload, dict) or "data" not in payload:
        raise ValueError("Unexpected API response format.")

    data = payload.get("data") or []
    if not isinstance(data, list):
        raise ValueError("API response 'data' is not a list.")

    logger.info("API returned %s entries.", len(data))
    return data


def extract_record(entry: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """Map API entry into carsome table schema."""
    reg_no = (entry.get("reg_no") or "").strip().upper()
    created_at = parse_datetime(entry.get("created_at"))

    car_block = (
        entry.get("data", {})
        .get("result", {})
        .get("data", {})
        .get("car", {})
    )

    if not car_block:
        logger.debug("Entry for reg_no %s missing car block, skipping.", reg_no or "<unknown>")
        return None, "missing car block"

    image = car_block.get("image")
    car_id = parse_int(car_block.get("id") or car_block.get("carId"))
    brand = normalize_upper(car_block.get("brandName"), "NO BRAND")
    model = normalize_upper(car_block.get("modelName"), "NO MODEL")
    model_group = "NO MODEL GROUP"
    variant = normalize_variant(car_block.get("carVariant"))
    year = parse_int(car_block.get("carYear"))
    mileage = parse_int(car_block.get("carMileage"))

    price = parse_int(
        car_block.get("allPrice", {}).get("price")
        or car_block.get("expSellingPrice")
    )

    if not reg_no or car_id is None or price is None:
        logger.debug(
            "Skipping entry due to missing reg_no/car_id/price. reg_no=%s car_id=%s price=%s",
            reg_no or "<unknown>",
            car_id,
            price,
        )
        return None, "missing reg_no/car_id/price"

    record = {
        "reg_no": reg_no,
        "image": image,
        "car_id": car_id,
        "brand": brand,
        "model": model,
        "model_group": model_group,
        "variant": variant,
        "year": year,
        "mileage": mileage,
        "price": price,
        "created_at": created_at,
    }
    return record, None


def upsert_records(records: List[Dict[str, Any]], dry_run: bool = False) -> Dict[str, int]:
    """Upsert records into carsome table and auto fill cars_standard_id."""
    if dry_run:
        logger.info("Dry-run enabled: %s records parsed (no DB changes).", len(records))
        return {"inserted": 0, "updated": 0}

    db_config = get_db_config()
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()
    ref_cursor = conn.cursor(cursor_factory=RealDictCursor)

    upsert_sql = """
        INSERT INTO carsome (
            reg_no, image, car_id, brand, model, model_group, variant,
            year, mileage, price, created_at, cars_standard_id,
            status, is_deleted, source, last_updated_at
        )
        VALUES (
            %(reg_no)s, %(image)s, %(car_id)s, %(brand)s, %(model)s,
            %(model_group)s, %(variant)s, %(year)s, %(mileage)s, %(price)s,
            %(created_at)s, %(cars_standard_id)s, 'active', FALSE, 'carsome', NOW()
        )
        ON CONFLICT (reg_no, car_id) DO UPDATE SET
            image = EXCLUDED.image,
            brand = EXCLUDED.brand,
            model = EXCLUDED.model,
            model_group = EXCLUDED.model_group,
            variant = EXCLUDED.variant,
            year = EXCLUDED.year,
            mileage = EXCLUDED.mileage,
            price = EXCLUDED.price,
            created_at = EXCLUDED.created_at,
            cars_standard_id = COALESCE(EXCLUDED.cars_standard_id, carsome.cars_standard_id),
            status = EXCLUDED.status,
            is_deleted = EXCLUDED.is_deleted,
            source = EXCLUDED.source,
            last_updated_at = NOW()
        RETURNING (xmax = 0) AS inserted_flag;
    """

    inserted = 0
    updated = 0
    standard_cache: Dict[Tuple[Optional[str], Optional[str], Optional[str], Optional[str]], Optional[int]] = {}

    try:
        for record in records:
            cache_key = (
                record["brand"],
                record["model_group"],
                record["model"],
                record["variant"],
            )
            if cache_key not in standard_cache:
                cars_standard_id = find_cars_standard_id(
                    ref_cursor,
                    record["brand"],
                    record["model_group"],
                    record["model"],
                    record["variant"],
                )
                standard_cache[cache_key] = cars_standard_id
            else:
                cars_standard_id = standard_cache[cache_key]

            record["cars_standard_id"] = cars_standard_id

            cursor.execute(upsert_sql, record)
            inserted_flag = cursor.fetchone()[0]
            if inserted_flag:
                inserted += 1
            else:
                updated += 1

        conn.commit()
        logger.info("DB upsert completed: inserted=%s updated=%s", inserted, updated)
    except Exception:
        conn.rollback()
        logger.exception("Failed to upsert Carsome data.")
        raise
    finally:
        cursor.close()
        ref_cursor.close()
        conn.close()

    return {"inserted": inserted, "updated": updated}


def append_run_log(
    total_entries: int,
    prepared: int,
    stats: Dict[str, int],
    failed_records: List[Dict[str, Optional[str]]],
) -> None:
    """Append high-level summary to logs/get_carsome_data.log."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = (
        f"[{timestamp}] total={total_entries} prepared={prepared} "
        f"inserted={stats.get('inserted', 0)} updated={stats.get('updated', 0)} "
        f"failed={len(failed_records)}"
    )
    with RUN_LOG_FILE.open("a", encoding="utf-8") as fp:
        fp.write(line + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch latest Carsome data and update carsome table.")
    parser.add_argument("--api-url", default=DEFAULT_API_URL, help="Scrut Carsome API URL.")
    parser.add_argument("--api-key", default=DEFAULT_API_KEY, help="Scrut Carsome API key.")
    parser.add_argument("--input-file", help="Optional JSON file with Carsome payload for offline testing.")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of records processed.")
    parser.add_argument("--dry-run", action="store_true", help="Parse data without touching the database.")
    args = parser.parse_args()

    try:
        if args.input_file:
            import json

            logger.info("Loading Carsome payload from file: %s", args.input_file)
            with open(args.input_file, "r", encoding="utf-8") as fp:
                payload = json.load(fp)

            if isinstance(payload, dict) and "data" in payload:
                entries = payload["data"] or []
            elif isinstance(payload, list):
                entries = payload
            else:
                raise ValueError("Input file must contain either a list or an object with a 'data' list.")
        else:
            entries = fetch_carsome_payload_from_api(args.api_url, args.api_key)

        if args.limit is not None:
            entries = entries[: args.limit]
            logger.info("Processing limited subset: %s entries", len(entries))

        records: List[Dict[str, Any]] = []
        failed_records: List[Dict[str, Optional[str]]] = []
        for entry in entries:
            record, error = extract_record(entry)
            if record:
                records.append(record)
            else:
                failed_records.append(
                    {
                        "reg_no": (entry.get("reg_no") or "").strip().upper() or None,
                        "reason": error,
                    }
                )

        logger.info("Prepared %s records for upsert.", len(records))
        if not records:
            logger.warning("No valid records to process.")
            append_run_log(len(entries), 0, {"inserted": 0, "updated": 0}, failed_records)
            return

        stats = upsert_records(records, dry_run=args.dry_run)
        logger.info(
            "Process finished. Inserted=%s Updated=%s Failed=%s",
            stats["inserted"],
            stats["updated"],
            len(failed_records),
        )
        append_run_log(len(entries), len(records), stats, failed_records)
    except Exception as exc:
        logger.error("Process failed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
