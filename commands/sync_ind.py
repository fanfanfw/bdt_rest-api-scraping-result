"""
Standalone Indonesia Car Data Sync Script
=========================================

Sync Indonesia car data from the scraping database into isolated Indonesia
unified target tables. This script does not write to Malaysia tables and does
not run Malaysia fill scripts.

Usage:
    python commands/sync_ind.py
    python commands/sync_ind.py today
    python commands/sync_ind.py week
    python commands/sync_ind.py month
    python commands/sync_ind.py all-data
    python commands/sync_ind.py --days 7
    python commands/sync_ind.py --hours 6
    python commands/sync_ind.py --since 2026-02-06T00:00:00
    python commands/sync_ind.py --all
    python commands/sync_ind.py --use-ads-date
    python commands/sync_ind.py --verbose
    python commands/sync_ind.py --source mobil123
    python commands/sync_ind.py --source mobil123,carmudi
"""

import argparse
import asyncio
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

try:
    import asyncpg
except ImportError:  # pragma: no cover - dependency check happens at runtime
    asyncpg = None

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor, execute_values
except ImportError:  # pragma: no cover - dependency check happens at runtime
    psycopg2 = None
    RealDictCursor = None
    execute_values = None

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional dependency in some environments
    load_dotenv = None


_repo_root = Path(__file__).resolve().parents[1]
if load_dotenv:
    load_dotenv(dotenv_path=_repo_root / ".env")
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

TARGET_TABLE = "cars_unified_ind"
TARGET_PRICE_HISTORY_TABLE = "price_history_unified_ind"

INDONESIA_SOURCES: Dict[str, Dict[str, Any]] = {
    "mobil123": {
        "source": "mobil123",
        "source_table": "cars_scrap_mobil123",
        "price_table": "price_history_scrap_mobil123",
        "country_code": "ID",
        "currency_code": "IDR",
        "has_information_ads_date": True,
    },
    "carmudi": {
        "source": "carmudi",
        "source_table": "cars_scrap_carmudi",
        "price_table": "price_history_scrap_carmudi",
        "country_code": "ID",
        "currency_code": "IDR",
        "has_information_ads_date": True,
    },
    "olx": {
        "source": "olx",
        "source_table": "cars_scrap_olx",
        "price_table": "price_history_scrap_olx",
        "country_code": "ID",
        "currency_code": "IDR",
        "has_information_ads_date": True,
    },
    "carsomeid": {
        "source": "carsomeid",
        "source_table": "cars_scrap_carsomeid",
        "price_table": "price_history_scrap_carsomeid",
        "country_code": "ID",
        "currency_code": "IDR",
        "has_information_ads_date": False,
    },
}

NULLISH_STRINGS = {"", "-", "N/A", "NA", "NULL", "NONE", "null", "none"}
CAR_COLUMNS = [
    "source",
    "country_code",
    "currency_code",
    "listing_url",
    "listing_id",
    "condition",
    "brand",
    "model",
    "variant",
    "series",
    "type",
    "year",
    "mileage",
    "transmission",
    "seat_capacity",
    "engine_cc",
    "fuel_type",
    "price",
    "location",
    "seller_name",
    "whatsapp_number",
    "contact_seller",
    "information_ads",
    "images",
    "status",
    "created_at",
    "last_scraped_at",
    "version",
    "information_ads_date",
]


class DatabaseConfig:
    """Database configuration using the same env var names as sync_cars.py."""

    def __init__(self):
        self.SOURCE_DB = {
            "host": os.getenv("SOURCE_DB_HOST", "127.0.0.1"),
            "port": int(os.getenv("SOURCE_DB_PORT", 5432)),
            "database": os.getenv("SOURCE_DB_NAME", "db_cars_scrap"),
            "user": os.getenv("SOURCE_DB_USER"),
            "password": os.getenv("SOURCE_DB_PASSWORD"),
        }
        self.TARGET_DB = {
            "host": os.getenv("DB_HOST", "127.0.0.1"),
            "port": int(os.getenv("DB_PORT", 5432)),
            "database": os.getenv("DB_NAME", "db_test"),
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD"),
        }

    def log_config(self):
        """Log database configuration without passwords."""
        logger.info("Database Configuration:")
        logger.info(
            "   Source DB: %s@%s:%s/%s",
            self.SOURCE_DB.get("user") or "<env SOURCE_DB_USER not set>",
            self.SOURCE_DB["host"],
            self.SOURCE_DB["port"],
            self.SOURCE_DB["database"],
        )
        logger.info(
            "   Target DB: %s@%s:%s/%s",
            self.TARGET_DB.get("user") or "<env DB_USER not set>",
            self.TARGET_DB["host"],
            self.TARGET_DB["port"],
            self.TARGET_DB["database"],
        )


def is_nullish(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() in NULLISH_STRINGS
    return False


def clean_optional_text(text: Any) -> Optional[str]:
    """Trim display text while preserving original casing."""
    if is_nullish(text):
        return None
    text_str = str(text).strip()
    return text_str if text_str else None


def normalize_field(text: Any, default_value: Optional[str] = None) -> Optional[str]:
    """Normalize identity/filter fields to uppercase text."""
    if is_nullish(text):
        return default_value

    try:
        import re

        text_str = str(text).strip()
        cleaned = re.sub(r"[-_()]+", " ", text_str)
        cleaned = re.sub(r"[^\w\s]", "", cleaned)
        cleaned = " ".join(cleaned.split()).upper()
        return cleaned if cleaned else default_value
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.warning("Error normalizing field %r: %s", text, exc)
        return default_value


def normalize_text_array(value: Any) -> Optional[List[str]]:
    """Normalize array-ish text input into list[str] or None."""
    if value is None:
        return None

    if isinstance(value, str):
        cleaned = clean_optional_text(value)
        return [cleaned] if cleaned else None

    if isinstance(value, (list, tuple)):
        cleaned_items: List[str] = []
        for item in value:
            cleaned = clean_optional_text(item)
            if cleaned:
                cleaned_items.append(cleaned)
        return cleaned_items or None

    cleaned = clean_optional_text(value)
    return [cleaned] if cleaned else None


class IndonesiaCarDataSyncService:
    """Indonesia-only car data sync service."""

    def __init__(self, config: DatabaseConfig, source_names: Optional[Sequence[str]] = None):
        self.config = config
        self.source_names = list(source_names or INDONESIA_SOURCES.keys())
        self.variant_fallback_count = 0
        self.variant_fallback_by_source = {source: 0 for source in self.source_names}

    def _source_configs(self) -> List[Dict[str, Any]]:
        return [INDONESIA_SOURCES[source] for source in self.source_names]

    async def fetch_source_data(
        self,
        source_config: Dict[str, Any],
        days_back: Optional[int] = None,
        hours_back: Optional[int] = None,
        since: Optional[datetime] = None,
        fetch_all: bool = False,
        use_ads_date_for_today: bool = False,
    ) -> List[Dict[str, Any]]:
        """Fetch car rows from a whitelisted Indonesia source table."""
        if asyncpg is None:
            raise RuntimeError("asyncpg is required to run Indonesia sync")

        table_name = source_config["source_table"]
        conn = None
        try:
            conn = await asyncpg.connect(**self.config.SOURCE_DB)
            base_query = f"SELECT * FROM public.{table_name}"

            query_args: List[Any] = []
            if fetch_all:
                query = f"{base_query} ORDER BY last_scraped_at DESC"
            elif since:
                query = f"""
                    {base_query}
                    WHERE last_scraped_at >= $1
                    ORDER BY last_scraped_at DESC
                """
                query_args = [since]
            elif hours_back:
                query = f"""
                    {base_query}
                    WHERE last_scraped_at >= (NOW() - ($1 * INTERVAL '1 hour'))
                    ORDER BY last_scraped_at DESC
                """
                query_args = [hours_back]
            elif days_back:
                query = f"""
                    {base_query}
                    WHERE last_scraped_at >= (NOW() - ($1 * INTERVAL '1 day'))
                    ORDER BY last_scraped_at DESC
                """
                query_args = [days_back]
            else:
                if use_ads_date_for_today and source_config.get("has_information_ads_date", True):
                    query = f"""
                        {base_query}
                        WHERE information_ads_date = CURRENT_DATE
                        ORDER BY last_scraped_at DESC
                    """
                else:
                    if use_ads_date_for_today and not source_config.get("has_information_ads_date", True):
                        logger.warning(
                            "%s has no information_ads_date column; using last_scraped_at for today filter",
                            source_config["source"],
                        )
                    query = f"""
                        {base_query}
                        WHERE last_scraped_at >= date_trunc('day', NOW())
                        ORDER BY last_scraped_at DESC
                    """

            logger.info("Fetching %s data from %s", source_config["source"], table_name)
            result = await conn.fetch(query, *query_args)
            return [dict(row) for row in result]
        except Exception as exc:
            logger.error("Fatal error fetching %s data from %s: %s", source_config["source"], table_name, exc)
            raise RuntimeError(
                f"Failed to fetch source data from {table_name} for {source_config['source']}: {exc}"
            ) from exc
        finally:
            if conn:
                await conn.close()

    async def fetch_price_history_data(
        self,
        source_config: Dict[str, Any],
        days_back: Optional[int] = None,
        hours_back: Optional[int] = None,
        since: Optional[datetime] = None,
        fetch_all: bool = False,
    ) -> List[Dict[str, Any]]:
        """Fetch price history rows from a whitelisted Indonesia source table."""
        if asyncpg is None:
            raise RuntimeError("asyncpg is required to run Indonesia sync")

        table_name = source_config["price_table"]
        conn = None
        try:
            conn = await asyncpg.connect(**self.config.SOURCE_DB)
            base_query = f"SELECT * FROM public.{table_name}"

            query_args: List[Any] = []
            if fetch_all:
                query = f"{base_query} ORDER BY changed_at DESC"
            elif since:
                query = f"""
                    {base_query}
                    WHERE changed_at >= $1
                    ORDER BY changed_at DESC
                """
                query_args = [since]
            elif hours_back:
                query = f"""
                    {base_query}
                    WHERE changed_at >= (NOW() - ($1 * INTERVAL '1 hour'))
                    ORDER BY changed_at DESC
                """
                query_args = [hours_back]
            elif days_back:
                query = f"""
                    {base_query}
                    WHERE changed_at >= (NOW() - ($1 * INTERVAL '1 day'))
                    ORDER BY changed_at DESC
                """
                query_args = [days_back]
            else:
                query = f"""
                    {base_query}
                    WHERE changed_at >= (NOW() - INTERVAL '30 days')
                    ORDER BY changed_at DESC
                """

            logger.info("Fetching %s price history from %s", source_config["source"], table_name)
            result = await conn.fetch(query, *query_args)
            return [dict(row) for row in result]
        except Exception as exc:
            logger.error("Fatal error fetching %s price history from %s: %s", source_config["source"], table_name, exc)
            raise RuntimeError(
                f"Failed to fetch price history from {table_name} for {source_config['source']}: {exc}"
            ) from exc
        finally:
            if conn:
                await conn.close()

    def normalize_car_data(self, data: Dict[str, Any], source_config: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize one Indonesia source row into cars_unified_ind shape."""
        source = source_config["source"]

        if source == "carsomeid":
            car_type = data.get("car_type")
            images = normalize_text_array(data.get("images"))
            if images is None:
                images = normalize_text_array(data.get("image"))
            information_ads_date = data.get("information_ads_date")
        else:
            car_type = data.get("type")
            images = normalize_text_array(data.get("images"))
            information_ads_date = data.get("information_ads_date")

        variant = normalize_field(data.get("variant"))
        if is_nullish(variant):
            variant = "NO VARIANT"
            self.variant_fallback_count += 1
            self.variant_fallback_by_source[source] = self.variant_fallback_by_source.get(source, 0) + 1

        normalized = {
            "source": source,
            "country_code": source_config["country_code"],
            "currency_code": source_config["currency_code"],
            "listing_url": data.get("listing_url"),
            "listing_id": str(data.get("listing_id")) if data.get("listing_id") is not None else None,
            "condition": normalize_field(data.get("condition")),
            "brand": normalize_field(data.get("brand")),
            "model": normalize_field(data.get("model")),
            "variant": variant,
            "series": normalize_field(data.get("series")),
            "type": normalize_field(car_type),
            "year": data.get("year"),
            "mileage": data.get("mileage"),
            "transmission": normalize_field(data.get("transmission")),
            "seat_capacity": clean_optional_text(data.get("seat_capacity")),
            "engine_cc": clean_optional_text(data.get("engine_cc")),
            "fuel_type": normalize_field(data.get("fuel_type")),
            "price": data.get("price"),
            "location": normalize_field(data.get("location")),
            "seller_name": clean_optional_text(data.get("seller_name")),
            "whatsapp_number": normalize_text_array(data.get("whatsapp_number")),
            "contact_seller": clean_optional_text(data.get("contact_seller")),
            "information_ads": clean_optional_text(data.get("information_ads")),
            "images": images,
            "status": clean_optional_text(data.get("status")) or "active",
            "created_at": data.get("created_at"),
            "last_scraped_at": data.get("last_scraped_at"),
            "version": data.get("version") or 1,
            "information_ads_date": information_ads_date,
        }
        return normalized

    def _validate_car_data(self, normalized_data: Iterable[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], int]:
        valid_data: List[Dict[str, Any]] = []
        skipped = 0
        strict_required_fields = ["source", "listing_url", "brand", "model", "price", "year", "mileage"]

        for data in normalized_data:
            missing_field = None
            for field in strict_required_fields:
                if is_nullish(data.get(field)):
                    missing_field = field
                    break
            if missing_field:
                skipped += 1
                logger.warning(
                    "Skipped record missing required %s: %s - %s",
                    missing_field,
                    data.get("source", "unknown"),
                    data.get("listing_url", "no url"),
                )
                continue
            valid_data.append(data)

        return valid_data, skipped

    def sync_to_target_database(self, normalized_data: List[Dict[str, Any]]) -> Tuple[int, int, int]:
        """Bulk upsert normalized car data into cars_unified_ind."""
        if psycopg2 is None or execute_values is None:
            raise RuntimeError("psycopg2 is required to run Indonesia sync")

        if not normalized_data:
            return 0, 0, 0

        valid_data, skipped = self._validate_car_data(normalized_data)
        if not valid_data:
            logger.warning("No valid car records to process")
            return 0, 0, skipped

        conn = None
        try:
            conn = psycopg2.connect(**self.config.TARGET_DB)
            cur = conn.cursor(cursor_factory=RealDictCursor)

            update_assignments = ",\n                    ".join(
                [
                    f"{column} = EXCLUDED.{column}"
                    for column in CAR_COLUMNS
                    if column not in {"source", "listing_url", "created_at", "last_scraped_at", "version"}
                ]
            )
            upsert_query = f"""
                INSERT INTO {TARGET_TABLE} (
                    {", ".join(CAR_COLUMNS)}
                ) VALUES %s
                ON CONFLICT (source, listing_url)
                DO UPDATE SET
                    {update_assignments},
                    created_at = COALESCE({TARGET_TABLE}.created_at, EXCLUDED.created_at),
                    last_scraped_at = EXCLUDED.last_scraped_at,
                    version = EXCLUDED.version
                RETURNING (xmax = 0) AS inserted
            """

            values = [tuple(data.get(column) for column in CAR_COLUMNS) for data in valid_data]

            logger.info("Bulk UPSERT into %s for %s car records", TARGET_TABLE, len(values))
            result = execute_values(cur, upsert_query, values, page_size=1000, fetch=True)
            inserted = sum(1 for row in result if row["inserted"])
            updated = len(result) - inserted
            conn.commit()
            logger.info("Car UPSERT complete: %s inserted, %s updated, %s skipped", inserted, updated, skipped)
            return inserted, updated, skipped
        except Exception as exc:
            logger.error("Fatal database car sync error for %s: %s", TARGET_TABLE, exc)
            if conn:
                conn.rollback()
            raise RuntimeError(f"Failed to sync car data into {TARGET_TABLE}: {exc}") from exc
        finally:
            if conn:
                conn.close()

    def sync_price_history_direct(
        self,
        price_data: List[Dict[str, Any]],
        source_config: Dict[str, Any],
    ) -> Tuple[int, int, int]:
        """Bulk upsert price history into price_history_unified_ind."""
        if psycopg2 is None or execute_values is None:
            raise RuntimeError("psycopg2 is required to run Indonesia sync")

        if not price_data:
            return 0, 0, 0

        source = source_config["source"]
        valid_data: List[Dict[str, Any]] = []
        skipped = 0

        for data in price_data:
            if (
                is_nullish(data.get("listing_url"))
                or is_nullish(data.get("listing_id"))
                or is_nullish(data.get("new_price"))
                or is_nullish(data.get("changed_at"))
            ):
                skipped += 1
                continue
            valid_data.append(data)

        if not valid_data:
            logger.warning("No valid %s price history records to process", source)
            return 0, 0, skipped

        conn = None
        try:
            conn = psycopg2.connect(**self.config.TARGET_DB)
            cur = conn.cursor(cursor_factory=RealDictCursor)

            listing_urls = sorted({row["listing_url"] for row in valid_data})
            existing_urls = set()
            chunk_size = 10000
            for i in range(0, len(listing_urls), chunk_size):
                chunk = listing_urls[i : i + chunk_size]
                cur.execute(
                    f"SELECT listing_url FROM {TARGET_TABLE} WHERE source = %s AND listing_url = ANY(%s::text[])",
                    (source, chunk),
                )
                existing_urls.update(row["listing_url"] for row in cur.fetchall())

            filtered_data = [row for row in valid_data if row["listing_url"] in existing_urls]
            skipped += len(valid_data) - len(filtered_data)

            if not filtered_data:
                logger.warning("No %s price history records remain after %s listing filter", source, TARGET_TABLE)
                return 0, 0, skipped

            upsert_query = f"""
                INSERT INTO {TARGET_PRICE_HISTORY_TABLE} (
                    source, country_code, currency_code, listing_id, listing_url, old_price, new_price, changed_at
                ) VALUES %s
                ON CONFLICT (listing_url, changed_at)
                DO UPDATE SET
                    source = EXCLUDED.source,
                    country_code = EXCLUDED.country_code,
                    currency_code = EXCLUDED.currency_code,
                    listing_id = EXCLUDED.listing_id,
                    old_price = EXCLUDED.old_price,
                    new_price = EXCLUDED.new_price
                RETURNING (xmax = 0) AS inserted
            """

            values = [
                (
                    source,
                    source_config["country_code"],
                    source_config["currency_code"],
                    str(row["listing_id"]) if row.get("listing_id") is not None else None,
                    row["listing_url"],
                    row.get("old_price"),
                    row["new_price"],
                    row["changed_at"],
                )
                for row in filtered_data
            ]

            logger.info("Bulk UPSERT into %s for %s %s price records", TARGET_PRICE_HISTORY_TABLE, len(values), source)
            result = execute_values(cur, upsert_query, values, page_size=1000, fetch=True)
            inserted = sum(1 for row in result if row["inserted"])
            updated = len(result) - inserted
            conn.commit()
            logger.info("%s price UPSERT complete: %s inserted, %s updated, %s skipped", source, inserted, updated, skipped)
            return inserted, updated, skipped
        except Exception as exc:
            logger.error("Fatal price history sync error for %s into %s: %s", source, TARGET_PRICE_HISTORY_TABLE, exc)
            if conn:
                conn.rollback()
            raise RuntimeError(
                f"Failed to sync price history for {source} into {TARGET_PRICE_HISTORY_TABLE}: {exc}"
            ) from exc
        finally:
            if conn:
                conn.close()

    async def sync_all_data(
        self,
        days_back: Optional[int] = None,
        hours_back: Optional[int] = None,
        since: Optional[datetime] = None,
        fetch_all: bool = False,
        use_ads_date_for_today: bool = False,
    ) -> Dict[str, Any]:
        """Fetch, normalize, and sync Indonesia car and price-history data."""
        logger.info("Starting Indonesia car data synchronization")
        logger.info("Sources: %s", ", ".join(self.source_names))

        source_configs = self._source_configs()
        source_stats = {
            config["source"]: {"fetched": 0, "normalized": 0}
            for config in source_configs
        }
        price_stats = {
            config["source"]: {"fetched": 0, "inserted": 0, "updated": 0, "skipped": 0}
            for config in source_configs
        }

        fetched_batches = await asyncio.gather(
            *[
                self.fetch_source_data(
                    config,
                    days_back=days_back,
                    hours_back=hours_back,
                    since=since,
                    fetch_all=fetch_all,
                    use_ads_date_for_today=use_ads_date_for_today,
                )
                for config in source_configs
            ]
        )

        all_normalized_data: List[Dict[str, Any]] = []
        for config, rows in zip(source_configs, fetched_batches):
            source = config["source"]
            source_stats[source]["fetched"] = len(rows)
            normalized_rows = [self.normalize_car_data(row, config) for row in rows]
            source_stats[source]["normalized"] = len(normalized_rows)
            all_normalized_data.extend(normalized_rows)
            logger.info("%s car data: %s fetched, %s normalized", source, len(rows), len(normalized_rows))

        car_inserted, car_updated, car_skipped = self.sync_to_target_database(all_normalized_data)

        price_batches = await asyncio.gather(
            *[
                self.fetch_price_history_data(
                    config,
                    days_back=days_back,
                    hours_back=hours_back,
                    since=since,
                    fetch_all=fetch_all,
                )
                for config in source_configs
            ]
        )

        for config, rows in zip(source_configs, price_batches):
            source = config["source"]
            price_stats[source]["fetched"] = len(rows)
            inserted, updated, skipped = self.sync_price_history_direct(rows, config)
            price_stats[source].update({"inserted": inserted, "updated": updated, "skipped": skipped})

        summary = {
            "cars": {
                "total_fetched": sum(stats["fetched"] for stats in source_stats.values()),
                "total_normalized": sum(stats["normalized"] for stats in source_stats.values()),
                "inserted": car_inserted,
                "updated": car_updated,
                "skipped": car_skipped,
                "variant_fallbacks": self.variant_fallback_count,
                "by_source": source_stats,
                "variant_fallbacks_by_source": self.variant_fallback_by_source,
            },
            "fill_results": {
                "cars_standard_updated": 0,
                "normalize_predict_mudahmy_updated": 0,
            },
            "price_history": price_stats,
        }
        logger.info("Indonesia car data synchronization completed")
        return summary


# Module-level wrappers for direct import/testing.
async def fetch_source_data(*args, **kwargs):
    return await IndonesiaCarDataSyncService(DatabaseConfig()).fetch_source_data(*args, **kwargs)


async def fetch_price_history_data(*args, **kwargs):
    return await IndonesiaCarDataSyncService(DatabaseConfig()).fetch_price_history_data(*args, **kwargs)


def normalize_car_data(data: Dict[str, Any], source_config: Dict[str, Any]) -> Dict[str, Any]:
    return IndonesiaCarDataSyncService(DatabaseConfig()).normalize_car_data(data, source_config)


def sync_to_target_database(normalized_data: List[Dict[str, Any]]) -> Tuple[int, int, int]:
    return IndonesiaCarDataSyncService(DatabaseConfig()).sync_to_target_database(normalized_data)


def sync_price_history_direct(price_data: List[Dict[str, Any]], source_config: Dict[str, Any]) -> Tuple[int, int, int]:
    return IndonesiaCarDataSyncService(DatabaseConfig()).sync_price_history_direct(price_data, source_config)


async def sync_all_data(*args, **kwargs) -> Dict[str, Any]:
    return await IndonesiaCarDataSyncService(DatabaseConfig()).sync_all_data(*args, **kwargs)


def display_summary(summary: Dict[str, Any]):
    """Display Indonesia sync summary."""
    print("\n" + "=" * 60)
    print("INDONESIA SYNCHRONIZATION COMPLETED")
    print("=" * 60)

    cars = summary["cars"]
    print("CAR DATA:")
    print(f"   Total fetched: {cars['total_fetched']}")
    print(f"   Total normalized: {cars['total_normalized']}")
    for source in INDONESIA_SOURCES:
        stats = cars.get("by_source", {}).get(source, {"fetched": 0, "normalized": 0})
        fallbacks = cars.get("variant_fallbacks_by_source", {}).get(source, 0)
        print(
            f"   - {source}: fetched={stats.get('fetched', 0)}, "
            f"normalized={stats.get('normalized', 0)}, variant_fallbacks={fallbacks}"
        )
    print(f"   Inserted: {cars['inserted']}")
    print(f"   Updated: {cars['updated']}")
    print(f"   Skipped: {cars['skipped']}")
    print(f"   Variant fallbacks: {cars['variant_fallbacks']}")

    fill = summary.get("fill_results", {})
    print("\nFILL RESULTS:")
    print(f"   Cars Standard ID: {fill.get('cars_standard_updated', 0)} updated")
    print(f"   Normalize Predict MudahMY ID: {fill.get('normalize_predict_mudahmy_updated', 0)} updated")

    print("\nPRICE HISTORY:")
    price_history = summary.get("price_history", {})
    for source in INDONESIA_SOURCES:
        stats = price_history.get(source, {"fetched": 0, "inserted": 0, "updated": 0, "skipped": 0})
        print(
            f"   - {source}: fetched={stats.get('fetched', 0)}, "
            f"inserted={stats.get('inserted', 0)}, updated={stats.get('updated', 0)}, "
            f"skipped={stats.get('skipped', 0)}"
        )

    print("\nSync completed successfully")


def parse_sources(raw_sources: Optional[str], parser: argparse.ArgumentParser) -> List[str]:
    if not raw_sources:
        return list(INDONESIA_SOURCES.keys())

    selected = [source.strip() for source in raw_sources.split(",") if source.strip()]
    if not selected:
        parser.error("--source must include at least one source")

    unknown = [source for source in selected if source not in INDONESIA_SOURCES]
    if unknown:
        allowed = ", ".join(INDONESIA_SOURCES.keys())
        parser.error(f"Unknown --source value(s): {', '.join(unknown)}. Allowed values: {allowed}")

    return selected


def parse_since(raw_since: str) -> datetime:
    since_raw = raw_since.strip().replace("Z", "+00:00")
    return datetime.fromisoformat(since_raw)


async def main():
    """Command-line entry point."""
    parser = argparse.ArgumentParser(description="Indonesia Car Data Sync Script")
    parser.add_argument("mode", nargs="?", choices=["today", "week", "month", "all-data"], help="Quick sync mode")
    parser.add_argument("--days", type=int, help="Number of days back to sync")
    parser.add_argument("--hours", type=int, help="Number of hours back to sync")
    parser.add_argument(
        "--since",
        type=str,
        help="Sync records since this ISO timestamp (e.g. 2026-02-06T00:00:00)",
    )
    parser.add_argument("--all", action="store_true", help="Sync all data")
    parser.add_argument(
        "--use-ads-date",
        action="store_true",
        help="For default today mode, filter by information_ads_date instead of last_scraped_at where available",
    )
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    parser.add_argument(
        "--source",
        type=str,
        help="Comma-separated Indonesia sources to sync: mobil123,carmudi,olx,carsomeid",
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    selected_sources = parse_sources(args.source, parser)

    days_back = None
    hours_back = None
    since = None
    fetch_all = False

    if args.mode:
        if args.mode == "today":
            days_back = None
        elif args.mode == "week":
            days_back = 7
        elif args.mode == "month":
            days_back = 30
        elif args.mode == "all-data":
            fetch_all = True
    elif args.all:
        fetch_all = True
    elif args.since:
        try:
            since = parse_since(args.since)
        except Exception as exc:
            print(f"Invalid --since value: {args.since} ({exc})", file=sys.stderr)
            sys.exit(2)
    elif args.hours is not None:
        hours_back = args.hours
    elif args.days is not None:
        days_back = args.days

    selected_ranges = sum(
        1
        for selected in (
            fetch_all,
            days_back is not None,
            hours_back is not None,
            since is not None,
        )
        if selected
    )
    if selected_ranges > 1:
        print("Cannot combine --all/--days/--hours/--since together", file=sys.stderr)
        sys.exit(1)

    print("Indonesia Car Data Synchronization Script")
    print("-" * 44)
    print(f"Sources: {', '.join(selected_sources)}")
    if fetch_all:
        print("Mode: Sync ALL data")
    elif since:
        print(f"Mode: Sync since {since.isoformat()}")
    elif hours_back is not None:
        print(f"Mode: Sync last {hours_back} hours")
    elif days_back is not None:
        print(f"Mode: Sync last {days_back} days")
    else:
        if args.use_ads_date:
            print("Mode: Sync today only (information_ads_date where available)")
        else:
            print("Mode: Sync today only (last_scraped_at)")

    config = DatabaseConfig()
    config.log_config()
    print()

    try:
        service = IndonesiaCarDataSyncService(config, selected_sources)
        summary = await service.sync_all_data(
            days_back=days_back,
            hours_back=hours_back,
            since=since,
            fetch_all=fetch_all,
            use_ads_date_for_today=args.use_ads_date,
        )
        display_summary(summary)
    except KeyboardInterrupt:
        print("\nSync interrupted by user")
        sys.exit(1)
    except Exception as exc:
        logger.error("Indonesia sync failed: %s", exc)
        print(f"\nSync failed: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
