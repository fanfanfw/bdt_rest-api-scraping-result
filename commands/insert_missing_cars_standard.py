#!/usr/bin/env python3
"""
Insert missing cars_standard rows from unified tables.

This script reads rows whose cars_standard_id is still NULL from a dynamic
unified table, then inserts new normalized combinations into cars_standard.

Examples:
    python commands/insert_missing_cars_standard.py \
        --table cars_unified \
        --sources mudahmy,carlistmy

    python commands/insert_missing_cars_standard.py \
        --table cars_unified_ind \
        --sources mobil123,carmudi,carsome,olx

    python commands/insert_missing_cars_standard.py \
        --table cars_unified_copy \
        --sources mudahmy,carlistmy \
        --fill-after
"""

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Sequence

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional in some local environments
    load_dotenv = None

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

if load_dotenv:
    load_dotenv(dotenv_path=BASE_DIR / ".env", override=True)

try:
    from commands.fill_cars_standard_id import fill_all_cars_standard_id
except ImportError:  # pragma: no cover - optional at runtime
    fill_all_cars_standard_id = None

logger = logging.getLogger(__name__)

MODEL_GROUP_DEFAULT = "NO MODEL GROUP"
SOURCE_ALIASES = {
    "carsome": "carsomeid",
}
NULLISH_VALUES = ("", "-", "N/A", "NA", "NULL", "NONE")
REQUIRED_TABLE_COLUMNS = {
    "id",
    "source",
    "cars_standard_id",
    "brand",
    "model",
    "variant",
}


def validate_table_name(table_name: str) -> str:
    """Allow only simple SQL identifiers for dynamic table names."""
    if not table_name or not table_name.replace("_", "").isalnum():
        raise ValueError(f"Invalid table name: {table_name!r}")
    return table_name


def parse_sources(raw_sources: str) -> List[str]:
    sources: List[str] = []
    for token in raw_sources.split(","):
        source = token.strip().lower()
        if not source:
            continue
        sources.append(SOURCE_ALIASES.get(source, source))
    return sorted(set(sources))


def get_db_config() -> Dict[str, object]:
    return {
        "host": os.getenv("DB_HOST", "127.0.0.1"),
        "port": int(os.getenv("DB_PORT", 5432)),
        "database": os.getenv("DB_NAME", "db_test"),
        "user": os.getenv("DB_USER", "fanfan"),
        "password": os.getenv("DB_PASSWORD", "cenanun"),
    }


def get_table_columns(cur: Any, table_name: str) -> set:
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = %s
        """,
        (table_name,),
    )
    return {row["column_name"] for row in cur.fetchall()}


def ensure_required_schema(cur: Any, table_name: str) -> None:
    source_columns = get_table_columns(cur, table_name)
    if not source_columns:
        raise RuntimeError(f"Table not found in current schema: {table_name}")

    missing_source_columns = REQUIRED_TABLE_COLUMNS - source_columns
    if missing_source_columns:
        missing = ", ".join(sorted(missing_source_columns))
        raise RuntimeError(f"{table_name} is missing required columns: {missing}")

    standard_columns = get_table_columns(cur, "cars_standard")
    required_standard_columns = {
        "id",
        "brand_norm",
        "model_group_norm",
        "model_norm",
        "variant_norm",
    }
    missing_standard_columns = required_standard_columns - standard_columns
    if missing_standard_columns:
        missing = ", ".join(sorted(missing_standard_columns))
        raise RuntimeError(f"cars_standard is missing required columns: {missing}")


def insert_missing_cars_standard(
    table_name: str,
    sources: Sequence[str],
    dry_run: bool = False,
) -> Dict[str, object]:
    table_name = validate_table_name(table_name)
    sources = sorted(set(sources))
    if not sources:
        raise ValueError("At least one source is required")

    db_config = get_db_config()
    conn = None

    try:
        import psycopg2
        from psycopg2.extras import RealDictCursor

        conn = psycopg2.connect(**db_config)
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=RealDictCursor)

        ensure_required_schema(cur, table_name)

        logger.info("Database: %s", db_config["database"])
        logger.info("Source table: %s", table_name)
        logger.info("Sources: %s", ", ".join(sources))

        # Serialize ID allocation and duplicate checks for this operation.
        cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", ("cars_standard_insert_missing",))

        nullish_placeholders = ", ".join(["%s"] * len(NULLISH_VALUES))
        params: List[object] = [list(sources)]
        params.extend(NULLISH_VALUES)
        params.extend(NULLISH_VALUES)
        params.extend(NULLISH_VALUES)

        sql = f"""
            WITH source_rows AS (
                SELECT DISTINCT
                    UPPER(TRIM(brand::text)) AS brand_norm,
                    %s::varchar AS model_group_norm,
                    UPPER(TRIM(model::text)) AS model_norm,
                    UPPER(TRIM(variant::text)) AS variant_norm
                FROM {table_name}
                WHERE source = ANY(%s::text[])
                  AND cars_standard_id IS NULL
                  AND brand IS NOT NULL
                  AND model IS NOT NULL
                  AND variant IS NOT NULL
                  AND UPPER(TRIM(brand::text)) NOT IN ({nullish_placeholders})
                  AND UPPER(TRIM(model::text)) NOT IN ({nullish_placeholders})
                  AND UPPER(TRIM(variant::text)) NOT IN ({nullish_placeholders})
            ), existing_matches AS (
                SELECT source_rows.*
                FROM source_rows
                WHERE EXISTS (
                    SELECT 1
                    FROM cars_standard cs
                    WHERE UPPER(TRIM(cs.brand_norm::text)) = source_rows.brand_norm
                      AND UPPER(TRIM(cs.model_group_norm::text)) = source_rows.model_group_norm
                      AND UPPER(TRIM(cs.model_norm::text)) = source_rows.model_norm
                      AND UPPER(TRIM(cs.variant_norm::text)) = source_rows.variant_norm
                )
            ), new_rows AS (
                SELECT source_rows.*
                FROM source_rows
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM cars_standard cs
                    WHERE UPPER(TRIM(cs.brand_norm::text)) = source_rows.brand_norm
                      AND UPPER(TRIM(cs.model_group_norm::text)) = source_rows.model_group_norm
                      AND UPPER(TRIM(cs.model_norm::text)) = source_rows.model_norm
                      AND UPPER(TRIM(cs.variant_norm::text)) = source_rows.variant_norm
                )
            ), numbered_rows AS (
                SELECT
                    COALESCE((SELECT MAX(id) FROM cars_standard), 0)
                        + ROW_NUMBER() OVER (ORDER BY brand_norm, model_norm, variant_norm) AS id,
                    brand_norm,
                    model_group_norm,
                    model_norm,
                    variant_norm
                FROM new_rows
            )
            SELECT
                (SELECT COUNT(*) FROM source_rows) AS distinct_candidates,
                (SELECT COUNT(*) FROM existing_matches) AS already_exists,
                (SELECT COUNT(*) FROM numbered_rows) AS to_insert
        """
        cur.execute(sql, [MODEL_GROUP_DEFAULT, *params])
        preview = dict(cur.fetchone())

        if dry_run or preview["to_insert"] == 0:
            conn.rollback()
            return {
                "status": "dry_run" if dry_run else "success",
                "table_name": table_name,
                "sources": sources,
                "distinct_candidates": preview["distinct_candidates"],
                "already_exists": preview["already_exists"],
                "inserted": 0,
            }

        insert_sql = f"""
            WITH source_rows AS (
                SELECT DISTINCT
                    UPPER(TRIM(brand::text)) AS brand_norm,
                    %s::varchar AS model_group_norm,
                    UPPER(TRIM(model::text)) AS model_norm,
                    UPPER(TRIM(variant::text)) AS variant_norm
                FROM {table_name}
                WHERE source = ANY(%s::text[])
                  AND cars_standard_id IS NULL
                  AND brand IS NOT NULL
                  AND model IS NOT NULL
                  AND variant IS NOT NULL
                  AND UPPER(TRIM(brand::text)) NOT IN ({nullish_placeholders})
                  AND UPPER(TRIM(model::text)) NOT IN ({nullish_placeholders})
                  AND UPPER(TRIM(variant::text)) NOT IN ({nullish_placeholders})
            ), new_rows AS (
                SELECT source_rows.*
                FROM source_rows
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM cars_standard cs
                    WHERE UPPER(TRIM(cs.brand_norm::text)) = source_rows.brand_norm
                      AND UPPER(TRIM(cs.model_group_norm::text)) = source_rows.model_group_norm
                      AND UPPER(TRIM(cs.model_norm::text)) = source_rows.model_norm
                      AND UPPER(TRIM(cs.variant_norm::text)) = source_rows.variant_norm
                )
            ), numbered_rows AS (
                SELECT
                    COALESCE((SELECT MAX(id) FROM cars_standard), 0)
                        + ROW_NUMBER() OVER (ORDER BY brand_norm, model_norm, variant_norm) AS id,
                    brand_norm,
                    model_group_norm,
                    model_norm,
                    variant_norm
                FROM new_rows
            )
            INSERT INTO cars_standard (
                id,
                brand_norm,
                model_group_norm,
                model_norm,
                variant_norm
            )
            SELECT
                id,
                brand_norm,
                model_group_norm,
                model_norm,
                variant_norm
            FROM numbered_rows
            RETURNING id, brand_norm, model_group_norm, model_norm, variant_norm
        """
        cur.execute(insert_sql, [MODEL_GROUP_DEFAULT, *params])
        inserted_rows = cur.fetchall()
        conn.commit()

        return {
            "status": "success",
            "table_name": table_name,
            "sources": sources,
            "distinct_candidates": preview["distinct_candidates"],
            "already_exists": preview["already_exists"],
            "inserted": len(inserted_rows),
            "first_inserted_ids": [row["id"] for row in inserted_rows[:20]],
        }
    except Exception:
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()


def print_result(result: Dict[str, object]) -> None:
    print("\nInsert Missing Cars Standard")
    print("-" * 50)
    print(f"Status              : {result['status']}")
    print(f"Table               : {result['table_name']}")
    print(f"Sources             : {', '.join(result['sources'])}")
    print(f"Distinct candidates : {result['distinct_candidates']}")
    print(f"Already exists      : {result['already_exists']}")
    print(f"Inserted            : {result['inserted']}")
    if result.get("first_inserted_ids"):
        print("First inserted IDs  : " + ", ".join(map(str, result["first_inserted_ids"])))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Insert missing cars_standard rows from a dynamic unified table/source list."
    )
    parser.add_argument("--table", required=True, help="Source unified table, e.g. cars_unified or cars_unified_ind")
    parser.add_argument("--sources", required=True, help="Comma-separated source list, e.g. mudahmy,carlistmy")
    parser.add_argument("--dry-run", action="store_true", help="Only count candidates; do not insert")
    parser.add_argument(
        "--fill-after",
        action="store_true",
        help="After inserting cars_standard rows, run fill_cars_standard_id for the same table/sources",
    )
    parser.add_argument("--batch-size", type=int, default=500, help="Batch size for --fill-after")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    table_name = validate_table_name(args.table)
    sources = parse_sources(args.sources)

    result = insert_missing_cars_standard(
        table_name=table_name,
        sources=sources,
        dry_run=args.dry_run,
    )
    print_result(result)

    if args.fill_after:
        if args.dry_run:
            logger.info("Skipping --fill-after because --dry-run is enabled")
        elif fill_all_cars_standard_id is None:
            raise RuntimeError("fill_cars_standard_id.py is not available for --fill-after")
        else:
            fill_result = fill_all_cars_standard_id(
                sources=sources,
                table_name=table_name,
                batch_size=args.batch_size,
            )
            print("\nFill Cars Standard ID")
            print("-" * 50)
            for key, value in fill_result.items():
                print(f"{key}: {value}")


if __name__ == "__main__":
    main()
