"""Telegram Daily Scrape Summary (Unified + Archive DB)

Reads the same unified table used by the dashboard (default: TB_UNIFIED=cars_unified)
and sends a daily summary message to Telegram.

Usage:
  python commands/telegram_daily_report.py --dry-run
  python commands/telegram_daily_report.py                  # sends to Telegram
  python commands/telegram_daily_report.py --date 2026-01-26
  python commands/telegram_daily_report.py --sync-subscribers --broadcast
  python commands/telegram_daily_report.py --sync-subscribers --sync-only
  python commands/telegram_daily_report.py --handle-updates --reply-report --handle-only
  python commands/telegram_daily_report.py --handle-updates --reply-report --reply-start --poll

Required env (.env):
  DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME
  TB_UNIFIED (optional, default: cars_unified)
  TELEGRAM_BOT_TOKEN
  TELEGRAM_CHAT_ID (optional if using --broadcast)

Optional env:
  TELEGRAM_REPORT_SOURCES=mudahmy,carlistmy
  TB_UNIFIED_ARCHIVE (optional, default: <TB_UNIFIED>_archive)
  TB_CARSOME (optional, default: carsome)
  TELEGRAM_SUBSCRIBERS_TABLE (optional, default: public.telegram_subscribers)
  TELEGRAM_STATE_TABLE (optional, default: public.telegram_bot_state)

Notes:
  - "TOTAL" counts include both unified + archive tables when the archive table exists.
  - "UNIQUE" counts are based on DISTINCT listing_url.
    UNIQUE(today) excludes listing_url that already exist in the archive table (when present).
  - Today's counts match the dashboard KPI definition (status in active/sold and cars_standard_id is not null).
  - For on-demand replies, run with --handle-updates --reply-report, then send /report (or /report YYYY-MM-DD) to the bot.
"""

import argparse
import os
import re
import sys
from datetime import date, datetime
from pathlib import Path

import psycopg2
import requests
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def load_env() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    load_dotenv(dotenv_path=repo_root / ".env", override=True)


def validate_table_name(name: str) -> str:
    """Allow only `schema.table` or `table` with safe characters."""
    if not name:
        raise ValueError("Empty table name")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?", name):
        raise ValueError(f"Invalid table name: {name!r}")
    return name


def get_db_config() -> dict:
    return {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": int(os.getenv("DB_PORT", "5432")),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "dbname": os.getenv("DB_NAME"),
    }


def derive_archive_table_name(table: str) -> str:
    """Derive archive table name from base table name (e.g., cars_unified -> cars_unified_archive)."""
    table = validate_table_name(table)
    if "." in table:
        schema, base = table.split(".", 1)
        return f"{schema}.{base}_archive"
    return f"{table}_archive"


def table_exists(conn, table: str) -> bool:
    """Check if table exists in PostgreSQL (supports schema.table or table)."""
    table = validate_table_name(table)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT to_regclass(%s) AS regclass", (table,))
        row = cur.fetchone()
    return bool(row and row.get("regclass"))

def column_exists(conn, table: str, column: str) -> bool:
    """Check if column exists in a table (supports schema.table or table)."""
    table = validate_table_name(table)
    column = column.strip()
    if not column:
        return False

    if "." in table:
        schema, plain_table = table.split(".", 1)
    else:
        schema, plain_table = "public", table

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s
              AND column_name = %s
            LIMIT 1
            """,
            (schema, plain_table, column),
        )
        return cur.fetchone() is not None


def _build_union_source_sql(tables: list[str]) -> str:
    """Build a UNION ALL subquery selecting the minimal fields needed for metrics."""
    safe_tables = [validate_table_name(t) for t in tables]
    parts = [
        f"SELECT LOWER(source) AS source, listing_url, information_ads_date FROM {t}"
        for t in safe_tables
    ]
    return " UNION ALL ".join(parts)


def get_default_subscribers_table() -> str:
    return "public.telegram_subscribers"


def get_default_state_table() -> str:
    return "public.telegram_bot_state"


def ensure_telegram_tables(conn, subscribers_table: str, state_table: str) -> None:
    subscribers_table = validate_table_name(subscribers_table)
    state_table = validate_table_name(state_table)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {subscribers_table} (
                chat_id BIGINT PRIMARY KEY,
                chat_type TEXT NULL,
                username TEXT NULL,
                first_name TEXT NULL,
                last_name TEXT NULL,
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                last_seen_at TIMESTAMP NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {state_table} (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
    conn.commit()


def get_bot_state(conn, state_table: str, key: str) -> str | None:
    state_table = validate_table_name(state_table)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(f"SELECT value FROM {state_table} WHERE key = %s", (key,))
        row = cur.fetchone()
    return row["value"] if row else None


def set_bot_state(conn, state_table: str, key: str, value: str) -> None:
    state_table = validate_table_name(state_table)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {state_table} (key, value, updated_at)
            VALUES (%s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (key)
            DO UPDATE SET value = EXCLUDED.value, updated_at = CURRENT_TIMESTAMP
            """,
            (key, value),
        )
    conn.commit()


def upsert_subscriber(conn, subscribers_table: str, chat: dict, started: bool) -> None:
    subscribers_table = validate_table_name(subscribers_table)
    chat_id = chat.get("id")
    if chat_id is None:
        return

    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {subscribers_table} (
                chat_id, chat_type, username, first_name, last_name,
                is_active, started_at, last_seen_at, created_at, updated_at
            )
            VALUES (
                %s, %s, %s, %s, %s,
                TRUE,
                CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP
            )
            ON CONFLICT (chat_id)
            DO UPDATE SET
                chat_type = EXCLUDED.chat_type,
                username = EXCLUDED.username,
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                is_active = TRUE,
                last_seen_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                chat_id,
                chat.get("type"),
                chat.get("username"),
                chat.get("first_name"),
                chat.get("last_name"),
            ),
        )
    conn.commit()


def sync_subscribers_from_updates(conn, subscribers_table: str, state_table: str) -> int:
    """
    Pull latest Telegram updates and register chats that send /start (private chats).
    Stores last_update_id in state_table to avoid reprocessing.
    """
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN")

    subscribers_table = validate_table_name(subscribers_table)
    state_table = validate_table_name(state_table)

    last_update_id_value = get_bot_state(conn, state_table, "last_update_id")
    offset = None
    if last_update_id_value and str(last_update_id_value).isdigit():
        offset = int(last_update_id_value) + 1

    params: dict[str, object] = {"timeout": 0}
    if offset is not None:
        params["offset"] = offset

    url = f"https://api.telegram.org/bot{token}/getUpdates"
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    if not isinstance(payload, dict) or not payload.get("ok"):
        raise RuntimeError(f"Telegram getUpdates failed: {payload!r}")

    updates = payload.get("result") or []
    if not isinstance(updates, list):
        return 0

    registered = 0
    max_update_id: int | None = None
    for update in updates:
        if not isinstance(update, dict):
            continue
        update_id = update.get("update_id")
        if isinstance(update_id, int):
            max_update_id = update_id if max_update_id is None else max(max_update_id, update_id)

        message = update.get("message") or update.get("edited_message")
        if not isinstance(message, dict):
            continue
        chat = message.get("chat") or {}
        if not isinstance(chat, dict):
            continue

        # Only private chats for subscription
        if chat.get("type") != "private":
            continue

        text = message.get("text") or ""
        if not isinstance(text, str):
            continue
        if not text.strip().startswith("/start"):
            continue

        upsert_subscriber(conn, subscribers_table, chat=chat, started=True)
        registered += 1

    if max_update_id is not None:
        set_bot_state(conn, state_table, "last_update_id", str(max_update_id))

    return registered

def _parse_telegram_command(text: str) -> tuple[str | None, list[str]]:
    tokens = (text or "").strip().split()
    if not tokens:
        return None, []
    first = tokens[0]
    if not isinstance(first, str) or not first.startswith("/"):
        return None, []
    cmd = first.split("@", 1)[0].lower()
    return cmd, tokens[1:]


def handle_updates_from_telegram(
    conn,
    *,
    subscribers_table: str,
    state_table: str,
    table: str,
    sources: list[str],
    default_report_date: date,
    reply_start: bool,
    reply_report: bool,
    updates_timeout: int = 0,
) -> dict:
    """
    One-shot handler for incoming bot commands via getUpdates.

    - /start: registers subscriber (private chats only)
    - /report [YYYY-MM-DD]: sends the formatted report back to the requesting chat (private chats only)
    """
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN")

    subscribers_table = validate_table_name(subscribers_table)
    state_table = validate_table_name(state_table)

    last_update_id_value = get_bot_state(conn, state_table, "last_update_id")
    offset = None
    if last_update_id_value and str(last_update_id_value).isdigit():
        offset = int(last_update_id_value) + 1

    params: dict[str, object] = {"timeout": int(updates_timeout)}
    if offset is not None:
        params["offset"] = offset

    url = f"https://api.telegram.org/bot{token}/getUpdates"
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    if not isinstance(payload, dict) or not payload.get("ok"):
        raise RuntimeError(f"Telegram getUpdates failed: {payload!r}")

    updates = payload.get("result") or []
    if not isinstance(updates, list):
        return {"updates_seen": 0, "subscribers_synced": 0, "reports_sent": 0}

    subscribers_synced = 0
    reports_sent = 0
    max_update_id: int | None = None
    report_cache: dict[date, str] = {}

    def get_report_text(d: date) -> str:
        if d not in report_cache:
            rows, totals = fetch_metrics(conn, table=table, report_date=d, sources=sources)
            report_cache[d] = format_message(d, rows, totals)
        return report_cache[d]

    for update in updates:
        if not isinstance(update, dict):
            continue
        update_id = update.get("update_id")
        if isinstance(update_id, int):
            max_update_id = update_id if max_update_id is None else max(max_update_id, update_id)

        message = update.get("message") or update.get("edited_message")
        if not isinstance(message, dict):
            continue
        chat = message.get("chat") or {}
        if not isinstance(chat, dict):
            continue

        # Only private chats (consistent with subscriber design)
        if chat.get("type") != "private":
            continue

        text = message.get("text") or ""
        if not isinstance(text, str):
            continue

        cmd, args = _parse_telegram_command(text)
        if cmd not in {"/start", "/report"}:
            continue

        if cmd == "/start":
            upsert_subscriber(conn, subscribers_table, chat=chat, started=True)
            subscribers_synced += 1
            if reply_start:
                try:
                    send_telegram_message(
                        token,
                        int(chat["id"]),
                        "✅ Subscribed.\nType /report to get the latest report now.",
                    )
                except Exception as e:  # noqa: BLE001
                    print(f"[telegram] failed to reply /start: {e}", file=sys.stderr)
            continue

        # /report [YYYY-MM-DD]
        if cmd == "/report" and reply_report:
            requested_date = default_report_date
            if args:
                try:
                    requested_date = parse_date(args[0])
                except Exception:  # noqa: BLE001
                    try:
                        send_telegram_message(
                            token,
                            int(chat["id"]),
                            "Format salah. Pakai: /report atau /report YYYY-MM-DD",
                        )
                    except Exception as e:  # noqa: BLE001
                        print(f"[telegram] failed to reply invalid /report: {e}", file=sys.stderr)
                    continue

            upsert_subscriber(conn, subscribers_table, chat=chat, started=False)
            try:
                send_telegram_message(token, int(chat["id"]), get_report_text(requested_date))
                reports_sent += 1
            except Exception as e:  # noqa: BLE001
                print(f"[telegram] failed to reply /report: {e}", file=sys.stderr)

    if max_update_id is not None:
        set_bot_state(conn, state_table, "last_update_id", str(max_update_id))

    return {
        "updates_seen": len(updates),
        "subscribers_synced": subscribers_synced,
        "reports_sent": reports_sent,
    }

def build_carsome_select(
    conn,
    carsome_table: str,
    *,
    include_status: bool,
    include_cars_standard_id: bool,
) -> str:
    """
    Build SELECT statement that matches the dashboard view's Carsome mapping.
    We synthesize listing_url because the carsome table may not have it.
    """
    carsome_table = validate_table_name(carsome_table)
    has_reg_no = column_exists(conn, carsome_table, "reg_no")

    if has_reg_no:
        listing_expr = (
            "CONCAT('carsome-', COALESCE(NULLIF(TRIM(reg_no), ''), id::text))"
        )
    else:
        listing_expr = "CONCAT('carsome-', id::text)"

    select_parts = [
        "LOWER(source) AS source",
        f"{listing_expr} AS listing_url",
        "COALESCE(created_at::date, CURRENT_DATE) AS information_ads_date",
    ]
    if include_cars_standard_id:
        select_parts.append("cars_standard_id")
    if include_status:
        select_parts.append("status")

    select_sql = ", ".join(select_parts)
    return f"SELECT {select_sql} FROM {carsome_table}"


def fetch_metrics(conn, table: str, report_date: date, sources: list[str]) -> tuple[list[dict], dict]:
    table = validate_table_name(table)

    sources_norm = [s.strip().lower() for s in sources if s.strip()]
    if not sources_norm:
        raise ValueError("No sources provided")

    # Match dashboard "Today" KPI: only active/sold listings
    dashboard_statuses = ["active", "sold"]

    archive_table = validate_table_name(os.getenv("TB_UNIFIED_ARCHIVE", derive_archive_table_name(table)))
    include_archive = table_exists(conn, archive_table)

    carsome_table = validate_table_name(os.getenv("TB_CARSOME", "carsome"))
    include_carsome = table_exists(conn, carsome_table)

    # All-time dataset: cars_unified + cars_unified_archive (if exists) + carsome (if exists)
    all_union_tables = [table] + ([archive_table] if include_archive else [])
    all_union_sql = _build_union_source_sql(all_union_tables)
    if include_carsome:
        carsome_all_sql = build_carsome_select(
            conn,
            carsome_table,
            include_status=False,
            include_cars_standard_id=False,
        )
        all_union_sql = f"{all_union_sql} UNION ALL {carsome_all_sql}"

    # Today's dataset (to match dashboard): cars_unified + carsome (no archive) + status filter + require cars_standard_id
    today_union_sql = (
        f"SELECT LOWER(source) AS source, listing_url, information_ads_date, cars_standard_id, status "
        f"FROM {table}"
    )
    if include_carsome:
        carsome_today_sql = build_carsome_select(
            conn,
            carsome_table,
            include_status=True,
            include_cars_standard_id=True,
        )
        today_union_sql = f"{today_union_sql} UNION ALL {carsome_today_sql}"

    # Per-source counts (unified + archive when present)
    per_source_sql = f"""
        WITH
          today_data AS ({today_union_sql}),
          all_data AS ({all_union_sql})
        SELECT
          COALESCE(t.source, a.source) AS source,
          COALESCE(t.today_count, 0) AS today_count,
          COALESCE(a.all_time_count, 0) AS all_time_count
        FROM (
          SELECT
            source,
            COUNT(*) FILTER (
              WHERE information_ads_date = %s
                AND LOWER(status) = ANY(%s)
                AND cars_standard_id IS NOT NULL
            ) AS today_count
          FROM today_data
          WHERE source = ANY(%s)
          GROUP BY 1
        ) t
        FULL JOIN (
          SELECT
            source,
            COUNT(*) AS all_time_count
          FROM all_data
          WHERE source = ANY(%s)
          GROUP BY 1
        ) a
        USING (source)
        ORDER BY 1
    """

    # Overall totals + uniques across selected sources
    totals_sql = f"""
        SELECT
            COUNT(*) FILTER (WHERE information_ads_date = %s) AS total_today,
            COUNT(*) AS total_all,
            COUNT(DISTINCT listing_url) AS unique_all
        FROM ({all_union_sql}) t
        WHERE source = ANY(%s)
    """

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(per_source_sql, (report_date, dashboard_statuses, sources_norm, sources_norm))
        rows = cur.fetchall() or []

        cur.execute(totals_sql, (report_date, sources_norm))
        totals = cur.fetchone() or {"total_today": 0, "total_all": 0, "unique_all": 0}
        totals["_archive_included"] = include_archive

        # Total "today" should match dashboard: use today_data + status filter
        totals_today_sql = f"""
            WITH today_data AS ({today_union_sql})
            SELECT COUNT(*) AS total_today
            FROM today_data
            WHERE source = ANY(%s)
              AND information_ads_date = %s
              AND LOWER(status) = ANY(%s)
              AND cars_standard_id IS NOT NULL
        """
        cur.execute(totals_today_sql, (sources_norm, report_date, dashboard_statuses))
        totals_today_row = cur.fetchone() or {"total_today": 0}
        totals["total_today"] = totals_today_row.get("total_today", 0)

        if include_archive:
            unique_today_sql = f"""
                SELECT
                    COUNT(DISTINCT td.listing_url) AS unique_today
                FROM ({today_union_sql}) td
                WHERE td.source = ANY(%s)
                  AND td.information_ads_date = %s
                  AND LOWER(td.status) = ANY(%s)
                  AND td.cars_standard_id IS NOT NULL
                  AND NOT EXISTS (
                    SELECT 1
                    FROM {archive_table} ca
                    WHERE LOWER(ca.source) = td.source
                      AND ca.listing_url = td.listing_url
                  )
            """
            cur.execute(unique_today_sql, (sources_norm, report_date, dashboard_statuses))
        else:
            unique_today_sql = f"""
                SELECT
                    COUNT(DISTINCT td.listing_url) AS unique_today
                FROM ({today_union_sql}) td
                WHERE td.source = ANY(%s)
                  AND td.information_ads_date = %s
                  AND LOWER(td.status) = ANY(%s)
                  AND td.cars_standard_id IS NOT NULL
            """
            cur.execute(unique_today_sql, (sources_norm, report_date, dashboard_statuses))

        unique_today_row = cur.fetchone() or {"unique_today": 0}
        totals["unique_today"] = unique_today_row.get("unique_today", 0)

    found = {r["source"]: r for r in rows}
    ordered_rows: list[dict] = []
    for src in sources_norm:
        r = found.get(src)
        if not r:
            ordered_rows.append({"source": src, "today_count": 0, "all_time_count": 0})
        else:
            ordered_rows.append(r)

    return ordered_rows, totals


def format_message(report_date: date, rows: list[dict], totals: dict) -> str:
    lines: list[str] = [report_date.strftime("%d-%b-%Y"), ""]

    for r in rows:
        label = str(r["source"]).upper()
        lines.append(f"{label} : {int(r['today_count'])} / {int(r['all_time_count'])} record/s")

    lines.append("")
    lines.append(f"TOTAL : {int(totals['total_today'])} / {int(totals['total_all'])} record/s")
    lines.append(f"UNIQUE : {int(totals['unique_today'])} / {int(totals['unique_all'])} record/s")
    return "\n".join(lines)


def get_subscriber_chat_ids(conn, subscribers_table: str) -> list[int]:
    subscribers_table = validate_table_name(subscribers_table)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            f"""
            SELECT chat_id
            FROM {subscribers_table}
            WHERE is_active = TRUE
            ORDER BY chat_id
            """
        )
        rows = cur.fetchall() or []
    return [int(r["chat_id"]) for r in rows if r.get("chat_id") is not None]


def send_telegram_message(token: str, chat_id: int, text: str) -> None:
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    resp = requests.post(
        url,
        data={
            "chat_id": str(chat_id),
            "text": text,
            "disable_web_page_preview": True,
        },
        timeout=30,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"Telegram sendMessage failed: chat_id={chat_id} HTTP {resp.status_code}: {resp.text}")


def send_telegram(text: str, *, chat_ids: list[int] | None = None) -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN")

    if chat_ids is None:
        chat_id = os.getenv("TELEGRAM_CHAT_ID")
        if not chat_id:
            raise RuntimeError("Missing TELEGRAM_CHAT_ID (or use --broadcast)")
        send_telegram_message(token, int(chat_id), text)
        return

    if not chat_ids:
        raise RuntimeError("No subscribers found to broadcast to")
    for chat_id in chat_ids:
        send_telegram_message(token, int(chat_id), text)


def main(argv: list[str] | None = None) -> int:
    load_env()

    parser = argparse.ArgumentParser(description="Send unified scrape daily summary to Telegram.")
    parser.add_argument("--date", dest="date_str", help="Report date in YYYY-MM-DD (default: today).")
    parser.add_argument("--dry-run", action="store_true", help="Print message only; do not send to Telegram.")
    parser.add_argument("--broadcast", action="store_true", help="Send to all subscribers who have /start the bot.")
    parser.add_argument("--sync-subscribers", action="store_true", help="Fetch /start subscribers from Telegram getUpdates.")
    parser.add_argument("--sync-only", action="store_true", help="Only sync subscribers, then exit.")
    parser.add_argument(
        "--handle-updates",
        action="store_true",
        help="Process Telegram updates and handle /start + /report (one-shot).",
    )
    parser.add_argument(
        "--handle-only",
        action="store_true",
        help="Only handle Telegram updates (requires --handle-updates), then exit.",
    )
    parser.add_argument(
        "--reply-start",
        action="store_true",
        help="When handling updates, reply to /start with a confirmation message.",
    )
    parser.add_argument(
        "--reply-report",
        action="store_true",
        help="When handling updates, reply to /report by sending the report to the requesting chat.",
    )
    parser.add_argument(
        "--poll",
        action="store_true",
        help="Run continuously (long-poll getUpdates) to respond to /start and /report instantly.",
    )
    parser.add_argument(
        "--long-poll-timeout",
        type=int,
        default=25,
        help="Seconds for Telegram long polling (only used with --poll).",
    )

    args = parser.parse_args(argv)
    if args.handle_only and not args.handle_updates:
        parser.error("--handle-only requires --handle-updates")
    if (args.reply_start or args.reply_report) and not args.handle_updates:
        parser.error("--reply-start/--reply-report require --handle-updates")
    if args.poll and not args.handle_updates:
        parser.error("--poll requires --handle-updates")
    report_date = parse_date(args.date_str) if args.date_str else date.today()

    table = os.getenv("TB_UNIFIED", "cars_unified")
    sources_env = os.getenv("TELEGRAM_REPORT_SOURCES", "mudahmy,carlistmy")
    sources = [s.strip() for s in sources_env.split(",") if s.strip()]

    db_config = get_db_config()
    if not db_config.get("user") or not db_config.get("password") or not db_config.get("dbname"):
        print("Missing DB_USER/DB_PASSWORD/DB_NAME in environment/.env", file=sys.stderr)
        return 2

    subscribers_table = os.getenv("TELEGRAM_SUBSCRIBERS_TABLE", get_default_subscribers_table())
    state_table = os.getenv("TELEGRAM_STATE_TABLE", get_default_state_table())

    if args.poll:
        if args.handle_only is False:
            parser.error("--poll is intended to be used with --handle-only (so it does not broadcast)")

        try:
            while True:
                conn = psycopg2.connect(**db_config)
                try:
                    ensure_telegram_tables(conn, subscribers_table=subscribers_table, state_table=state_table)
                    stats = handle_updates_from_telegram(
                        conn,
                        subscribers_table=subscribers_table,
                        state_table=state_table,
                        table=table,
                        sources=sources,
                        default_report_date=report_date,
                        reply_start=args.reply_start,
                        reply_report=args.reply_report,
                        updates_timeout=max(0, int(args.long_poll_timeout)),
                    )
                finally:
                    conn.close()

                if args.dry_run or stats.get("reports_sent") or stats.get("subscribers_synced"):
                    print(f"[telegram] handled updates: {stats}", file=sys.stderr)
        except KeyboardInterrupt:
            return 0

    conn = psycopg2.connect(**db_config)
    try:
        if args.sync_subscribers or args.broadcast or args.handle_updates:
            ensure_telegram_tables(conn, subscribers_table=subscribers_table, state_table=state_table)

        if args.sync_subscribers:
            synced = sync_subscribers_from_updates(conn, subscribers_table=subscribers_table, state_table=state_table)
            if args.dry_run or args.sync_only:
                print(f"[telegram] synced subscribers: {synced}", file=sys.stderr)
            if args.sync_only:
                return 0

        if args.handle_updates:
            stats = handle_updates_from_telegram(
                conn,
                subscribers_table=subscribers_table,
                state_table=state_table,
                table=table,
                sources=sources,
                default_report_date=report_date,
                reply_start=args.reply_start,
                reply_report=args.reply_report,
            )
            if args.dry_run:
                print(f"[telegram] handled updates: {stats}", file=sys.stderr)
            if args.handle_only:
                return 0

        rows, totals = fetch_metrics(conn, table=table, report_date=report_date, sources=sources)
    finally:
        conn.close()

    message = format_message(report_date, rows, totals)
    if args.dry_run:
        print(message)
        return 0

    if args.broadcast:
        conn = psycopg2.connect(**db_config)
        try:
            ensure_telegram_tables(conn, subscribers_table=subscribers_table, state_table=state_table)
            chat_ids = get_subscriber_chat_ids(conn, subscribers_table=subscribers_table)
        finally:
            conn.close()
        send_telegram(message, chat_ids=chat_ids)
    else:
        send_telegram(message)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
