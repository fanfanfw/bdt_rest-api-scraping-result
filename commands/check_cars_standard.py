"""
CLI helper untuk membandingkan & menyinkronkan tabel cars_standard antara DB lokal dan remote.

examples:
    python commands/check_cars_standard.py --max-differences 50
    python commands/check_cars_standard.py --sync-direction remote-to-local
    python commands/check_cars_standard.py --sync-direction local-to-remote --ids 10,42
    python commands/check_cars_standard.py --sync-direction remote-to-local --sync-all
"""

import argparse
import asyncio
import json
import sys
from datetime import datetime
from pathlib import Path

from typing import List, Optional

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.append(str(BASE_DIR))

load_dotenv(override=True)

from app.services import (  # noqa: E402
    compare_cars_standard_tables,
    sync_cars_standard_tables,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Bandingkan tabel cars_standard lokal vs remote dan opsional sinkronkan datanya."
    )
    parser.add_argument(
        "--max-differences",
        type=int,
        default=100,
        help="Batas maksimum perbedaan kolom yang ditampilkan (default: 100).",
    )
    parser.add_argument(
        "--sync-direction",
        choices=["remote-to-local", "local-to-remote"],
        help="Jika diisi, data dari sumber akan menimpa target (remote-to-local atau sebaliknya).",
    )
    parser.add_argument(
        "--sync-all",
        action="store_true",
        help="Saat sinkronisasi, salin semua baris dari sumber (abaikan deteksi mismatch).",
    )
    parser.add_argument(
        "--ids",
        type=str,
        help="Batasi sinkronisasi ke ID tertentu. Pisahkan dengan koma. Berlaku jika --sync-direction dipakai.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Cetak hasil dalam format JSON mentah.",
    )
    return parser.parse_args()


def parse_id_list(raw_ids: Optional[str]) -> Optional[List[int]]:
    if not raw_ids:
        return None
    cleaned = []
    for token in raw_ids.split(","):
        token = token.strip()
        if not token:
            continue
        try:
            cleaned.append(int(token))
        except ValueError as exc:
            raise ValueError(f"ID tidak valid: {token}") from exc
    return cleaned or None


async def run_operations(
    max_differences: int,
    sync_direction: Optional[str],
    ids: Optional[List[int]],
    sync_all: bool,
):
    comparison = await compare_cars_standard_tables(max_differences=max_differences)
    sync_result = None

    if sync_direction:
        sync_result = await sync_cars_standard_tables(
            direction=sync_direction,
            ids=ids,
            sync_all=sync_all,
        )

    return comparison, sync_result


def print_comparison(data: dict) -> None:
    checked_at = data.get("checked_at")
    if isinstance(checked_at, str):
        timestamp = checked_at
    elif isinstance(checked_at, datetime):
        timestamp = checked_at.isoformat()
    else:
        timestamp = str(checked_at)

    print(f"Status          : {data['status']}")
    print(f"Diperiksa pada  : {timestamp}")
    print(f"Jumlah lokal    : {data['local_count']}")
    print(f"Jumlah remote   : {data['remote_count']}")
    print(f"Hilang di lokal : {len(data['missing_in_local'])} baris")
    print(f"Hilang di remote: {len(data['missing_in_remote'])} baris")
    print("")

    if data["missing_in_local"]:
        print("ID yang ada di remote tapi hilang di lokal:")
        print(", ".join(map(str, data["missing_in_local"])))
        print("")

    if data["missing_in_remote"]:
        print("ID yang ada di lokal tapi hilang di remote:")
        print(", ".join(map(str, data["missing_in_remote"])))
        print("")

    differences = data["differences"]
    if not differences:
        print("Tidak ada perbedaan kolom pada ID yang sama.")
        return

    print(f"Perbedaan kolom yang ditampilkan: {len(differences)} baris")
    for diff in differences:
        print(
            f"- ID {diff['id']} kolom '{diff['column']}': "
            f"lokal={diff['local_value']} | remote={diff['remote_value']}"
        )


def print_sync_result(data: dict) -> None:
    print("\n=== Sinkronisasi Dijalankan ===")
    print(f"Arah sinkron   : {data['direction']}")
    print(f"Tabel          : {data['table']}")
    print(f"Total disalin  : {data['total_synced']}")

    if not data["synced_ids"]:
        print("Tidak ada baris yang diubah.")
    else:
        print(f"ID yang disinkronkan: {', '.join(map(str, data['synced_ids']))}")

    if data["missing_in_source"]:
        print(
            "ID berikut tidak ditemukan di sumber sehingga dilewati: "
            + ", ".join(map(str, data["missing_in_source"]))
        )


def main() -> None:
    args = parse_args()
    try:
        ids = parse_id_list(args.ids)
    except ValueError as exc:
        print(f"Error input: {exc}", file=sys.stderr)
        sys.exit(2)

    comparison, sync_result = asyncio.run(
        run_operations(
            max_differences=args.max_differences,
            sync_direction=args.sync_direction,
            ids=ids,
            sync_all=args.sync_all,
        )
    )
    comparison_data = comparison.dict()
    sync_data = sync_result.dict() if sync_result else None

    if args.json:
        payload = {
            "comparison": comparison_data,
            "sync": sync_data,
        }
        print(json.dumps(payload, default=str, indent=2))
        return

    print_comparison(comparison_data)
    if sync_data:
        print_sync_result(sync_data)


if __name__ == "__main__":
    main()
