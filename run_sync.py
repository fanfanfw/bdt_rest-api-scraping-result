import asyncio
from app.services import sync_data_from_remote

async def main():
    print("🚀 Menjalankan proses sinkronisasi data...")
    result = await sync_data_from_remote()
    print("✅ Sinkronisasi selesai.\n")

    print("📊 Ringkasan Hasil Sinkronisasi:")
    for source, summary in result.items():
        if source == "status":
            continue
        print(f"  - {source.upper()}:")
        print(f"    Total fetched : {summary['total_fetched']}")
        print(f"    Inserted      : {summary['inserted']}")
        print(f"    Skipped       : {summary['skipped']}")

if __name__ == "__main__":
    asyncio.run(main())
