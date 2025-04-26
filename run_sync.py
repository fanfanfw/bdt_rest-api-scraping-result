import asyncio
from app.services import sync_data_from_remote

async def main():
    print("🚀 Menjalankan proses sinkronisasi data...")
    result = await sync_data_from_remote()
    print("✅ Sinkronisasi selesai:", result)

if __name__ == "__main__":
    asyncio.run(main())
