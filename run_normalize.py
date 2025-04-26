import asyncio
from app.services import normalize_data_to_cars_normalize

async def main():
    print("🚀 Menjalankan proses normalisasi data ke tabel cars_normalize...")
    result = await normalize_data_to_cars_normalize()
    print("✅ Normalisasi selesai:", result)

if __name__ == "__main__":
    asyncio.run(main())
