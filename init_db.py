import asyncio
from models import init_orm, close_orm

async def main():
    await init_orm()
    await close_orm()

asyncio.run(main())