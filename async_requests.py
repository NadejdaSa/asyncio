import asyncio
import datetime
import aiohttp
import ssl
from more_itertools import chunked

from models import SwapiPeople, Session, init_orm, close_orm

MAX_REQUESTS = 10

def extract_id_from_url(url: str) -> int:
    return int(url.rstrip("/").split("/")[-1])

async def get_name(session, url):
    if not url:
        return ''
    async with session.get(url) as resp:
        data = await resp.json()
        return data.get("name") or data.get("title", "")
    
async def get_names(session, urls):
    if not urls:
        return ""
    names = await asyncio.gather(*(get_name(session, url) for url in urls))
    return ", ".join(names)

async def get_person(person_id, session):
    async with session.get(f"https://swapi.dev/api/people/{person_id}/") as response:
        if response.status != 200:
            return None
        data = await response.json()
        homeworld = await get_name(session, data.get("homeworld"))
        films = await get_names(session, data.get("films"))
        species = await get_names(session, data.get("species"))
        starships = await get_names(session, data.get("starships"))
        vehicles = await get_names(session, data.get("vehicles"))
        return SwapiPeople(
            id=extract_id_from_url(data["url"]),
            name=data.get("name"),
            birth_year=data.get("birth_year"),
            eye_color=data.get("eye_color"),
            gender=data.get("gender"),
            hair_color=data.get("hair_color"),
            height=data.get("height"),
            mass=data.get("mass"),
            skin_color=data.get("skin_color"),
            homeworld=homeworld,
            films=films,
            species=species,
            starships=starships,
            vehicles=vehicles,
        )

async def insert_people(people_objects):
    async with Session() as session:
        session.add_all(people_objects)
        await session.commit()

async def main():
    await init_orm()

    sslcontext = ssl.create_default_context()
    sslcontext.check_hostname = False
    sslcontext.verify_mode = ssl.CERT_NONE

    connector = aiohttp.TCPConnector(ssl=sslcontext)

    async with aiohttp.ClientSession(connector=connector) as http_session:
        tasks = []

        for chunk in chunked(range(1, 84), MAX_REQUESTS):
            coro = [get_person(pid, http_session) for pid in chunk]
            results = await asyncio.gather(*coro)
            results = [r for r in results if r]
            task = asyncio.create_task(insert_people(results))
            tasks.append(task)

        await asyncio.gather(*tasks)

    await close_orm()

start = datetime.datetime.now()
asyncio.run(main())
print(datetime.datetime.now() - start)
