import os

from sqlalchemy import Integer, String
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncAttrs
from sqlalchemy.orm import DeclarativeBase,Mapped, mapped_column


POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "swapi")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DSN = f"postgresql+asyncpg://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

engine = create_async_engine(POSTGRES_DSN)
Session = async_sessionmaker(bind=engine, expire_on_commit=False)

class Base(DeclarativeBase, AsyncAttrs):
    pass

class SwapiPeople(Base):
    __tablename__ = 'swapi_people'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String)
    birth_year: Mapped[str] = mapped_column(String, nullable=True)
    eye_color: Mapped[str] = mapped_column(String, nullable=True)
    gender: Mapped[str] = mapped_column(String, nullable=True)
    hair_color: Mapped[str] = mapped_column(String, nullable=True)
    height: Mapped[str] = mapped_column(String, nullable=True)
    mass: Mapped[str] = mapped_column(String, nullable=True)
    skin_color: Mapped[str] = mapped_column(String, nullable=True)
    homeworld: Mapped[str] = mapped_column(String, nullable=True)
    films: Mapped[str] = mapped_column(String, nullable=True)     
    species: Mapped[str] = mapped_column(String, nullable=True)   
    starships: Mapped[str] = mapped_column(String, nullable=True)  
    vehicles: Mapped[str] = mapped_column(String, nullable=True)

async def init_orm():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

async def close_orm():
    await engine.dispose()
