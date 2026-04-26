import asyncio
import os
from datetime import datetime
from typing import AsyncGenerator
from uuid import UUID, uuid4

from sqlalchemy import (
    BigInteger, Boolean, Column, DateTime, Float, Integer,
    Numeric, String, Text, func, text
)
from sqlalchemy.dialects.postgresql import JSONB, UUID as PG_UUID
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://kawkaw:secret@db:5432/kawkaw")

async_engine = create_async_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
    echo=False,
)

AsyncSessionLocal = async_sessionmaker(
    async_engine,
    class_=AsyncSession,
    expire_on_commit=False,
)

# Sync engine for use inside scanner threads
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

_sync_url = DATABASE_URL.replace("postgresql+asyncpg://", "postgresql+psycopg2://", 1)
try:
    import psycopg2  # noqa: F401
    sync_engine = create_engine(_sync_url, pool_pre_ping=True, pool_size=3, max_overflow=5)
except ImportError:
    # Fall back to asyncpg-based URL for environments without psycopg2
    # Scanner will use async approach instead
    sync_engine = None

SyncSessionLocal = sessionmaker(bind=sync_engine, autocommit=False, autoflush=False) if sync_engine else None


class Base(DeclarativeBase):
    pass


class Image(Base):
    __tablename__ = "images"

    id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)
    file_path: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    file_name: Mapped[str] = mapped_column(Text, nullable=False)
    file_extension: Mapped[str] = mapped_column(Text, nullable=False)
    file_size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    file_mtime: Mapped[float] = mapped_column(Float, nullable=False)

    duplicate_id: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    duplicate_id_method: Mapped[str] = mapped_column(Text, nullable=False)

    camera_make: Mapped[str | None] = mapped_column(Text, nullable=True)
    camera_model: Mapped[str | None] = mapped_column(Text, nullable=True, index=True)
    lens_model: Mapped[str | None] = mapped_column(Text, nullable=True)
    date_taken: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    shutter_count: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    image_unique_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    f_number: Mapped[float | None] = mapped_column(Numeric(6, 2), nullable=True)
    exposure_time: Mapped[str | None] = mapped_column(Text, nullable=True)
    iso: Mapped[int | None] = mapped_column(Integer, nullable=True)
    gps_lat: Mapped[float | None] = mapped_column(Float, nullable=True)
    gps_lon: Mapped[float | None] = mapped_column(Float, nullable=True)

    exif_json: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    is_deleted: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, index=True)
    is_marked: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, index=True)
    trash_moved: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    trash_path: Mapped[str | None] = mapped_column(Text, nullable=True)

    first_seen: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    last_scanned: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())


class ScanRun(Base):
    __tablename__ = "scan_runs"

    id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    scan_type: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False, default="running")
    files_scanned: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    files_new: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    files_updated: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    files_skipped: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    files_deleted: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    log_lines: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


async def create_all_tables() -> None:
    for attempt in range(10):
        try:
            async with async_engine.begin() as conn:
                await conn.execute(text('CREATE EXTENSION IF NOT EXISTS "pgcrypto"'))
                await conn.run_sync(Base.metadata.create_all)
                # Create GIN index on exif_json manually if not exists
                await conn.execute(text(
                    "CREATE INDEX IF NOT EXISTS idx_images_exif_gin ON images USING GIN (exif_json)"
                ))
                # Migration: widen shutter_count from INTEGER to BIGINT if needed
                # (Canon shutter counts can exceed 2^31 on high-cycle bodies)
                await conn.execute(text("""
                    DO $$ BEGIN
                        IF EXISTS (
                            SELECT 1 FROM information_schema.columns
                            WHERE table_name = 'images'
                              AND column_name = 'shutter_count'
                              AND data_type = 'integer'
                        ) THEN
                            ALTER TABLE images ALTER COLUMN shutter_count TYPE BIGINT;
                        END IF;
                    END $$;
                """))
            return
        except Exception as e:
            if attempt == 9:
                raise RuntimeError(f"Failed to connect to database after 10 attempts: {e}") from e
            wait = 2 ** attempt
            await asyncio.sleep(wait)
