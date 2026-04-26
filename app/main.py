import asyncio
import json
import logging
import os
import queue
import threading
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncGenerator

from fastapi import Depends, FastAPI, HTTPException, Query, Response
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from .crud import (
    create_scan_run,
    export_duplicates_csv,
    get_all_files,
    get_camera_models,
    get_dashboard_stats,
    get_duplicate_groups,
    get_extensions,
    get_image_exif,
    get_scan_history,
    get_scan_run,
    get_scan_run_logs,
    ignore_group,
    mark_image,
    move_to_trash,
    undelete_image,
    unignore_group,
)
from .database import AsyncSessionLocal, SyncSessionLocal, create_all_tables
from .scanner import run_scan_sync
from .scheduler import reschedule_job, start_scheduler, stop_scheduler, trigger_immediate_scan

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)

DATA_DIR = Path("/app/data")
SETTINGS_FILE = DATA_DIR / "settings.json"
STATIC_DIR = Path("/app/static")
TEMPLATES_DIR = Path(__file__).parent / "templates"

# ─── Settings ────────────────────────────────────────────────────────────────

# Container-side mount paths that correspond to MOUNT_1..5 and MOUNT_TRASH env vars
MOUNT_SLOTS = {
    "vol1": "/scans/vol1",
    "vol2": "/scans/vol2",
    "vol3": "/scans/vol3",
    "vol4": "/scans/vol4",
    "vol5": "/scans/vol5",
    "trash": "/scans/trash",
}


def _load_settings() -> dict:
    defaults = {
        "scan_folders": [],
        "trash_folder": "/scans/trash",
        "scan_schedule": "02:30",
        "timezone": "UTC",
    }
    if SETTINGS_FILE.exists():
        try:
            saved = json.loads(SETTINGS_FILE.read_text())
            # Overlay saved values, preserving type (skip null/empty lists selectively)
            for k, v in saved.items():
                if v is not None and v != "":
                    defaults[k] = v
        except Exception as e:
            logger.warning(f"Could not read settings file: {e}")
    return defaults


def _save_settings(settings: dict) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    SETTINGS_FILE.write_text(json.dumps(settings, indent=2))


_settings = _load_settings()

# ─── Scan state ──────────────────────────────────────────────────────────────

_scan_lock = threading.Lock()
_current_scan_id: str | None = None
_scan_log_queue: queue.Queue = queue.Queue()
_cancel_event = threading.Event()
_scan_running = threading.Event()


def _get_sync_session():
    if SyncSessionLocal is None:
        raise RuntimeError("Sync database session factory not available (psycopg2 not installed)")
    return SyncSessionLocal()


def _generate_thumbnail_sync(file_path: str, size: int = 300) -> bytes | None:
    import io
    clamped = min(max(size, 64), 1200)
    try:
        from PIL import Image as PILImage
        with PILImage.open(file_path) as img:
            img.thumbnail((clamped, clamped), PILImage.LANCZOS)
            buf = io.BytesIO()
            img.convert("RGB").save(buf, format="JPEG", quality=78, optimize=True)
            return buf.getvalue()
    except Exception:
        pass
    try:
        import exifread
        with open(file_path, "rb") as f:
            tags = exifread.process_file(f, details=True)
        thumb = tags.get("JPEGThumbnail")
        if isinstance(thumb, bytes) and len(thumb) > 200:
            if clamped < 300:
                from PIL import Image as PILImage
                with PILImage.open(io.BytesIO(thumb)) as img:
                    img.thumbnail((clamped, clamped), PILImage.LANCZOS)
                    buf = io.BytesIO()
                    img.convert("RGB").save(buf, format="JPEG", quality=78)
                    return buf.getvalue()
            return thumb
    except Exception:
        pass
    return None


def _do_scan(scan_run_id: str, scan_type: str) -> None:
    global _current_scan_id
    _scan_running.set()
    try:
        run_scan_sync(
            scan_run_id=scan_run_id,
            scan_type=scan_type,
            scan_folders=_settings["scan_folders"],
            trash_folder=_settings["trash_folder"],
            sync_session_factory=_get_sync_session,
            log_queue=_scan_log_queue,
            cancel_event=_cancel_event,
        )
    except Exception as e:
        logger.exception(f"Scan {scan_run_id} failed: {e}")
        _scan_log_queue.put(f"[ERROR] Scan failed: {e}")
    finally:
        _scan_lock.release()
        _scan_running.clear()
        _current_scan_id = None
        _cancel_event.clear()


def _scheduled_scan_job() -> None:
    global _current_scan_id
    if not _scan_lock.acquire(blocking=False):
        logger.info("Scheduled scan skipped — another scan is already running")
        return

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        scan_run_id = loop.run_until_complete(_create_scan_run_async("scheduled"))
    finally:
        loop.close()

    _current_scan_id = scan_run_id
    _cancel_event.clear()
    _do_scan(scan_run_id, "scheduled")


async def _create_scan_run_async(scan_type: str) -> str:
    async with AsyncSessionLocal() as db:
        try:
            scan_id = await create_scan_run(db, scan_type)
            await db.commit()
            return scan_id
        except Exception:
            await db.rollback()
            raise


# ─── Lifespan ─────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _settings
    _settings = _load_settings()
    await create_all_tables()
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    start_scheduler(_settings["scan_schedule"], _scheduled_scan_job, tz=_settings.get("timezone", "UTC"))
    logger.info("KawKaw Anti Duplicate started")
    yield
    stop_scheduler()
    logger.info("KawKaw Anti Duplicate stopped")


# ─── App ──────────────────────────────────────────────────────────────────────

app = FastAPI(title="KawKaw Anti Duplicate", lifespan=lifespan)

if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


# ─── UI ───────────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def index():
    html_path = TEMPLATES_DIR / "index.html"
    if html_path.exists():
        return HTMLResponse(content=html_path.read_text())
    return HTMLResponse(content="<h1>KawKaw Anti Duplicate</h1><p>UI not found.</p>")


# ─── Dashboard ────────────────────────────────────────────────────────────────

@app.get("/api/dashboard")
async def api_dashboard(db: AsyncSession = Depends(get_db)):
    return await get_dashboard_stats(db)


# ─── Duplicates ───────────────────────────────────────────────────────────────

@app.get("/api/duplicates")
async def api_duplicates(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    include_ignored: bool = Query(False),
    db: AsyncSession = Depends(get_db),
):
    return await get_duplicate_groups(db, page=page, page_size=page_size, include_ignored=include_ignored)


@app.post("/api/groups/{duplicate_id}/ignore")
async def api_ignore_group(duplicate_id: str, db: AsyncSession = Depends(get_db)):
    return await ignore_group(db, duplicate_id)


@app.post("/api/groups/{duplicate_id}/unignore")
async def api_unignore_group(duplicate_id: str, db: AsyncSession = Depends(get_db)):
    return await unignore_group(db, duplicate_id)


# ─── Files ────────────────────────────────────────────────────────────────────

@app.get("/api/files")
async def api_files(
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=500),
    search: str | None = Query(None),
    camera_model: str | None = Query(None),
    extension: str | None = Query(None),
    date_from: str | None = Query(None),
    date_to: str | None = Query(None),
    is_marked: bool | None = Query(None),
    is_deleted: bool | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    return await get_all_files(
        db,
        page=page,
        page_size=page_size,
        search=search,
        camera_model=camera_model,
        extension=extension,
        date_from=date_from,
        date_to=date_to,
        is_marked=is_marked,
        is_deleted=is_deleted,
    )


@app.get("/api/files/{image_id}/exif")
async def api_file_exif(image_id: str, db: AsyncSession = Depends(get_db)):
    data = await get_image_exif(db, image_id)
    if data is None:
        raise HTTPException(status_code=404, detail="Image not found")
    return data


class MarkRequest(BaseModel):
    marked: bool


@app.post("/api/files/{image_id}/mark")
async def api_mark_image(image_id: str, body: MarkRequest, db: AsyncSession = Depends(get_db)):
    result = await mark_image(db, image_id, body.marked)
    if result is None:
        raise HTTPException(status_code=404, detail="Image not found")
    return result


@app.post("/api/files/{image_id}/trash")
async def api_trash_image(image_id: str, db: AsyncSession = Depends(get_db)):
    trash_folder = _settings.get("trash_folder", "/mnt/photos/.trash")
    try:
        result = await move_to_trash(db, image_id, trash_folder)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    if result is None:
        raise HTTPException(status_code=404, detail="Image not found")
    return result


@app.post("/api/files/{image_id}/undelete")
async def api_undelete_image(image_id: str, db: AsyncSession = Depends(get_db)):
    result = await undelete_image(db, image_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Image not found")
    return result


@app.get("/api/files/{image_id}/thumbnail")
async def api_thumbnail(
    image_id: str,
    size: int = Query(300, ge=64, le=1200),
    db: AsyncSession = Depends(get_db),
):
    from sqlalchemy import select
    from .database import Image as DBImage

    result = await db.execute(
        select(DBImage.file_path).where(
            DBImage.id == image_id,
            DBImage.is_deleted == False,
        )
    )
    row = result.first()
    if not row:
        raise HTTPException(status_code=404, detail="Image not found")

    file_path = row[0]
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not on disk")

    loop = asyncio.get_event_loop()
    thumb_data = await loop.run_in_executor(None, _generate_thumbnail_sync, file_path, size)
    if not thumb_data:
        raise HTTPException(status_code=404, detail="Could not generate thumbnail")

    return Response(
        content=thumb_data,
        media_type="image/jpeg",
        headers={"Cache-Control": "public, max-age=3600"},
    )


@app.get("/api/filters/cameras")
async def api_camera_models(db: AsyncSession = Depends(get_db)):
    return await get_camera_models(db)


@app.get("/api/filters/extensions")
async def api_extensions(db: AsyncSession = Depends(get_db)):
    return await get_extensions(db)


# ─── Scan ─────────────────────────────────────────────────────────────────────

class ScanStartRequest(BaseModel):
    scan_type: str = "manual"


@app.post("/api/scan/start")
async def api_scan_start(body: ScanStartRequest = ScanStartRequest()):
    global _current_scan_id
    if not _scan_lock.acquire(blocking=False):
        raise HTTPException(status_code=409, detail="A scan is already running")

    scan_type = body.scan_type if body.scan_type in ("manual", "deleted_check") else "manual"

    try:
        scan_run_id = await _create_scan_run_async(scan_type)
    except Exception as e:
        _scan_lock.release()
        raise HTTPException(status_code=500, detail=str(e))

    _current_scan_id = scan_run_id
    _cancel_event.clear()

    thread = threading.Thread(
        target=_do_scan,
        args=(scan_run_id, scan_type),
        daemon=True,
        name=f"scan-{scan_run_id[:8]}",
    )
    thread.start()

    return {"scan_run_id": scan_run_id, "status": "started", "scan_type": scan_type}


@app.post("/api/scan/deleted-check")
async def api_deleted_check():
    global _current_scan_id
    if not _scan_lock.acquire(blocking=False):
        raise HTTPException(status_code=409, detail="A scan is already running")

    try:
        scan_run_id = await _create_scan_run_async("deleted_check")
    except Exception as e:
        _scan_lock.release()
        raise HTTPException(status_code=500, detail=str(e))

    _current_scan_id = scan_run_id
    _cancel_event.clear()

    thread = threading.Thread(
        target=_do_scan,
        args=(scan_run_id, "deleted_check"),
        daemon=True,
        name=f"scan-del-{scan_run_id[:8]}",
    )
    thread.start()

    return {"scan_run_id": scan_run_id, "status": "started", "scan_type": "deleted_check"}


@app.post("/api/scan/cancel")
async def api_scan_cancel():
    if not _scan_running.is_set():
        raise HTTPException(status_code=400, detail="No scan running")
    _cancel_event.set()
    return {"status": "cancel_requested"}


@app.get("/api/scan/status")
async def api_scan_status(db: AsyncSession = Depends(get_db)):
    running = _scan_running.is_set()
    sid = _current_scan_id
    result = {"running": running, "scan_run_id": sid}
    if sid:
        run = await get_scan_run(db, sid)
        if run:
            result["scan_run"] = run
    return result


@app.get("/api/scan/stream")
async def api_scan_stream():
    async def event_generator():
        while True:
            try:
                line = await asyncio.get_event_loop().run_in_executor(
                    None, lambda: _scan_log_queue.get(timeout=1)
                )
                yield f"data: {line}\n\n"
            except Exception:
                # Timeout — check if scan still running
                if not _scan_running.is_set():
                    yield "data: [SCAN_COMPLETE]\n\n"
                    break
                yield ": heartbeat\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@app.get("/api/scan/history")
async def api_scan_history(limit: int = Query(20, ge=1, le=100), db: AsyncSession = Depends(get_db)):
    return await get_scan_history(db, limit=limit)


@app.get("/api/scan/{scan_run_id}/logs")
async def api_scan_logs(scan_run_id: str, db: AsyncSession = Depends(get_db)):
    logs = await get_scan_run_logs(db, scan_run_id)
    return {"log_lines": logs}


# ─── Export ───────────────────────────────────────────────────────────────────

@app.get("/api/export/duplicates.csv")
async def api_export_csv(db: AsyncSession = Depends(get_db)):
    csv_data = await export_duplicates_csv(db)
    return Response(
        content=csv_data,
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=kawkaw_duplicates.csv"},
    )


# ─── Settings ─────────────────────────────────────────────────────────────────

@app.get("/api/settings")
async def api_get_settings():
    return _settings


class SettingsRequest(BaseModel):
    scan_folders: list[str] | None = None
    trash_folder: str | None = None
    scan_schedule: str | None = None
    timezone: str | None = None


@app.post("/api/settings")
async def api_update_settings(body: SettingsRequest):
    global _settings
    if body.scan_folders is not None:
        _settings["scan_folders"] = [f.strip() for f in body.scan_folders if f.strip()]
    if body.trash_folder is not None:
        _settings["trash_folder"] = body.trash_folder.strip()
    if body.timezone is not None:
        _settings["timezone"] = body.timezone.strip() or "UTC"
    if body.scan_schedule is not None:
        _settings["scan_schedule"] = body.scan_schedule.strip()
    # Reschedule whenever schedule or timezone changes
    if body.scan_schedule is not None or body.timezone is not None:
        reschedule_job(_settings["scan_schedule"], _scheduled_scan_job, tz=_settings.get("timezone", "UTC"))

    _save_settings(_settings)
    return {"ok": True, "settings": _settings}


# ─── Browse ───────────────────────────────────────────────────────────────────

@app.get("/api/browse")
async def api_browse(path: str = Query("/scans")):
    """
    List subdirectories at the given path. Restricted to /scans/ for security.
    Returns {path, parent, entries: [{name, path, entry_count}]}.
    """
    import pathlib

    try:
        target = pathlib.Path(path).resolve()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid path")

    scans_root = pathlib.Path("/scans").resolve()
    if not (str(target) == str(scans_root) or str(target).startswith(str(scans_root) + "/")):
        raise HTTPException(status_code=403, detail="Browsing outside /scans/ is not permitted")

    if not target.exists():
        raise HTTPException(status_code=404, detail="Path does not exist")
    if not target.is_dir():
        raise HTTPException(status_code=400, detail="Path is not a directory")

    entries = []
    try:
        for entry in sorted(target.iterdir(), key=lambda e: e.name.lower()):
            if not entry.is_dir() or entry.name.startswith("."):
                continue
            # Count direct children for display
            try:
                child_count = sum(1 for c in entry.iterdir() if c.is_dir() and not c.name.startswith("."))
            except PermissionError:
                child_count = 0
            entries.append({
                "name": entry.name,
                "path": str(entry),
                "child_dirs": child_count,
            })
    except PermissionError:
        pass

    parent = str(target.parent) if str(target) != str(scans_root) else None

    return {"path": str(target), "parent": parent, "entries": entries}


# ─── Mounts ───────────────────────────────────────────────────────────────────

@app.get("/api/mounts")
async def api_mounts():
    """
    Returns which MOUNT_1..5 / MOUNT_TRASH env vars are set and their
    corresponding container-side paths. The GUI uses this to show users
    which paths are available to add as scan folders.
    """
    import os as _os
    mounts = []
    for i in range(1, 6):
        host_path = _os.getenv(f"MOUNT_{i}", "")
        container_path = f"/scans/vol{i}"
        if host_path and host_path not in ("/tmp", ""):
            mounts.append({
                "slot": f"vol{i}",
                "env_var": f"MOUNT_{i}",
                "host_path": host_path,
                "container_path": container_path,
                "configured": True,
            })
        else:
            mounts.append({
                "slot": f"vol{i}",
                "env_var": f"MOUNT_{i}",
                "host_path": host_path or "(not set)",
                "container_path": container_path,
                "configured": False,
            })

    trash_host = _os.getenv("MOUNT_TRASH", "")
    mounts.append({
        "slot": "trash",
        "env_var": "MOUNT_TRASH",
        "host_path": trash_host or "(not set)",
        "container_path": "/scans/trash",
        "configured": bool(trash_host and trash_host != "/tmp"),
        "is_trash": True,
    })
    return {"mounts": mounts}
