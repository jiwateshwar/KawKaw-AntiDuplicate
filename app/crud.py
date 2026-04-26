import csv
import io
import os
import shutil
from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import delete, func, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession

from .database import IgnoredGroup, Image, ScanRun


async def get_dashboard_stats(db: AsyncSession) -> dict:
    total = (await db.execute(select(func.count()).select_from(Image).where(Image.is_deleted == False))).scalar() or 0
    deleted = (await db.execute(select(func.count()).select_from(Image).where(Image.is_deleted == True))).scalar() or 0
    marked = (await db.execute(select(func.count()).select_from(Image).where(Image.is_marked == True, Image.is_deleted == False))).scalar() or 0
    trash = (await db.execute(select(func.count()).select_from(Image).where(Image.trash_moved == True))).scalar() or 0

    dup_groups = (
        await db.execute(
            text("""
                SELECT COUNT(*) FROM (
                    SELECT duplicate_id
                    FROM images
                    WHERE is_deleted = FALSE AND trash_moved = FALSE
                      AND duplicate_id NOT IN (SELECT duplicate_id FROM ignored_groups)
                    GROUP BY duplicate_id
                    HAVING COUNT(*) >= 2
                ) sub
            """)
        )
    ).scalar() or 0

    last_scan_row = (
        await db.execute(
            select(ScanRun).order_by(ScanRun.started_at.desc()).limit(1)
        )
    ).scalars().first()

    last_scan = None
    if last_scan_row:
        last_scan = {
            "id": str(last_scan_row.id),
            "started_at": last_scan_row.started_at.isoformat() if last_scan_row.started_at else None,
            "finished_at": last_scan_row.finished_at.isoformat() if last_scan_row.finished_at else None,
            "scan_type": last_scan_row.scan_type,
            "status": last_scan_row.status,
            "files_scanned": last_scan_row.files_scanned,
            "files_new": last_scan_row.files_new,
        }

    return {
        "total_files": total,
        "duplicate_groups": dup_groups,
        "deleted_count": deleted,
        "marked_count": marked,
        "trash_count": trash,
        "last_scan": last_scan,
    }


async def get_duplicate_groups(
    db: AsyncSession, page: int = 1, page_size: int = 50, include_ignored: bool = False
) -> dict:
    offset = (page - 1) * page_size
    ign_filter = "" if include_ignored else "AND duplicate_id NOT IN (SELECT duplicate_id FROM ignored_groups)"

    groups_q = await db.execute(
        text(f"""
            SELECT
                duplicate_id,
                COUNT(*) as cnt,
                MIN(camera_make) as camera_make,
                MIN(camera_model) as camera_model,
                MIN(date_taken) as date_taken,
                MIN(duplicate_id_method) as method,
                SUM(file_size_bytes) as total_size
            FROM images
            WHERE is_deleted = FALSE AND trash_moved = FALSE {ign_filter}
            GROUP BY duplicate_id
            HAVING COUNT(*) >= 2
            ORDER BY cnt DESC, MIN(date_taken) ASC NULLS LAST
            LIMIT :limit OFFSET :offset
        """),
        {"limit": page_size, "offset": offset},
    )
    group_rows = groups_q.fetchall()

    total_q = await db.execute(
        text(f"""
            SELECT COUNT(*) FROM (
                SELECT duplicate_id
                FROM images
                WHERE is_deleted = FALSE AND trash_moved = FALSE {ign_filter}
                GROUP BY duplicate_id
                HAVING COUNT(*) >= 2
            ) sub
        """)
    )
    total_groups = total_q.scalar() or 0

    if not group_rows:
        return {"total_groups": total_groups, "page": page, "page_size": page_size, "groups": []}

    dup_ids = [r[0] for r in group_rows]

    ignored_ids: set[str] = set()
    if include_ignored:
        ig_result = await db.execute(
            select(IgnoredGroup.duplicate_id).where(IgnoredGroup.duplicate_id.in_(dup_ids))
        )
        ignored_ids = {row[0] for row in ig_result.fetchall()}

    imgs_q = await db.execute(
        select(Image).where(
            Image.duplicate_id.in_(dup_ids),
            Image.is_deleted == False,
            Image.trash_moved == False,
        ).order_by(Image.duplicate_id, Image.date_taken)
    )
    imgs = imgs_q.scalars().all()

    imgs_by_dup: dict[str, list] = {}
    for img in imgs:
        imgs_by_dup.setdefault(str(img.duplicate_id), []).append(_image_to_dict(img))

    groups = []
    for row in group_rows:
        dup_id = row[0]
        groups.append({
            "duplicate_id": dup_id,
            "count": row[1],
            "camera_make": row[2],
            "camera_model": row[3],
            "date_taken": row[4].isoformat() if row[4] else None,
            "method": row[5],
            "total_size_bytes": row[6],
            "is_ignored": dup_id in ignored_ids,
            "files": imgs_by_dup.get(dup_id, []),
        })

    return {"total_groups": total_groups, "page": page, "page_size": page_size, "groups": groups}


async def ignore_group(db: AsyncSession, duplicate_id: str) -> dict:
    existing = (await db.execute(
        select(IgnoredGroup).where(IgnoredGroup.duplicate_id == duplicate_id)
    )).scalars().first()
    if not existing:
        db.add(IgnoredGroup(duplicate_id=duplicate_id))
        await db.flush()
    return {"duplicate_id": duplicate_id, "ignored": True}


async def unignore_group(db: AsyncSession, duplicate_id: str) -> dict:
    await db.execute(delete(IgnoredGroup).where(IgnoredGroup.duplicate_id == duplicate_id))
    await db.flush()
    return {"duplicate_id": duplicate_id, "ignored": False}


async def get_all_files(
    db: AsyncSession,
    page: int = 1,
    page_size: int = 100,
    search: str | None = None,
    camera_model: str | None = None,
    extension: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    is_marked: bool | None = None,
    is_deleted: bool | None = None,
) -> dict:
    q = select(Image)
    count_q = select(func.count()).select_from(Image)

    filters = []
    if search:
        like = f"%{search}%"
        from sqlalchemy import or_
        search_filter = or_(Image.file_name.ilike(like), Image.file_path.ilike(like))
        filters.append(search_filter)
    if camera_model:
        filters.append(Image.camera_model.ilike(f"%{camera_model}%"))
    if extension:
        filters.append(Image.file_extension == extension.lower())
    if date_from:
        try:
            filters.append(Image.date_taken >= datetime.fromisoformat(date_from))
        except ValueError:
            pass
    if date_to:
        try:
            filters.append(Image.date_taken <= datetime.fromisoformat(date_to))
        except ValueError:
            pass
    if is_marked is not None:
        filters.append(Image.is_marked == is_marked)
    if is_deleted is not None:
        filters.append(Image.is_deleted == is_deleted)

    if filters:
        from sqlalchemy import and_
        q = q.where(and_(*filters))
        count_q = count_q.where(and_(*filters))

    total = (await db.execute(count_q)).scalar() or 0
    offset = (page - 1) * page_size
    q = q.order_by(Image.date_taken.desc().nullslast(), Image.first_seen.desc()).limit(page_size).offset(offset)

    result = await db.execute(q)
    files = result.scalars().all()

    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "files": [_image_to_dict(f) for f in files],
    }


async def get_image_exif(db: AsyncSession, image_id: str) -> dict | None:
    result = await db.execute(select(Image).where(Image.id == UUID(image_id)))
    img = result.scalars().first()
    if not img:
        return None
    return img.exif_json or {}


async def mark_image(db: AsyncSession, image_id: str, marked: bool) -> dict | None:
    result = await db.execute(select(Image).where(Image.id == UUID(image_id)))
    img = result.scalars().first()
    if not img:
        return None
    img.is_marked = marked
    await db.flush()
    return _image_to_dict(img)


async def move_to_trash(db: AsyncSession, image_id: str, trash_folder: str) -> dict | None:
    result = await db.execute(select(Image).where(Image.id == UUID(image_id)))
    img = result.scalars().first()
    if not img:
        return None
    if img.trash_moved:
        return _image_to_dict(img)

    # Compute destination path
    subdir = (img.camera_model or "unknown").replace(" ", "_").replace("/", "_")
    date_prefix = img.date_taken.strftime("%Y-%m-%d") if img.date_taken else "no_date"
    dest_dir = os.path.join(trash_folder, f"{subdir}_{date_prefix}")
    os.makedirs(dest_dir, exist_ok=True)

    base = img.file_name
    dest = os.path.join(dest_dir, base)
    # Handle collision
    if os.path.exists(dest):
        name, ext = os.path.splitext(base)
        counter = 1
        while os.path.exists(dest):
            dest = os.path.join(dest_dir, f"{name}_{counter}{ext}")
            counter += 1

    shutil.move(img.file_path, dest)

    img.trash_moved = True
    img.trash_path = dest
    await db.flush()
    return _image_to_dict(img)


async def undelete_image(db: AsyncSession, image_id: str) -> dict | None:
    result = await db.execute(select(Image).where(Image.id == UUID(image_id)))
    img = result.scalars().first()
    if not img:
        return None
    img.is_deleted = False
    await db.flush()
    return _image_to_dict(img)


async def get_scan_history(db: AsyncSession, limit: int = 20) -> list:
    result = await db.execute(
        select(ScanRun).order_by(ScanRun.started_at.desc()).limit(limit)
    )
    runs = result.scalars().all()
    return [_scan_run_to_dict(r) for r in runs]


async def get_scan_run(db: AsyncSession, scan_run_id: str) -> dict | None:
    result = await db.execute(select(ScanRun).where(ScanRun.id == UUID(scan_run_id)))
    run = result.scalars().first()
    if not run:
        return None
    return _scan_run_to_dict(run)


async def get_scan_run_logs(db: AsyncSession, scan_run_id: str) -> list:
    result = await db.execute(select(ScanRun).where(ScanRun.id == UUID(scan_run_id)))
    run = result.scalars().first()
    if not run:
        return []
    return run.log_lines or []


async def create_scan_run(db: AsyncSession, scan_type: str) -> str:
    run = ScanRun(scan_type=scan_type, status="running", log_lines=[])
    db.add(run)
    await db.flush()
    scan_id = str(run.id)
    return scan_id


async def export_duplicates_csv(db: AsyncSession) -> str:
    rows = await db.execute(
        text("""
            SELECT i.duplicate_id, i.duplicate_id_method, i.file_path, i.file_name,
                   i.camera_make, i.camera_model, i.date_taken, i.file_size_bytes,
                   i.is_marked, i.trash_moved, i.file_extension, i.shutter_count,
                   i.image_unique_id
            FROM images i
            INNER JOIN (
                SELECT duplicate_id
                FROM images
                WHERE is_deleted = FALSE AND trash_moved = FALSE
                GROUP BY duplicate_id
                HAVING COUNT(*) >= 2
            ) dups ON i.duplicate_id = dups.duplicate_id
            WHERE i.is_deleted = FALSE AND i.trash_moved = FALSE
            ORDER BY i.duplicate_id, i.date_taken
        """)
    )
    all_rows = rows.fetchall()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "duplicate_id", "method", "file_path", "file_name",
        "camera_make", "camera_model", "date_taken", "file_size_bytes",
        "is_marked", "trash_moved", "file_extension", "shutter_count", "image_unique_id"
    ])
    for row in all_rows:
        writer.writerow([
            row[0], row[1], row[2], row[3],
            row[4], row[5],
            row[6].isoformat() if row[6] else "",
            row[7], row[8], row[9], row[10], row[11], row[12]
        ])
    return output.getvalue()


async def get_camera_models(db: AsyncSession) -> list[str]:
    result = await db.execute(
        select(Image.camera_model).distinct().where(Image.camera_model.isnot(None)).order_by(Image.camera_model)
    )
    return [r[0] for r in result.fetchall()]


async def get_extensions(db: AsyncSession) -> list[str]:
    result = await db.execute(
        select(Image.file_extension).distinct().where(Image.file_extension.isnot(None)).order_by(Image.file_extension)
    )
    return [r[0] for r in result.fetchall()]


def _image_to_dict(img: Image) -> dict:
    return {
        "id": str(img.id),
        "file_path": img.file_path,
        "file_name": img.file_name,
        "file_extension": img.file_extension,
        "file_size_bytes": img.file_size_bytes,
        "duplicate_id": img.duplicate_id,
        "duplicate_id_method": img.duplicate_id_method,
        "camera_make": img.camera_make,
        "camera_model": img.camera_model,
        "lens_model": img.lens_model,
        "date_taken": img.date_taken.isoformat() if img.date_taken else None,
        "shutter_count": img.shutter_count,
        "image_unique_id": img.image_unique_id,
        "f_number": float(img.f_number) if img.f_number is not None else None,
        "exposure_time": img.exposure_time,
        "iso": img.iso,
        "gps_lat": img.gps_lat,
        "gps_lon": img.gps_lon,
        "is_deleted": img.is_deleted,
        "is_marked": img.is_marked,
        "trash_moved": img.trash_moved,
        "trash_path": img.trash_path,
        "first_seen": img.first_seen.isoformat() if img.first_seen else None,
        "last_scanned": img.last_scanned.isoformat() if img.last_scanned else None,
    }


def _scan_run_to_dict(run: ScanRun) -> dict:
    return {
        "id": str(run.id),
        "started_at": run.started_at.isoformat() if run.started_at else None,
        "finished_at": run.finished_at.isoformat() if run.finished_at else None,
        "scan_type": run.scan_type,
        "status": run.status,
        "files_scanned": run.files_scanned,
        "files_new": run.files_new,
        "files_updated": run.files_updated,
        "files_skipped": run.files_skipped,
        "files_deleted": run.files_deleted,
        "error_message": run.error_message,
    }
