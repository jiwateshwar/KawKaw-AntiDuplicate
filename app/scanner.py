import hashlib
import io
import logging
import os
import queue
import threading
from datetime import datetime, timezone
from typing import Iterator

import exifread

logger = logging.getLogger(__name__)

SUPPORTED_EXTENSIONS = {
    ".jpg", ".jpeg", ".png", ".tiff", ".tif",
    ".arw", ".cr2", ".cr3", ".nef", ".dng",
    ".heic", ".heif", ".raf", ".orf", ".rw2",
    ".pef", ".srw", ".x3f", ".3fr", ".rwl",
}


def _to_json_safe(value):
    """Recursively convert exifread values to JSON-serialisable types."""
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8", errors="replace")
        except Exception:
            return repr(value)
    if isinstance(value, (list, tuple)):
        return [_to_json_safe(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _to_json_safe(v) for k, v in value.items()}
    # exifread IFDRational, Ratio, etc.
    if hasattr(value, "num") and hasattr(value, "den"):
        if value.den == 0:
            return None
        return f"{value.num}/{value.den}"
    if hasattr(value, "values"):
        vals = value.values
        if isinstance(vals, bytes):
            try:
                return vals.decode("utf-8", errors="replace").strip("\x00")
            except Exception:
                return repr(vals)
        if isinstance(vals, list):
            if len(vals) == 1:
                return _to_json_safe(vals[0])
            return [_to_json_safe(v) for v in vals]
        return _to_json_safe(vals)
    return str(value)


def _rational_to_float(tag_value) -> float | None:
    """Convert an exifread IFDRational/Ratio to float."""
    try:
        v = _to_json_safe(tag_value)
        if isinstance(v, (int, float)):
            return float(v)
        if isinstance(v, str) and "/" in v:
            num, den = v.split("/", 1)
            den_f = float(den)
            if den_f == 0:
                return None
            return float(num) / den_f
        if v is not None:
            return float(v)
    except Exception:
        pass
    return None


def _parse_gps_coord(values) -> float | None:
    """Convert GPS DMS rational list to decimal degrees."""
    try:
        parts = []
        for v in values:
            parts.append(_rational_to_float(v))
        if len(parts) >= 3 and all(p is not None for p in parts[:3]):
            return parts[0] + parts[1] / 60.0 + parts[2] / 3600.0
    except Exception:
        pass
    return None


def _parse_datetime(tag_value) -> datetime | None:
    """Parse EXIF datetime string '2024:03:12 14:30:00' into a datetime."""
    try:
        raw = _to_json_safe(tag_value)
        if not raw or not isinstance(raw, str):
            return None
        raw = raw.strip().replace("\x00", "")
        for fmt in ("%Y:%m:%d %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y:%m:%d"):
            try:
                return datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc)
            except ValueError:
                continue
    except Exception:
        pass
    return None


def _extract_exif_with_exifread(file_path: str) -> dict:
    with open(file_path, "rb") as f:
        tags = exifread.process_file(f, details=True, stop_tag="UNDEF")

    raw_tags: dict = {}
    for key, val in tags.items():
        raw_tags[key] = _to_json_safe(val)

    def get(key):
        return tags.get(key)

    image_unique_id = _to_json_safe(get("EXIF ImageUniqueID") or get("Image ImageUniqueID"))
    if isinstance(image_unique_id, str):
        image_unique_id = image_unique_id.strip("\x00 ")

    date_taken = _parse_datetime(
        get("EXIF DateTimeOriginal") or get("EXIF DateTimeDigitized") or get("Image DateTime")
    )

    shutter_count = None
    sc_tag = get("MakerNote ShutterCount") or get("EXIF BodySerialNumber")
    if sc_tag is not None:
        try:
            raw_sc = _to_json_safe(sc_tag)
            if isinstance(raw_sc, str) and raw_sc.isdigit():
                shutter_count = int(raw_sc)
            elif isinstance(raw_sc, (int, float)):
                shutter_count = int(raw_sc)
        except Exception:
            pass

    camera_make = _to_json_safe(get("Image Make"))
    if isinstance(camera_make, str):
        camera_make = camera_make.strip()

    camera_model = _to_json_safe(get("Image Model"))
    if isinstance(camera_model, str):
        camera_model = camera_model.strip()

    lens_model = _to_json_safe(get("EXIF LensModel") or get("MakerNote LensType"))
    if isinstance(lens_model, str):
        lens_model = lens_model.strip()

    f_number = _rational_to_float(get("EXIF FNumber"))

    exposure_time_tag = get("EXIF ExposureTime")
    exposure_time = _to_json_safe(exposure_time_tag) if exposure_time_tag else None
    if isinstance(exposure_time, (int, float)):
        exposure_time = str(exposure_time)

    iso = None
    iso_tag = get("EXIF ISOSpeedRatings") or get("EXIF ISO")
    if iso_tag is not None:
        try:
            iso = int(_to_json_safe(iso_tag))
        except (TypeError, ValueError):
            pass

    gps_lat = None
    gps_lon = None
    lat_tag = get("GPS GPSLatitude")
    lon_tag = get("GPS GPSLongitude")
    lat_ref = _to_json_safe(get("GPS GPSLatitudeRef"))
    lon_ref = _to_json_safe(get("GPS GPSLongitudeRef"))
    if lat_tag:
        gps_lat = _parse_gps_coord(lat_tag.values if hasattr(lat_tag, "values") else [])
        if gps_lat is not None and isinstance(lat_ref, str) and lat_ref.upper() == "S":
            gps_lat = -gps_lat
    if lon_tag:
        gps_lon = _parse_gps_coord(lon_tag.values if hasattr(lon_tag, "values") else [])
        if gps_lon is not None and isinstance(lon_ref, str) and lon_ref.upper() == "W":
            gps_lon = -gps_lon

    return {
        "raw_tags": raw_tags,
        "image_unique_id": image_unique_id if image_unique_id else None,
        "date_taken": date_taken,
        "shutter_count": shutter_count,
        "camera_make": camera_make if camera_make else None,
        "camera_model": camera_model if camera_model else None,
        "lens_model": lens_model if lens_model else None,
        "f_number": f_number,
        "exposure_time": exposure_time,
        "iso": iso,
        "gps_lat": gps_lat,
        "gps_lon": gps_lon,
    }


def _extract_exif_with_pillow(file_path: str) -> dict:
    """Fallback EXIF extractor using Pillow (useful for HEIC/HEIF)."""
    from PIL import Image as PILImage
    from PIL.ExifTags import TAGS

    result = {
        "raw_tags": {},
        "image_unique_id": None,
        "date_taken": None,
        "shutter_count": None,
        "camera_make": None,
        "camera_model": None,
        "lens_model": None,
        "f_number": None,
        "exposure_time": None,
        "iso": None,
        "gps_lat": None,
        "gps_lon": None,
    }

    with PILImage.open(file_path) as img:
        exif_data = img._getexif() if hasattr(img, "_getexif") else None
        if not exif_data:
            info = img.info or {}
            if "exif" in info:
                import piexif
                try:
                    exif_data = piexif.load(info["exif"])
                except Exception:
                    pass

    if not exif_data or not isinstance(exif_data, dict):
        return result

    raw: dict = {}
    for tag_id, value in exif_data.items():
        tag_name = TAGS.get(tag_id, str(tag_id))
        raw[tag_name] = _to_json_safe(value)
    result["raw_tags"] = raw

    result["camera_make"] = raw.get("Make", "").strip() or None
    result["camera_model"] = raw.get("Model", "").strip() or None
    result["lens_model"] = raw.get("LensModel", "").strip() or None
    result["image_unique_id"] = raw.get("ImageUniqueID", "").strip() or None

    dt_raw = raw.get("DateTimeOriginal") or raw.get("DateTimeDigitized") or raw.get("DateTime")
    if dt_raw:
        result["date_taken"] = _parse_datetime(dt_raw)

    fn = raw.get("FNumber")
    if fn:
        try:
            result["f_number"] = float(fn.split("/")[0]) / float(fn.split("/")[1]) if "/" in str(fn) else float(fn)
        except Exception:
            pass

    et = raw.get("ExposureTime")
    if et:
        result["exposure_time"] = str(et)

    iso = raw.get("ISOSpeedRatings")
    if iso:
        try:
            result["iso"] = int(iso)
        except Exception:
            pass

    return result


def extract_exif(file_path: str) -> dict:
    """
    Extract all EXIF data from an image file.
    Returns dict with raw_tags and parsed scalar fields.
    Never raises — on failure returns minimal dict with error info.
    """
    try:
        result = _extract_exif_with_exifread(file_path)
        if not result["raw_tags"] and file_path.lower().endswith((".heic", ".heif", ".cr3")):
            raise ValueError("No tags from exifread, trying Pillow")
        return result
    except Exception as e1:
        try:
            result = _extract_exif_with_pillow(file_path)
            return result
        except Exception as e2:
            return {
                "raw_tags": {"_error": str(e1), "_pillow_error": str(e2), "_method": "fallback"},
                "image_unique_id": None,
                "date_taken": None,
                "shutter_count": None,
                "camera_make": None,
                "camera_model": None,
                "lens_model": None,
                "f_number": None,
                "exposure_time": None,
                "iso": None,
                "gps_lat": None,
                "gps_lon": None,
            }


def compute_duplicate_id(exif: dict, file_path: str) -> tuple[str, str]:
    """
    Returns (duplicate_id_hex, method_name).
    Priority:
      1. SHA256(make|model|image_unique_id|date_taken_iso)  → 'exif_unique_id'
      2. SHA256(str(shutter_count)|date_taken_iso)          → 'shutter_datetime'
      3. MD5 of first 65536 bytes of file                   → 'md5_64k'
    """
    uid = exif.get("image_unique_id")
    date_taken = exif.get("date_taken")
    make = exif.get("camera_make") or ""
    model = exif.get("camera_model") or ""
    shutter = exif.get("shutter_count")

    if uid and date_taken and uid.replace("0", "").strip():
        date_iso = date_taken.strftime("%Y-%m-%dT%H:%M:%S") if isinstance(date_taken, datetime) else str(date_taken)
        fingerprint = f"{make}|{model}|{uid}|{date_iso}"
        return hashlib.sha256(fingerprint.encode()).hexdigest(), "exif_unique_id"

    if shutter and shutter > 0 and date_taken:
        date_iso = date_taken.strftime("%Y-%m-%dT%H:%M:%S") if isinstance(date_taken, datetime) else str(date_taken)
        fingerprint = f"{make}|{model}|{shutter}|{date_iso}"
        return hashlib.sha256(fingerprint.encode()).hexdigest(), "shutter_datetime"

    # Fallback: MD5 of first 64 KB
    try:
        with open(file_path, "rb") as f:
            chunk = f.read(65536)
        return hashlib.md5(chunk).hexdigest(), "md5_64k"
    except Exception:
        return hashlib.md5(file_path.encode()).hexdigest(), "md5_64k"


def walk_scan_folders(folders: list[str], trash_folder: str) -> Iterator[tuple[str, os.stat_result]]:
    """
    Yields (absolute_path, stat_result) for every supported image file
    under the given folders. Skips symlinks, hidden dirs, and the trash folder.
    """
    trash_norm = os.path.normpath(trash_folder) if trash_folder else None

    for folder in folders:
        folder = os.path.normpath(folder)
        if not os.path.isdir(folder):
            logger.warning(f"Scan folder does not exist or is not a directory: {folder}")
            continue

        for root, dirs, files in os.walk(folder, followlinks=False):
            # Prune hidden dirs and trash folder in-place
            dirs[:] = [
                d for d in dirs
                if not d.startswith(".")
                and os.path.normpath(os.path.join(root, d)) != trash_norm
            ]

            for fname in files:
                ext = os.path.splitext(fname)[1].lower()
                if ext not in SUPPORTED_EXTENSIONS:
                    continue

                path = os.path.join(root, fname)

                # Skip symlinks
                if os.path.islink(path):
                    continue

                try:
                    st = os.stat(path)
                except OSError:
                    continue

                yield path, st


def run_scan_sync(
    scan_run_id: str,
    scan_type: str,
    scan_folders: list[str],
    trash_folder: str,
    sync_session_factory,
    log_queue: queue.Queue,
    cancel_event: threading.Event,
) -> None:
    """
    Full scan orchestrator — runs synchronously in a thread pool executor.
    Writes directly to DB via synchronous SQLAlchemy session.
    """
    from datetime import timezone as tz
    from sqlalchemy import text as sa_text
    import json

    session = sync_session_factory()
    collected_logs: list[str] = []

    def log(msg: str):
        ts = datetime.now(tz.utc).strftime("%H:%M:%S")
        line = f"[{ts}] {msg}"
        logger.info(line)
        log_queue.put(line)
        collected_logs.append(line)

    try:
        _run_scan_body(
            scan_run_id, scan_type, scan_folders, trash_folder,
            session, log, cancel_event
        )
    except Exception as e:
        logger.exception("Scan failed with exception")
        try:
            session.execute(
                sa_text(
                    "UPDATE scan_runs SET status='error', finished_at=NOW(), error_message=:msg WHERE id=:id"
                ),
                {"msg": str(e), "id": scan_run_id},
            )
            session.commit()
        except Exception:
            pass
    finally:
        # Persist all collected log lines to the scan_run record
        try:
            session.execute(
                sa_text("UPDATE scan_runs SET log_lines=CAST(:logs AS jsonb) WHERE id=:id"),
                {"logs": json.dumps(collected_logs), "id": scan_run_id},
            )
            session.commit()
        except Exception:
            pass
        session.close()


def _run_scan_body(
    scan_run_id: str,
    scan_type: str,
    scan_folders: list[str],
    trash_folder: str,
    session,
    log,
    cancel_event: threading.Event,
) -> None:
    from sqlalchemy import text as sa_text
    from datetime import timezone as tz

    counts = {
        "scanned": 0, "new": 0, "updated": 0, "skipped": 0, "deleted": 0
    }

    # Phase 1: Load known paths from DB
    log(f"Loading existing catalog from database...")
    rows = session.execute(
        sa_text("SELECT file_path, file_mtime, id FROM images WHERE is_deleted = FALSE AND trash_moved = FALSE")
    ).fetchall()
    known: dict[str, tuple] = {row[0]: (str(row[2]), row[1]) for row in rows}
    log(f"Catalog loaded: {len(known)} known files")

    seen_paths: set[str] = set()
    batch_values: list[dict] = []

    def flush_batch():
        if not batch_values:
            return
        for v in batch_values:
            existing_id = known.get(v["file_path"])
            if existing_id:
                session.execute(
                    sa_text("""
                        UPDATE images SET
                            file_name=:file_name,
                            file_extension=:file_extension,
                            file_size_bytes=:file_size_bytes,
                            file_mtime=:file_mtime,
                            duplicate_id=:duplicate_id,
                            duplicate_id_method=:duplicate_id_method,
                            camera_make=:camera_make,
                            camera_model=:camera_model,
                            lens_model=:lens_model,
                            date_taken=:date_taken,
                            shutter_count=:shutter_count,
                            image_unique_id=:image_unique_id,
                            f_number=:f_number,
                            exposure_time=:exposure_time,
                            iso=:iso,
                            gps_lat=:gps_lat,
                            gps_lon=:gps_lon,
                            exif_json=CAST(:exif_json AS jsonb),
                            last_scanned=NOW()
                        WHERE file_path=:file_path
                    """),
                    v,
                )
            else:
                import json
                session.execute(
                    sa_text("""
                        INSERT INTO images (
                            id, file_path, file_name, file_extension, file_size_bytes, file_mtime,
                            duplicate_id, duplicate_id_method,
                            camera_make, camera_model, lens_model,
                            date_taken, shutter_count, image_unique_id,
                            f_number, exposure_time, iso, gps_lat, gps_lon,
                            exif_json, is_deleted, is_marked, trash_moved,
                            first_seen, last_scanned
                        ) VALUES (
                            gen_random_uuid(), :file_path, :file_name, :file_extension,
                            :file_size_bytes, :file_mtime,
                            :duplicate_id, :duplicate_id_method,
                            :camera_make, :camera_model, :lens_model,
                            :date_taken, :shutter_count, :image_unique_id,
                            :f_number, :exposure_time, :iso, :gps_lat, :gps_lon,
                            CAST(:exif_json AS jsonb), FALSE, FALSE, FALSE,
                            NOW(), NOW()
                        )
                        ON CONFLICT (file_path) DO UPDATE SET
                            file_name=EXCLUDED.file_name,
                            file_extension=EXCLUDED.file_extension,
                            file_size_bytes=EXCLUDED.file_size_bytes,
                            file_mtime=EXCLUDED.file_mtime,
                            duplicate_id=EXCLUDED.duplicate_id,
                            duplicate_id_method=EXCLUDED.duplicate_id_method,
                            camera_make=EXCLUDED.camera_make,
                            camera_model=EXCLUDED.camera_model,
                            lens_model=EXCLUDED.lens_model,
                            date_taken=EXCLUDED.date_taken,
                            shutter_count=EXCLUDED.shutter_count,
                            image_unique_id=EXCLUDED.image_unique_id,
                            f_number=EXCLUDED.f_number,
                            exposure_time=EXCLUDED.exposure_time,
                            iso=EXCLUDED.iso,
                            gps_lat=EXCLUDED.gps_lat,
                            gps_lon=EXCLUDED.gps_lon,
                            exif_json=EXCLUDED.exif_json,
                            last_scanned=NOW()
                    """),
                    v,
                )
        session.commit()
        batch_values.clear()

    # Phase 2: Walk filesystem
    log(f"Starting filesystem walk over {len(scan_folders)} folder(s)...")
    for file_path, st in walk_scan_folders(scan_folders, trash_folder):
        if cancel_event.is_set():
            log("Scan cancelled by user.")
            break

        seen_paths.add(file_path)
        mtime = st.st_mtime
        known_entry = known.get(file_path)

        if known_entry and abs(known_entry[1] - mtime) < 0.01:
            # Unchanged — just bump last_scanned with a cheap update
            session.execute(
                sa_text("UPDATE images SET last_scanned=NOW() WHERE file_path=:p"),
                {"p": file_path},
            )
            counts["skipped"] += 1
            counts["scanned"] += 1
            if counts["scanned"] % 500 == 0:
                session.commit()
            if counts["scanned"] % 100 == 0:
                log(f"Progress: {counts['scanned']} files processed ({counts['new']} new, {counts['updated']} updated, {counts['skipped']} skipped)")
            continue

        # New or modified file — extract EXIF
        exif = extract_exif(file_path)
        dup_id, method = compute_duplicate_id(exif, file_path)

        import json
        v = {
            "file_path": file_path,
            "file_name": os.path.basename(file_path),
            "file_extension": os.path.splitext(file_path)[1].lower(),
            "file_size_bytes": st.st_size,
            "file_mtime": mtime,
            "duplicate_id": dup_id,
            "duplicate_id_method": method,
            "camera_make": exif.get("camera_make"),
            "camera_model": exif.get("camera_model"),
            "lens_model": exif.get("lens_model"),
            "date_taken": exif.get("date_taken"),
            "shutter_count": exif.get("shutter_count"),
            "image_unique_id": exif.get("image_unique_id"),
            "f_number": float(exif["f_number"]) if exif.get("f_number") is not None else None,
            "exposure_time": exif.get("exposure_time"),
            "iso": exif.get("iso"),
            "gps_lat": exif.get("gps_lat"),
            "gps_lon": exif.get("gps_lon"),
            "exif_json": json.dumps(exif.get("raw_tags") or {}),
        }

        if known_entry:
            counts["updated"] += 1
        else:
            counts["new"] += 1
        counts["scanned"] += 1

        batch_values.append(v)
        if len(batch_values) >= 100:
            flush_batch()
            log(f"Progress: {counts['scanned']} files processed ({counts['new']} new, {counts['updated']} updated, {counts['skipped']} skipped)")

    flush_batch()

    # Phase 3: Deleted check
    if scan_type == "deleted_check":
        log("Checking for deleted files...")
        all_rows = session.execute(
            sa_text("SELECT file_path FROM images WHERE is_deleted=FALSE AND trash_moved=FALSE")
        ).fetchall()
        for (path,) in all_rows:
            if not os.path.exists(path):
                session.execute(
                    sa_text("UPDATE images SET is_deleted=TRUE, last_scanned=NOW() WHERE file_path=:p"),
                    {"p": path},
                )
                counts["deleted"] += 1
        session.commit()
        log(f"Deleted check complete: {counts['deleted']} files marked as deleted")

    # Phase 4: Finalise scan_run
    total = counts["scanned"]
    log(f"Scan complete. Total: {total}, New: {counts['new']}, Updated: {counts['updated']}, Skipped: {counts['skipped']}, Deleted: {counts['deleted']}")

    session.execute(
        sa_text("""
            UPDATE scan_runs SET
                status='completed',
                finished_at=NOW(),
                files_scanned=:scanned,
                files_new=:new,
                files_updated=:updated,
                files_skipped=:skipped,
                files_deleted=:deleted
            WHERE id=:id
        """),
        {
            "scanned": counts["scanned"],
            "new": counts["new"],
            "updated": counts["updated"],
            "skipped": counts["skipped"],
            "deleted": counts["deleted"],
            "id": scan_run_id,
        },
    )
    session.commit()
