#!/usr/bin/env python3
"""
reseed-mover.py — Automatically repoints rTorrent torrents to *arr-imported paths.

After Radarr/Sonarr copies media into the library, they change the torrent
label to '<service>_imported' (e.g. radarr_imported). This script:
  1. Queries rTorrent (XMLRPC) for torrents with a *_imported label
     still pointing at /downloads/complete/
  2. Uses the torrent hash to look up the imported path via the *arr API
  3. Verifies the file(s) exist at the new location
  4a. Single-file: stops torrent, updates d.directory, rehashes, restarts
  4b. Multi-file (subfolder): *arr renames the folder so rTorrent can't
      reseed — erases the torrent from rTorrent instead
  5. Deletes the old copy from /downloads/complete/

Environment variables:
  RTORRENT_XMLRPC_URL   rTorrent XMLRPC endpoint    (default: http://rutorrent:8080/RPC2)
  RADARR_URL            Radarr base URL              (default: http://radarr:7878)
  RADARR_API_KEY        Radarr API key               (required)
  SONARR_URL            Sonarr base URL              (default: http://sonarr:8989)
  SONARR_API_KEY        Sonarr API key               (required)
  DOWNLOADS_BASE        Download root to match        (default: /downloads/complete)
  DRY_RUN               Log only, don't change        (default: false)
  CLEANUP_OLD           Delete old copy after move    (default: true)
  VERIFY_FILES          Check files exist before move (default: true)
"""

import xmlrpc.client
import requests
import os
import shutil
import logging
import sys
from pathlib import Path
from dataclasses import dataclass, field

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("reseed-mover")

# ─── Config ──────────────────────────────────────────────────────────────────

RTORRENT_URL = os.getenv("RTORRENT_XMLRPC_URL", "http://rutorrent:8080/RPC2")
DOWNLOADS_BASE = os.getenv("DOWNLOADS_BASE", "/downloads/complete")
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"
CLEANUP_OLD = os.getenv("CLEANUP_OLD", "true").lower() == "true"
VERIFY_FILES = os.getenv("VERIFY_FILES", "true").lower() == "true"

SERVICES = {
    "radarr": {
        "url": os.getenv("RADARR_URL", "http://radarr:7878"),
        "api_key": os.getenv("RADARR_API_KEY", ""),
        "api_version": "v3",
    },
    "sonarr": {
        "url": os.getenv("SONARR_URL", "http://sonarr:8989"),
        "api_key": os.getenv("SONARR_API_KEY", ""),
        "api_version": "v3",
    },
}


# ─── Data ────────────────────────────────────────────────────────────────────

@dataclass
class Torrent:
    info_hash: str
    name: str
    directory: str
    label: str
    service: str          # resolved *arr service name (radarr, sonarr)
    is_multi_file: bool
    size_bytes: int
    files: list[dict] = field(default_factory=list)


# ─── rTorrent helpers ────────────────────────────────────────────────────────

def get_rtorrent():
    return xmlrpc.client.ServerProxy(RTORRENT_URL)


ACCEPTED_LABELS = {
    "radarr_imported": "radarr",
    "sonarr_imported": "sonarr",
}


def parse_service_from_label(label: str) -> str | None:
    """Map a torrent label to its *arr service name, or None if irrelevant."""
    return ACCEPTED_LABELS.get(label.strip().lower())


def get_pending_torrents(rt) -> list[Torrent]:
    """Return all torrents with a *_imported label still in DOWNLOADS_BASE."""
    results = rt.d.multicall2(
        "",
        "main",
        "d.hash=",
        "d.name=",
        "d.directory=",
        "d.custom1=",        # label set by *arr
        "d.is_multi_file=",
        "d.size_bytes=",
    )

    pending = []
    for info_hash, name, directory, label, is_multi, size in results:
        service = parse_service_from_label(label)
        if service and directory.startswith(DOWNLOADS_BASE):
            pending.append(Torrent(
                info_hash=info_hash.upper(),
                name=name,
                directory=directory,
                label=label.strip().lower(),
                service=service,
                is_multi_file=bool(is_multi),
                size_bytes=size,
            ))

    log.info("Found %d imported torrent(s) still in %s", len(pending), DOWNLOADS_BASE)
    return pending


def get_torrent_files(rt, info_hash: str) -> list[dict]:
    """Get per-file paths inside a torrent (relative to d.directory)."""
    results = rt.f.multicall(info_hash, "", "f.path=", "f.size_bytes=")
    return [{"path": path, "size": size} for path, size in results]


def update_torrent_directory(rt, torrent: Torrent, new_dir: str) -> bool:
    """Stop → set directory → check hash → start."""
    h = torrent.info_hash
    try:
        rt.d.stop(h)
        rt.d.directory.set(h, new_dir)
        rt.d.check_hash(h)
        rt.d.start(h)
        return True
    except Exception as e:
        log.error("  XMLRPC error: %s", e)
        try:
            rt.d.start(h)  # best-effort restart on failure
        except Exception:
            pass
        return False


# ─── *arr API helpers ────────────────────────────────────────────────────────

def arr_get(service: str, endpoint: str, params: dict | None = None) -> dict | list:
    cfg = SERVICES[service]
    url = f"{cfg['url']}/api/{cfg['api_version']}/{endpoint}"
    resp = requests.get(url, headers={"X-Api-Key": cfg["api_key"]}, params=params or {})
    resp.raise_for_status()
    return resp.json()


def lookup_radarr(torrent: Torrent) -> str | None:
    """
    Find the library path for a Radarr-imported movie by download hash.

    Returns the directory containing the movie file, e.g.
    /media/Plex Cloud Sync/Movies/Movie Name (2023) {tmdb-12345}
    """
    history = arr_get("radarr", "history", {
        "downloadId": torrent.info_hash,
        "pageSize": 50,
        "eventType": 3,  # downloadFolderImported
    })

    records = history.get("records", []) if isinstance(history, dict) else history
    for rec in records:
        movie_id = rec.get("movieId")
        if not movie_id:
            continue

        movie = arr_get("radarr", f"movie/{movie_id}")

        # Prefer the actual file path over the movie root
        movie_file = movie.get("movieFile")
        if movie_file and movie_file.get("path"):
            return str(Path(movie_file["path"]).parent)

        if movie.get("path"):
            return movie["path"]

    return None


def lookup_sonarr(torrent: Torrent) -> str | None:
    """
    Find the library path for a Sonarr-imported episode/season by download hash.

    For single-episode torrents → returns the season folder.
    For season packs → returns the series root folder.
    """
    history = arr_get("sonarr", "history", {
        "downloadId": torrent.info_hash,
        "pageSize": 50,
        "eventType": 3,  # downloadFolderImported
    })

    records = history.get("records", []) if isinstance(history, dict) else history
    if not records:
        return None

    series_id = records[0].get("seriesId")
    if not series_id:
        return None

    series = arr_get("sonarr", f"series/{series_id}")

    if torrent.is_multi_file:
        # Season pack — point to the series root; files span season dirs
        return series.get("path")

    # Single episode — look up the episode to get its file path directly
    episode_id = records[0].get("episodeId")
    if episode_id:
        episode = arr_get("sonarr", f"episode/{episode_id}")
        file_id = episode.get("episodeFileId")
        if file_id:
            ef = arr_get("sonarr", f"episodefile/{file_id}")
            if ef.get("path"):
                return str(Path(ef["path"]).parent)

    return series.get("path")


LOOKUP = {
    "radarr": lookup_radarr,
    "sonarr": lookup_sonarr,
}


# ─── Verification ────────────────────────────────────────────────────────────

def verify_files_exist(torrent: Torrent, new_dir: str) -> bool:
    """
    Confirm that the torrent's files actually exist at the new location.

    This prevents rTorrent from failing the hash check and going into an
    error state. For multi-file torrents we check that the torrent's root
    folder (torrent.name) exists as a subdirectory.
    """
    new_path = Path(new_dir)

    if torrent.is_multi_file:
        # d.directory for multi-file is the *parent* of the torrent folder,
        # so we expect new_dir/torrent.name/ to exist
        root = new_path / torrent.name
        if not root.is_dir():
            # *arr may have renamed the folder — check if files exist
            # directly under new_dir instead
            for f in torrent.files:
                # Try without the torrent-name prefix
                parts = Path(f["path"]).parts
                if len(parts) > 1:
                    candidate = new_path / Path(*parts[1:])
                else:
                    candidate = new_path / f["path"]
                if candidate.exists():
                    continue
                log.warning("  File not found: %s", candidate)
                return False
            log.info("  Files found (renamed folder)")
            return True
        return True

    # Single file: check torrent.name exists in new_dir
    target = new_path / torrent.name
    if target.exists():
        return True

    # *arr may have renamed the file — check if *any* file of similar size exists
    for item in new_path.iterdir():
        if item.is_file() and abs(item.stat().st_size - torrent.size_bytes) < 1024:
            log.warning(
                "  Original filename not found, but size-matched: %s",
                item.name,
            )
            log.warning(
                "  If *arr renamed the file, rTorrent will fail the hash check. "
                "Skipping to be safe."
            )
            return False

    log.warning("  File not found at target: %s", target)
    return False


def erase_torrent(rt, torrent: Torrent) -> bool:
    """Remove the torrent entry from rTorrent entirely (d.erase)."""
    try:
        rt.d.stop(torrent.info_hash)
        rt.d.erase(torrent.info_hash)
        return True
    except Exception as e:
        log.error("  XMLRPC erase error: %s", e)
        return False


# ─── Cleanup ─────────────────────────────────────────────────────────────────

def cleanup(torrent: Torrent):
    """
    Remove the old copy from downloads/complete/.

    For both single and multi-file torrents the content root is
    d.directory / d.name:
      single-file → /downloads/complete/radarr/movie.mkv       (a file)
      multi-file  → /downloads/complete/radarr/ReleaseGroup/    (a directory)
    """
    target = Path(torrent.directory) / torrent.name

    if not target.exists():
        return

    if DRY_RUN:
        log.info("  [DRY RUN] Would delete: %s", target)
        return

    try:
        if target.is_dir():
            shutil.rmtree(target)
        else:
            target.unlink()
        log.info("  Cleaned up: %s", target)
    except OSError as e:
        log.warning("  Cleanup failed: %s", e)


# ─── Main ────────────────────────────────────────────────────────────────────

def main() -> int:
    if DRY_RUN:
        log.info("══════ DRY RUN — no changes will be made ══════")

    rt = get_rtorrent()
    torrents = get_pending_torrents(rt)

    stats = {"repointed": 0, "erased": 0, "skipped": 0, "failed": 0}

    for t in torrents:
        log.info("▶ %s  [%s → %s]  hash=%s  multi=%s",
                 t.name, t.label, t.service, t.info_hash[:8], t.is_multi_file)

        # ── Resolve service config ────────────────────────────────────
        svc = SERVICES.get(t.service)
        if not svc or not svc["api_key"]:
            log.warning("  No config/API key for service '%s', skipping", t.service)
            stats["skipped"] += 1
            continue

        lookup_fn = LOOKUP.get(t.service)
        if not lookup_fn:
            log.warning("  No lookup handler for '%s'", t.service)
            stats["skipped"] += 1
            continue

        # ── Load per-file info ────────────────────────────────────────
        try:
            t.files = get_torrent_files(rt, t.info_hash)
        except Exception as e:
            log.warning("  Could not get file list: %s", e)

        # ── Look up imported path via *arr API ────────────────────────
        try:
            new_path = lookup_fn(t)
        except requests.HTTPError as e:
            log.error("  *arr API error: %s", e)
            stats["failed"] += 1
            continue

        if not new_path:
            log.info("  Not found in %s history, skipping", t.service)
            stats["skipped"] += 1
            continue

        # Already moved?
        if t.directory == new_path:
            log.info("  Already pointing to library path, skipping")
            stats["skipped"] += 1
            continue

        log.info("  %s → %s", t.directory, new_path)

        # ── Multi-file torrents (subfolder) ───────────────────────────
        # rTorrent resolves files as d.directory/f.path, and f.path
        # includes the torrent's original root folder name
        # (e.g. "SomeRelease-GROUP/movie.mkv").  Since *arr renames
        # that folder, repointing d.directory can't make the paths
        # line up — so we erase the torrent and clean up instead.
        if t.is_multi_file:
            log.info("  Multi-file torrent — *arr renamed folder, can't reseed")
            if DRY_RUN:
                log.info("  [DRY RUN] Would erase torrent and delete old files")
                stats["erased"] += 1
                continue

            if erase_torrent(rt, t):
                log.info("  ✓ Erased from rTorrent")
                stats["erased"] += 1
                if CLEANUP_OLD:
                    cleanup(t)
            else:
                stats["failed"] += 1
            continue

        # ── Single-file torrents ──────────────────────────────────────
        # Verify the file exists at the library path with matching name.
        if VERIFY_FILES and not verify_files_exist(t, new_path):
            log.warning("  File verification failed — skipping (rename mismatch?)")
            stats["failed"] += 1
            continue

        if DRY_RUN:
            log.info("  [DRY RUN] Would repoint directory")
            stats["repointed"] += 1
            continue

        if update_torrent_directory(rt, t, new_path):
            log.info("  ✓ Repointed, still seeding")
            stats["repointed"] += 1
            if CLEANUP_OLD:
                cleanup(t)
        else:
            stats["failed"] += 1

    log.info(
        "═══ Finished: %d repointed, %d erased, %d skipped, %d failed ═══",
        stats["repointed"], stats["erased"], stats["skipped"], stats["failed"],
    )
    return 0 if stats["failed"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
