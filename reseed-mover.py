#!/usr/bin/env python3
"""
reseed-mover.py — Automatically repoints rTorrent torrents to *arr-imported paths.

After Radarr/Sonarr copies media into the library, they change the torrent
label to '<service>_imported' (e.g. radarr_imported). This script:
  1. Queries rTorrent (XMLRPC) for torrents with a *_imported label
     still pointing at /downloads/complete/
  2. Uses the torrent hash to look up the imported path via the *arr API
  3. Verifies the file(s) exist at the new location
  4a. Single-file, or a multi-file season pack whose release-name subfolder
      is still intact under the target directory: stops torrent, updates
      d.directory, rehashes, restarts. Not gated — the torrent keeps
      seeding from the library, so this happens as soon as it's imported.
      Same for multi-file torrents whose files all sit directly in the
      target directory under their original names and sizes (e.g. a
      one-.mkv movie release): d.directory_base.set points the torrent
      at that directory itself.
  4b. Multi-file torrents where *arr imported only some of the files
      directly into the target folder (e.g. a season pack whose bonus
      .mkv or .nfo matched no episode): the files *arr skipped are copied
      from the download into the target folder under their original
      names, then the torrent is repointed as in 4a. Only done when every
      file either already matches (name + size) at the target or doesn't
      exist there at all — a same-named file with a different size is
      never overwritten.
  4c. Anything else (e.g. *arr renamed the files) can't be reseeded in
      place — rTorrent has no per-file path override (f.path.set doesn't
      exist). The torrent is left alone: never erased, never deleted.
  5. After a successful repoint, deletes the old copy from
     /downloads/complete/ (the torrent now seeds from the library)

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
  HASH_TIMEOUT          Seconds to wait for a rehash   (default: 600)
                        before leaving it to the next run
  RUN_BUDGET            Seconds after which no new     (default: 2400)
                        repoints are started this run
                        (keep below the Job's activeDeadlineSeconds)

Repoints are resumable: before touching a torrent, its original d.directory
is stored in d.custom=reseed_mover_old_dir. Each run first resolves any
torrent still carrying that marker (hash timed out, job killed, XMLRPC
error) — finishing the repoint if the rehash completed at the new location,
reverting to the old directory otherwise.
"""

import xmlrpc.client
import requests
import os
import shutil
import logging
import sys
import time
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
HASH_TIMEOUT = float(os.getenv("HASH_TIMEOUT", "600"))
RUN_BUDGET = float(os.getenv("RUN_BUDGET", "2400"))

# d.custom key holding a torrent's original d.directory while a repoint is
# in flight. Non-empty means "unresolved repoint" — see resume_interrupted().
MARKER = "reseed_mover_old_dir"
# Set when a repoint had to be reverted. Such torrents are skipped from then
# on — retrying would stop seeding for two rehashes every run and fail the
# same way. Clear it (d.custom.set=reseed_mover_gave_up,"") to retry.
GAVE_UP = "reseed_mover_gave_up"

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
        f"d.custom={GAVE_UP}",
    )

    pending = []
    gave_up = 0
    for info_hash, name, directory, label, is_multi, size, failed_before in results:
        service = parse_service_from_label(label)
        if service and directory.startswith(DOWNLOADS_BASE):
            if failed_before:
                gave_up += 1
                continue
            pending.append(Torrent(
                info_hash=info_hash.upper(),
                name=name,
                directory=directory,
                label=label.strip().lower(),
                service=service,
                is_multi_file=bool(is_multi),
                size_bytes=size,
            ))

    log.info("Found %d imported torrent(s) still in %s (%d more skipped: earlier repoint reverted)",
             len(pending), DOWNLOADS_BASE, gave_up)
    return pending


def get_torrent_files(rt, info_hash: str) -> list[dict]:
    """Get per-file paths inside a torrent (relative to d.directory)."""
    results = rt.f.multicall(info_hash, "", "f.path=", "f.size_bytes=")
    return [{"path": path, "size": size} for path, size in results]


def content_root(directory: str, name: str, is_multi_file: bool) -> Path:
    """
    The torrent's data on disk. For multi-file torrents rTorrent's
    d.directory already ends in the release name (and f.path is relative to
    it); for single-file torrents d.directory is the containing folder.
    """
    return Path(directory) if is_multi_file else Path(directory) / name


def save_session(rt, h: str) -> None:
    try:
        rt.d.save_full_session(h)
    except Exception as e:
        log.warning("  Could not save session (state may not survive a restart): %s", e)


def hash_state(rt, h: str) -> str:
    """
    'hashing', 'failed', 'complete' or 'incomplete'.

    A failed hash check (I/O error) leaves d.hashing non-zero and sets
    d.hashing_failed instead, so that must be checked first or a failure
    looks like "still hashing" forever.
    """
    if rt.d.hashing_failed(h):
        return "failed"
    if rt.d.hashing(h) != 0:
        return "hashing"
    return "complete" if rt.d.complete(h) == 1 else "incomplete"


def revert_repoint(rt, h: str, old_dir: str) -> None:
    """
    Point the torrent back at old_dir, rehash and restart it.

    Only call this when the torrent isn't mid-hash: d.close on a torrent
    that is still hashing (and hasn't failed) trips an internal_error in
    rTorrent's close_throw(), which aborts the whole client.
    d.directory_base.set takes the full path without appending the name,
    which matches what d.directory reported for both single- and
    multi-file torrents.
    """
    rt.d.stop(h)
    rt.d.close(h)
    rt.d.directory_base.set(h, old_dir)
    rt.d.check_hash(h)  # no-op if hashing already queued (failed state)
    rt.d.start(h)       # deferred by rTorrent until the rehash finishes
    rt.d.custom.set(h, GAVE_UP, "1")
    rt.d.custom.set(h, MARKER, "")
    save_session(rt, h)


def resolve_repoint(rt, h: str, old_dir: str, state: str) -> str:
    """Act on a finished (or still running) rehash. Returns ok/failed/pending."""
    if state == "hashing":
        log.warning("  Still hashing — leaving it for the next run to resolve")
        return "pending"
    if state == "complete":
        rt.d.start(h)
        rt.d.custom.set(h, MARKER, "")
        save_session(rt, h)
        return "ok"
    log.error("  Hash check %s at new location — reverting to %s, NOT cleaning up",
              "failed" if state == "failed" else "found missing data", old_dir)
    revert_repoint(rt, h, old_dir)
    return "failed"


def repoint_torrent(rt, torrent: Torrent, new_dir: str, flat: bool = False) -> str:
    """
    Stop → close → set directory → rehash → wait → start (or revert).

    Returns 'ok', 'failed' or 'pending'. The old directory is recorded in
    MARKER before anything changes, so if the rehash outlasts HASH_TIMEOUT,
    the job gets killed, or XMLRPC errors out midway, the next run's
    resume_interrupted() finishes or reverts it — the torrent is never left
    stopped and pointing at a location nothing will look at again.

    The torrent must be closed, not just stopped: libtorrent refuses to
    change the root dir of an open download. d.directory.set auto-appends
    the release name for multi-file torrents, which is what the season-pack
    repoint wants (it only runs once new_dir/<name>/ is confirmed intact).
    With flat=True, d.directory_base.set points the torrent at new_dir
    itself instead, for multi-file torrents whose files *arr placed
    directly in new_dir (see multi_file_flat_match).
    """
    h = torrent.info_hash
    try:
        if rt.d.hashing(h) != 0:
            log.warning("  Torrent is already hashing — not touching it this run")
            return "pending"

        rt.d.custom.set(h, MARKER, torrent.directory)
        save_session(rt, h)
        rt.d.stop(h)
        rt.d.close(h)
        if flat:
            rt.d.directory_base.set(h, new_dir)
        else:
            rt.d.directory.set(h, new_dir)
        rt.d.check_hash(h)

        deadline = time.monotonic() + HASH_TIMEOUT
        while (state := hash_state(rt, h)) == "hashing" and time.monotonic() < deadline:
            time.sleep(2)

        return resolve_repoint(rt, h, torrent.directory, state)
    except Exception as e:
        # Deliberately no d.start here: if the directory already changed,
        # starting would make rTorrent download into the library path.
        log.error("  XMLRPC error: %s — leaving it for the next run to resolve", e)
        return "failed"


def resume_interrupted(rt, stats: dict) -> None:
    """Finish or revert repoints a previous run left unresolved (see MARKER)."""
    rows = rt.d.multicall2(
        "", "main",
        "d.hash=", "d.name=", "d.directory=", "d.is_multi_file=",
        f"d.custom={MARKER}",
    )
    for h, name, directory, is_multi, old_dir in rows:
        if not old_dir:
            continue
        log.info("▶ %s  unresolved repoint  hash=%s", name, h[:8])
        log.info("  %s → %s", old_dir, directory)

        if DRY_RUN:
            log.info("  [DRY RUN] Would resolve interrupted repoint")
            stats["pending"] += 1
            continue

        try:
            if directory == old_dir:
                # Interrupted before the directory changed — just undo the stop.
                log.info("  Directory never changed, restarting")
                rt.d.start(h)
                rt.d.custom.set(h, MARKER, "")
                save_session(rt, h)
                stats["skipped"] += 1
                continue

            result = resolve_repoint(rt, h, old_dir, hash_state(rt, h))
        except Exception as e:
            log.error("  XMLRPC error: %s", e)
            stats["failed"] += 1
            continue

        if result == "ok":
            log.info("  ✓ Repointed, still seeding")
            stats["repointed"] += 1
            if CLEANUP_OLD:
                cleanup(content_root(old_dir, name, bool(is_multi)), name)
        else:
            stats["failed" if result == "failed" else "pending"] += 1


# ─── *arr API helpers ────────────────────────────────────────────────────────

def arr_get(service: str, endpoint: str, params: dict | None = None) -> dict | list:
    cfg = SERVICES[service]
    url = f"{cfg['url']}/api/{cfg['api_version']}/{endpoint}"
    resp = requests.get(url, headers={"X-Api-Key": cfg["api_key"]}, params=params or {})
    resp.raise_for_status()
    return resp.json()


def log_sonarr_naming_config() -> None:
    """
    Best-effort, informational only: log Sonarr's renameEpisodes setting
    once at startup, to help interpret why season-pack structural-match
    checks succeed or fail. Not required for correctness — the runtime
    directory check in multi_file_structural_match is authoritative.
    """
    try:
        cfg = arr_get("sonarr", "config/naming")
        log.info("Sonarr renameEpisodes=%s (informational only)", cfg.get("renameEpisodes"))
    except Exception as e:
        log.debug("Could not fetch Sonarr naming config: %s", e)


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
    Find the library path for a Sonarr-imported episode or season pack by
    download hash.

    Resolves via the first history record's episode file path's parent
    directory — this naturally yields the season folder for a pack (all its
    episodes share one season dir) and the containing folder for a single
    episode, with no need to branch on is_multi_file or reconstruct a
    "Season NN" path manually. Falls back to the series root only if no
    episode-file detail is available yet.
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

    episode_id = records[0].get("episodeId")
    if episode_id:
        episode = arr_get("sonarr", f"episode/{episode_id}")
        file_id = episode.get("episodeFileId")
        if file_id:
            ef = arr_get("sonarr", f"episodefile/{file_id}")
            if ef.get("path"):
                return str(Path(ef["path"]).parent)

    # Fallback: no episode-file detail yet — best we can do is series root
    series = arr_get("sonarr", f"series/{series_id}")
    return series.get("path")


LOOKUP = {
    "radarr": lookup_radarr,
    "sonarr": lookup_sonarr,
}


# ─── Verification ────────────────────────────────────────────────────────────

def multi_file_structural_match(torrent: Torrent, new_dir: str, deep_verify: bool = True) -> bool:
    """
    True only if new_dir/torrent.name exists as a directory AND (when
    deep_verify) every file in the torrent is present at its original
    relative path underneath it.

    d.directory.set auto-appends torrent.name for multi-file torrents, so a
    repoint only lines up when *arr left the release-name subfolder intact
    rather than flattening files directly into the target folder — the
    latter (common) case can't be reseeded in place since rTorrent has no
    per-file path override.
    """
    root = Path(new_dir) / torrent.name
    if not root.is_dir():
        return False
    if not deep_verify:
        return True
    for f in torrent.files:
        # f.path is relative to the release folder, not to new_dir
        if not (root / f["path"]).exists():
            return False
    return True


def multi_file_flat_match(torrent: Torrent, new_dir: str) -> bool:
    """
    True if every file in the torrent sits directly under new_dir at its
    original relative path, with the same size — i.e. *arr dropped the
    release-folder level but kept the file names (typical for a movie
    release that is one .mkv in a folder, or a pack imported without
    renaming). d.directory_base.set(new_dir) then makes rTorrent resolve
    new_dir/f.path to exactly those files, so it can be reseeded in place.
    """
    if not torrent.files:
        return False
    root = Path(new_dir)
    for f in torrent.files:
        p = root / f["path"]
        if not p.is_file() or p.stat().st_size != f["size"]:
            return False
    return True


def verify_files_exist(torrent: Torrent, new_dir: str) -> bool:
    """
    Confirm that the torrent's single file actually exists at the new
    location. Prevents rTorrent from failing the hash check and going into
    an error state.
    """
    new_path = Path(new_dir)
    if not new_path.is_dir():
        log.warning("  Target directory doesn't exist: %s", new_path)
        return False

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


def plan_missing_files(torrent: Torrent, new_dir: str) -> list[dict] | None:
    """
    For a multi-file torrent *arr imported only partly into new_dir (files
    flattened, original names kept), return the files it skipped — the ones
    to copy over from the download so multi_file_flat_match() succeeds.

    Returns None — meaning "don't touch it" — unless every file either
    already sits at new_dir/f.path with the right size, or is absent there
    and complete in the download. Also None if nothing matched at all
    (then new_dir isn't where this release went, and copying the whole
    pack there would be wrong) or a path would escape new_dir.
    """
    if not torrent.files:
        return None
    root = Path(new_dir)
    src_root = content_root(torrent.directory, torrent.name, torrent.is_multi_file)
    matched, missing = 0, []
    for f in torrent.files:
        rel = Path(f["path"])
        if rel.is_absolute() or ".." in rel.parts:
            log.warning("  Unsafe file path in torrent: %s", f["path"])
            return None
        dst = root / rel
        if dst.exists():
            if dst.is_file() and dst.stat().st_size == f["size"]:
                matched += 1
                continue
            log.info("  %s exists at target with a different size — not overwriting", rel)
            return None
        src = src_root / rel
        if not src.is_file() or src.stat().st_size != f["size"]:
            log.info("  %s missing at target and incomplete in the download", rel)
            return None
        missing.append(f)
    if matched == 0:
        return None
    return missing


def copy_missing_files(torrent: Torrent, new_dir: str, missing: list[dict]) -> bool:
    """
    Copy the files plan_missing_files() found into new_dir under their
    original relative paths. Each goes to a temp name first and is renamed
    once its size checks out, so an interrupted copy never leaves a
    truncated file under the real name (which would fail the rehash).
    """
    src_root = content_root(torrent.directory, torrent.name, torrent.is_multi_file)
    for f in missing:
        src = src_root / f["path"]
        dst = Path(new_dir) / f["path"]
        tmp = dst.with_name(dst.name + ".reseed-partial")
        try:
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(src, tmp)
            if tmp.stat().st_size != f["size"]:
                raise OSError(f"size mismatch after copy ({tmp.stat().st_size} != {f['size']})")
            os.replace(tmp, dst)
            log.info("  Copied %s (%.1f GiB)", f["path"], f["size"] / 2**30)
        except OSError as e:
            log.error("  Copy of %s failed: %s", f["path"], e)
            try:
                tmp.unlink(missing_ok=True)
            except OSError:
                pass
            return False
    return True


# ─── Cleanup ─────────────────────────────────────────────────────────────────

def cleanup(target: Path, name: str):
    """
    Remove the old copy from downloads/complete/, given its content_root():
      single-file → /downloads/complete/radarr/movie.mkv       (a file)
      multi-file  → /downloads/complete/radarr/ReleaseGroup/    (a directory)

    Refuses anything that isn't <DOWNLOADS_BASE>/<category>/<torrent name>
    or deeper — a d.directory that doesn't end in the release name (e.g.
    after a d.directory_base.set rename) must never rmtree a whole category.
    """
    try:
        rel = target.resolve().relative_to(Path(DOWNLOADS_BASE).resolve())
    except ValueError:
        rel = None
    if rel is None or len(rel.parts) < 2 or target.name != name:
        log.error("  Refusing to delete %s (not <%s>/<category>/<torrent name>)",
                  target, DOWNLOADS_BASE)
        return

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
    log_sonarr_naming_config()
    run_started = time.monotonic()

    stats = {"repointed": 0, "skipped": 0, "pending": 0, "failed": 0}
    resume_interrupted(rt, stats)
    torrents = get_pending_torrents(rt)

    def do_repoint(t: Torrent, new_path: str, kind: str, flat: bool = False) -> None:
        if DRY_RUN:
            log.info("  [DRY RUN] Would repoint directory%s", kind)
            stats["repointed"] += 1
            return
        if time.monotonic() - run_started > RUN_BUDGET:
            log.info("  Run budget (%.0fs) used up — deferring to the next run", RUN_BUDGET)
            stats["skipped"] += 1
            return
        result = repoint_torrent(rt, t, new_path, flat)
        if result == "ok":
            log.info("  ✓ Repointed%s, still seeding", kind)
            stats["repointed"] += 1
            if CLEANUP_OLD:
                cleanup(content_root(t.directory, t.name, t.is_multi_file), t.name)
        else:
            stats[result] += 1

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
        # rTorrent resolves files as d.directory/f.path, where d.directory
        # already ends in the release folder name (e.g.
        # ".../sonarr/SomeRelease-GROUP") and f.path is relative to it. If
        # *arr left that folder intact under the target directory,
        # d.directory.set + rehash can still reseed in place (see
        # repoint_torrent). If *arr flattened the files directly into the
        # target folder, d.directory_base.set does the same — provided every
        # file is there, so files *arr skipped get copied over first. Renamed
        # files can't be reseeded (rTorrent has no per-file path override);
        # those torrents are left alone, never erased.
        if t.is_multi_file:
            structural_match = multi_file_structural_match(t, new_path, deep_verify=VERIFY_FILES)

            if structural_match:
                log.info("  Multi-file torrent — release subfolder intact, attempting repoint")
                do_repoint(t, new_path, " (season pack)")
                continue

            if multi_file_flat_match(t, new_path):
                log.info("  Multi-file torrent — files directly in target folder, attempting repoint")
                do_repoint(t, new_path, " (flattened)", flat=True)
                continue

            missing = plan_missing_files(t, new_path)
            if missing:
                total = sum(f["size"] for f in missing)
                log.info("  Multi-file torrent — %d of %d file(s) not imported by %s (%.1f GiB), "
                         "copying them to the target before repointing",
                         len(missing), len(t.files), t.service, total / 2**30)
                for f in missing:
                    log.info("    + %s", f["path"])
                if DRY_RUN:
                    log.info("  [DRY RUN] Would copy the above, then repoint directory (completed)")
                    stats["repointed"] += 1
                    continue
                if time.monotonic() - run_started > RUN_BUDGET:
                    log.info("  Run budget (%.0fs) used up — deferring to the next run", RUN_BUDGET)
                    stats["skipped"] += 1
                    continue
                if not copy_missing_files(t, new_path, missing):
                    stats["failed"] += 1
                    continue
                do_repoint(t, new_path, " (completed)", flat=True)
                continue

            log.info("  Multi-file torrent — files renamed or not where expected, can't reseed "
                     "in place; leaving it in rTorrent untouched")
            stats["skipped"] += 1
            continue

        # ── Single-file torrents ──────────────────────────────────────
        # Verify the file exists at the library path with matching name.
        # The *_imported label already guarantees the import succeeded, so
        # a mismatch here is almost always a rename by *arr — permanent,
        # not a run failure, so it mustn't turn every run red.
        if VERIFY_FILES and not verify_files_exist(t, new_path):
            log.warning("  File verification failed — skipping (rename mismatch?)")
            stats["skipped"] += 1
            continue

        do_repoint(t, new_path, "")

    log.info(
        "═══ Finished: %d repointed, %d skipped, %d pending, %d failed ═══",
        stats["repointed"], stats["skipped"], stats["pending"], stats["failed"],
    )
    return 0 if stats["failed"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
