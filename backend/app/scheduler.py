"""
Daily schedule runner: enable/disable targets by scope at configured local times.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta

import aiosqlite

from .target_scope import batch_set_enabled, resolve_target_ids

logger = logging.getLogger(__name__)

# (schedule_id, YYYY-MM-DD HH:MM) keys already fired
_fired: set[str] = set()
_MAX_FIRED_CACHE = 5000

# Minute fully processed (local time, second=0). None until the first tick.
_last_minute: datetime | None = None

# Bound the catch-up window so a long pause cannot replay an unbounded backlog.
_MAX_CATCHUP_MINUTES = 120

# fire key -> consecutive failures (bounded retry so one broken schedule does
# not block all later schedules forever)
_retry_count: dict[str, int] = {}
_MAX_RETRIES = 5


def _weekday_set(weekdays_csv: str) -> set[int]:
    """CSV of 0-6 where 0=Monday (datetime.weekday()). Empty => every day."""
    s = (weekdays_csv or "").strip()
    if not s:
        return set(range(7))
    out = set()
    for part in s.split(","):
        part = part.strip()
        if part.isdigit() and 0 <= int(part) <= 6:
            out.add(int(part))
    return out if out else set(range(7))


async def run_schedule_checker(db_path: str) -> None:
    """Poll every 15s; fire schedules for every elapsed minute (incl. catch-up)."""
    global _fired, _last_minute, _retry_count
    while True:
        try:
            await _tick(db_path)
        except Exception as exc:
            logger.error("Schedule checker error: %s", exc)
        await asyncio.sleep(15)


def _truncate_minute(dt: datetime) -> datetime:
    return dt.replace(second=0, microsecond=0)


async def _tick(db_path: str) -> None:
    global _last_minute

    now = _truncate_minute(datetime.now())

    # First run: initialise the watermark but do not fire (we cannot know how
    # long the process was down, and firing stale minutes on boot is surprising).
    if _last_minute is None:
        _last_minute = now
        logger.info("Schedule checker initialised at %s", now)
        return

    # Nothing new since the last tick.
    if now <= _last_minute:
        return

    # Bound the catch-up window so a long pause cannot replay a huge backlog.
    earliest = now - timedelta(minutes=_MAX_CATCHUP_MINUTES)
    if _last_minute < earliest:
        _last_minute = earliest

    schedules = await _load_schedules(db_path)
    if not schedules:
        _last_minute = now
        return

    # Walk every fully-elapsed minute, including the current one. A minute is
    # considered elapsed once we have entered it (truncated "now"), which gives
    # the tick at 10:40:xx a stable target for schedules set to "10:40".
    minute = _last_minute + timedelta(minutes=1)
    while minute <= now:
        await _process_minute(db_path, schedules, minute)
        minute += timedelta(minutes=1)

    _last_minute = now

    # Prune memory maps so they cannot grow without bound over long uptimes.
    _prune_caches()


async def _load_schedules(db_path: str) -> list[dict]:
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM target_schedules WHERE enabled=1"
        ) as cur:
            return [dict(r) for r in await cur.fetchall()]


async def _process_minute(db_path: str, schedules: list[dict], minute: datetime) -> None:
    hhmm = minute.strftime("%H:%M")
    wd = minute.weekday()
    fire_key_base = minute.strftime("%Y-%m-%d ") + hhmm

    for sch in schedules:
        if sch.get("time_hhmm") != hhmm:
            continue
        if wd not in _weekday_set(sch.get("weekdays", "")):
            continue

        fk = f"{sch['id']}:{fire_key_base}"
        if fk in _fired:
            continue
        if _retry_count.get(fk, 0) >= _MAX_RETRIES:
            _fired.add(fk)  # give up after the bound so later minutes stay healthy
            continue

        try:
            enabled = sch["action"] == "enable"
            ids = await resolve_target_ids(
                db_path,
                sch["scope_type"],
                sch.get("scope_value") or "",
            )
            result = await batch_set_enabled(db_path, ids, enabled)
        except Exception as exc:
            # One bad schedule must not abort the whole minute: retry on the
            # next tick, then give up after _MAX_RETRIES.
            _retry_count[fk] = _retry_count.get(fk, 0) + 1
            logger.error(
                "Schedule #%s '%s' failed (%d/%d): %s",
                sch["id"], sch.get("name"), _retry_count[fk], _MAX_RETRIES, exc,
            )
            continue

        _fired.add(fk)
        _retry_count.pop(fk, None)
        logger.info(
            "Schedule #%s '%s' fired: %s %d targets",
            sch["id"],
            sch.get("name"),
            sch["action"],
            result["updated"],
        )


def _prune_caches() -> None:
    """Drop old entries so the in-memory maps cannot grow without bound."""
    if len(_fired) > _MAX_FIRED_CACHE:
        _fired.clear()
    if len(_retry_count) > _MAX_FIRED_CACHE:
        _retry_count.clear()
